/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveHandler.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "session/CleanerSession.hpp"
#include "session/DataTransferSession.hpp"
#include "session/EmptyDriveProbe.hpp"
#include "session/Session.hpp"

#include <unistd.h>
#include <utility>

namespace cta::tape::daemon {

DriveHandler::DriveHandler(const TapedConfig& config, log::Logger& log)
    : m_config(config),
      m_driveInfo(m_config.drive.name,
                  utils::getShortHostname(),
                  m_config.drive.logical_library_name,
                  m_config.drive.device,
                  m_config.drive.control_path),
      m_lc(log),
      m_mediaChanger(mediachanger::RmcProxy(m_config.rmcd.host,
                                            m_config.rmcd.port,
                                            m_config.rmcd.request_timeout_secs,
                                            m_config.rmcd.request_attempts),
                     log) {
  m_lc.log(log::INFO, "Initialising Catalogue");
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(m_config.catalogue.config_file);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);
  m_catalogue = catalogueFactory->create();

  m_lc.log(log::INFO, "Initialising Scheduler");
#ifndef CTA_PGSCHED
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                      utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                      m_lc.logger());
#else
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                      utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                      m_config.scheduler.number_of_connections,
                                                      m_lc.logger());
#endif
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = m_config.scheduler.tape_cache_max_age_secs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_config.scheduler.retrieve_queue_cache_max_age_secs;
  m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);
  m_scheduler = std::make_unique<Scheduler>(*m_catalogue,
                                            *m_schedDb,
                                            m_config.scheduler.backend_name,
                                            m_config.mounts.minimum_queued_files,
                                            m_config.mounts.minimum_queued_bytes);

  m_lc.log(log::INFO, "Scheduler and Catalogue initialised");
}

void DriveHandler::stop() {
  m_stopSource.request_stop();
}

void DriveHandler::waitForDriveToBeUp() {
  // TODO: graceful shutdown (separate MR)
  while (true) {
    try {
      m_lc.log(log::DEBUG, "Transition from down to up starting.");
      auto desiredState = m_scheduler->getDesiredDriveState(m_config.drive.name, m_lc);
      if (!desiredState.up) {
        m_lc.log(log::DEBUG, "Desired drive state is NOT UP, setting it DOWN");
        // Refresh the status to trigger the timeout update
        m_scheduler->reportDriveStatus(m_driveInfo,
                                       common::dataStructures::MountType::NoMount,
                                       common::dataStructures::DriveStatus::Down,
                                       m_lc);

        // We wait a bit before polling the scheduler again.
        // TODO: Ensure graceful shutdown can interrupt this sleep
        sleep(m_config.mounts.drive_state_poll_interval_secs);
      } else {
        m_lc.log(log::DEBUG, "Desired drive state is UP.");
        break;
      }
    } catch (Scheduler::NoSuchDrive& e) {
      // The scheduler does not even know about this drive. We will report our state
      // (default status is down).
      putDriveDown(e.getMessageValue());
      // TODO
    }
  }
}

void DriveHandler::putDriveDown(std::string_view errorMsg) {
  m_lc.logEvent(log::ERR, errorMsg, semconv::log::EventNameValues::kPuttingTapeDriveDown);
  try {
    m_scheduler->reportDriveStatus(m_driveInfo,
                                   common::dataStructures::MountType::NoMount,
                                   common::dataStructures::DriveStatus::Down,
                                   m_lc);
    common::dataStructures::DesiredDriveState driveState;
    driveState.up = false;
    driveState.forceDown = false;
    driveState.setReasonFromLogMsg(log::ERR, errorMsg);
    m_scheduler->setDesiredDriveState(m_config.drive.name, driveState, m_lc);
  } catch (exception::Exception& ex) {
    // TODO: we probably need a separate exception for this so that we can handle this
    // This is not recoverable
    log::ScopedParamContainer param(m_lc);
    param.add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::CRIT, "In DriveHandler::runChild(): failed to set the drive down. Reporting fatal error.");
    // TODO: state reporting?
    // driveHandlerProxy->reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    sleep(1);
  }
}

std::unique_ptr<TapeMount> DriveHandler::getNextMount() {
  try {
    // TODO: add timeout?
    if (m_scheduler->getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
      return m_scheduler->getNextMount(m_driveInfo.logicalLibrary,
                                       m_driveInfo.driveName,
                                       m_lc,
                                       static_cast<uint64_t>(m_config.mounts.get_next_mount_timeout_secs) * 1000000);
    }
  } catch (exception::LostDatabaseConnection&) {
    // TODO: add retry mechanism (or wait for the DB to be up again)
    // This should probably be consolidated with the rest of the lost DB functionality
    m_lc.log(log::ERR, "Lost database error while scheduling new mount. Retrying.");
  }
  return nullptr;
}

// TODO: handle lost database connections cleanly. No need to crash the whole thing on those
// We should have clearly defined behaviour there
int DriveHandler::run() {
  // TODO: wait for catalogue and scheduler to be reachable within a reasonable timeout

  // For a separate MR: add a config option for automatically putting the drive up on startup when possible
  if (!registerDrive(false)) {
    return 1;
  }

  // TODO: if the logical library does not exist (yet), what do we do? Just wait?
  // TODO: telemetry drive state tracking
  // Needs to be done after the catalogue initialization

  // TODO: we probably don't need this polling anymore; add it to reportDriveStatus
  // [[maybe_unused]] ::daemon::DriveSessionTracker driveSessionTracker(m_catalogue, driveInfo.driveName); // TODO

  // TODO: add stop token here

  while (true) {
    // TODO: track whether probing is required locally so an early desired-state change cannot skip it.
    if (!m_scheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
      waitForDriveToBeUp();

      m_scheduler->reportDriveStatus(m_driveInfo,
                                     common::dataStructures::MountType::NoMount,
                                     common::dataStructures::DriveStatus::Probing,
                                     m_lc);
      EmptyDriveProbe emptyDriveProbe(m_lc.logger(), m_driveInfo, m_sysWrapper);
      m_lc.log(log::DEBUG, "Transition from down to up detected. Will check if a tape is in the drive.");

      // Start by running the cleaner to unload any possible tape
      // For a separate MR: Add an option to config to allow this
      if (false) {
        // TODO: handle failure of this correctly
        // Can the cleaner return true/false based on whether it succeeded or not?
        executeCleanerSession();
      }

      if (!emptyDriveProbe.driveIsEmpty()) {
        // TODO: distinguish a detected tape from a failed probe when reporting the drive-down reason.
        // TODO: log warning
        std::string errorMsg = "A tape was detected in the drive. Putting the drive down.";
        errorMsg += emptyDriveProbe.getProbeErrorMsg().value_or("");
        putDriveDown(errorMsg);
        // Continue the loop so that we wait for the drive to come up again
        continue;
      } else {
        m_lc.log(log::DEBUG, "No tape detected in the drive. Proceeding with scheduling.");
      }
    }

    // Report state
    m_scheduler->reportDriveStatus(m_driveInfo,
                                   common::dataStructures::MountType::NoMount,
                                   common::dataStructures::DriveStatus::Up,
                                   m_lc);
    // TODO: rip out session reporting
    // tapeSessionReporter.reportState(::session::SessionState::Scheduling,
    //                                 ::session::SessionType::Undetermined);

    std::unique_ptr<TapeMount> tapeMount;

    struct MountReferenceReset {
      TapeSessionTracker& tracker;

      ~MountReferenceReset() { tracker.setMount(nullptr); }
    } mountReferenceReset {m_tapeSessionTracker};

    utils::Timer t;
    try {
      tapeMount = getNextMount();
      if (tapeMount != nullptr) {
        m_tapeSessionTracker.setMount(tapeMount.get());
      }
    } catch (exception::TimeoutException&) {
      log::ScopedParamContainer params(m_lc);
      // TODO: should this be a string?
      params.add("totalScheduleMountTime", std::to_string(t.secs()));
      // TODO: is this really a locking issue?
      m_lc.log(log::WARNING,
               "Timed out while scheduling new mount. Could not acquire global scheduler lock in "
                 + std::to_string(m_config.mounts.get_next_mount_timeout_secs) + " seconds.");

      m_lc.log(log::DEBUG,
               "No new mount found. (sleeping " + std::to_string(m_config.mounts.idle_scheduling_interval_secs)
                 + " seconds)");
      // TODO Before we sleep, should we check for down/up transition to be more responsive?
      // TODO What about graceful shutdown? It should be able to interrupt this sleep
      sleep(m_config.mounts.idle_scheduling_interval_secs);
      // At this point, start the loop from the beginning
      continue;
    }

    // TODO: if no mount is available, wait and continue instead of treating nullptr as a transfer failure.
    // Now that we have a mount, execute the data transfer session
    // TODO: handle non-database transfer exceptions too, setting drive state and handling cleanup.
    bool success = tapeMount != nullptr && executeDataTransferSession(*tapeMount);
    if (!success) {
      // TODO: we need a better reason here
      putDriveDown("Data transfer session failed");
      // After this, the loop will continue by waiting to be up again
    }
    // This is for another MR, but we should rip out the cleaner functionality from the transfer sessions and rely on CleanerSession only
  }

  // At this point, the drive is exiting. Start cleanup
  executeCleanerSession();
  // Put the drive down
  // TODO: this is not correct, because it may already be down
  putDriveDown("[cta-taped] Exiting cta-taped");
  // TODO: correct exit code
  return 0;
}

bool DriveHandler::registerDrive(bool putUpIfPossible) {
  // TODO: I don't think this method works correctly
  if (!m_scheduler->checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    // TODO: log message?
    return false;
  }

  cta::common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (Scheduler::NoSuchDrive&) {
    m_lc.log(log::INFO, "In DriveHandler::runChild(): the desired drive state doesn't exist in the Catalogue DB");
  }

  cta::common::dataStructures::SecurityIdentity securityIdentity;
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  m_scheduler->createTapeDriveStatus(m_driveInfo,
                                     driveState,
                                     cta::common::dataStructures::MountType::NoMount,
                                     cta::common::dataStructures::DriveStatus::Down,
                                     securityIdentity,
                                     m_lc);

  // This is not the same as the previous reason; should we rethink those reasons?
  std::string startupReason = "[cta-taped] Startup";

  // If there was no previous reason or if the previous reason was a clean exit,

  // Get the drive state to see if there is a reason or not, we don't want to change the reason
  // why a drive is down at the startup of taped. If it's setted up a previous Reason From Log
  // it will be change for this one.
  // TODO: fix the clean-exit comparison; an 11-character substring cannot match the full exit reason.
  if (!currentDesiredDriveState.reason
      || currentDesiredDriveState.reason.value().substr(0, 11) == "[cta-taped] Exiting cta-taped") {
    // If there is no
    driveState.reason = startupReason;
    if (putUpIfPossible) {
      // In these cases we could safely put the drive up
      driveState.up = true;
    }
  } else {
    // In all other cases, we keep the same reason as before and we put the drive down
    driveState.reason = currentDesiredDriveState.reason.value();
  }

  m_scheduler->setDesiredDriveState(m_driveInfo.driveName, driveState, m_lc);
  m_scheduler->reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
  return true;
  // TODO: catch exception?
}

bool DriveHandler::executeDataTransferSession(TapeMount& tapeMount) {
  // TODO: this should eventually rely only on transfer config
  // Anything that needs something non-transfer related should probably be extracted out of data transfer session
  DataTransferConfig dataTransferConfig;
  dataTransferConfig.bufsz = m_config.transfers.buffer_size_bytes;
  dataTransferConfig.bulkRequestMigrationMaxBytes = m_config.transfers.archive.fetch_max_bytes;
  dataTransferConfig.bulkRequestMigrationMaxFiles = m_config.transfers.archive.fetch_max_files;
  dataTransferConfig.archiveDismountPolicy.set(m_config.transfers.archive.underfill.watch_period_secs,
                                               m_config.transfers.archive.underfill.minimum_samples,
                                               m_config.transfers.archive.underfill.start_threshold_percent,
                                               m_config.transfers.archive.underfill.recovery_threshold_percent);
  dataTransferConfig.bulkRequestRecallMaxBytes = m_config.transfers.retrieve.fetch_max_bytes;
  dataTransferConfig.bulkRequestRecallMaxFiles = m_config.transfers.retrieve.fetch_max_files;
  dataTransferConfig.maxBytesBeforeFlush = m_config.transfers.archive.flush_max_bytes;
  dataTransferConfig.maxFilesBeforeFlush = m_config.transfers.archive.flush_max_files;
  dataTransferConfig.nbBufs = m_config.transfers.buffer_count;
  dataTransferConfig.nbDiskThreads = m_config.transfers.disk_io_threads;
  dataTransferConfig.useLbp = true;
  dataTransferConfig.useRAO = m_config.transfers.retrieve.rao.enabled;
  dataTransferConfig.raoLtoAlgorithm = m_config.transfers.retrieve.rao.lto_algorithm;
  dataTransferConfig.raoLtoAlgorithmOptions = "cost_heuristic_name:cta";  // Only option available
  dataTransferConfig.externalFreeDiskSpaceScript = m_config.transfers.retrieve.external_free_disk_space_script;
  dataTransferConfig.tapeLoadTimeout = m_config.mounts.tape_load_timeout_secs;
  dataTransferConfig.xrootTimeout = 0;
  dataTransferConfig.useEncryption = m_config.transfers.encryption.enabled;
  dataTransferConfig.externalEncryptionKeyScript = m_config.transfers.encryption.external_key_script;
  dataTransferConfig.wdIdleSessionTimer = m_config.mounts.idle_scheduling_interval_secs;
  dataTransferConfig.driveStatePollIntervalSecs = m_config.mounts.drive_state_poll_interval_secs;
  dataTransferConfig.wdGetNextMountMaxSecs = m_config.mounts.get_next_mount_timeout_secs;
  dataTransferConfig.wdNoBlockMoveMaxSecs = m_config.transfers.no_block_move_timeout_secs;

  DataTransferSession dataTransferSession(utils::getShortHostname(),
                                          m_lc.logger(),
                                          m_sysWrapper,
                                          m_driveInfo,
                                          m_mediaChanger,
                                          tapeMount,
                                          m_tapeSessionTracker,
                                          dataTransferConfig,
                                          *m_scheduler);
  // This is hacky; this whole end of session action stuff should be ripped out
  return dataTransferSession.execute() == Session::EndOfSessionAction::MARK_DRIVE_AS_UP;
}

void DriveHandler::executeCleanerSession(const std::optional<std::string>& vid) {
  const auto cleanerSession = std::make_unique<CleanerSession>(m_mediaChanger,
                                                               m_lc.logger(),
                                                               m_driveInfo,
                                                               m_sysWrapper,
                                                               vid.value_or(""),
                                                               true,
                                                               m_config.mounts.tape_load_timeout_secs,
                                                               *m_catalogue,
                                                               *m_scheduler);

  cleanerSession->execute();
}

bool DriveHandler::isLive() const {
  // TODO: look into the timeouts and see if we have spent too much time in any given state
  // We don't ping the catalogue/scheduler here as that would just result in cascading failures
  // A restart won't fix things
  return true;
}

bool DriveHandler::isReady() const {
  // TODO ping catalogue and scheduler
  return true;
}
}  // namespace cta::tape::daemon
