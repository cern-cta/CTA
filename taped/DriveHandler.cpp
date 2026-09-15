/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveHandler.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/exception/Exception.hpp"
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
  m_lc.log(log::INFO, "Waiting for the desired drive state to become up.");
  // TODO: graceful shutdown (separate MR)
  while (true) {
    common::dataStructures::DesiredDriveState desiredState;
    try {
      desiredState = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
    } catch (const Scheduler::NoSuchDrive&) {
      m_lc.log(log::WARNING, "Drive is missing from the catalogue. Attempting to register it as down.");
      if (!registerDrive(false)) {
        m_lc.log(log::CRIT, "Failed to register the missing drive. Cannot continue waiting for it to become up.");
        throw exception::Exception("In DriveHandler::waitForDriveToBeUp(): failed to register the missing drive");
      }
      m_lc.log(log::INFO, "Missing drive registered as down. Waiting for the desired drive state to become up.");
      // Re-read the desired state on the next iteration after registration.
      // TODO: Ensure graceful shutdown can interrupt this sleep
      sleep(m_config.mounts.drive_state_poll_interval_secs);
      continue;
    }

    if (desiredState.up) {
      m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive probing.");
      return;
    }

    m_lc.log(log::DEBUG, "Desired drive state is down. Refreshing the reported down status.");
    // Refresh the status to trigger the timeout update. Reporting failures propagate to the caller.
    m_scheduler->reportDriveStatus(m_driveInfo,
                                   common::dataStructures::MountType::NoMount,
                                   common::dataStructures::DriveStatus::Down,
                                   m_lc);

    // TODO: Ensure graceful shutdown can interrupt this sleep
    sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

void DriveHandler::putDriveDown(common::dataStructures::DriveDownReason reason, std::string_view detail) {
  const auto errorMsg = common::dataStructures::formatDriveDownReason(reason, detail);
  m_lc.logEvent(common::dataStructures::driveDownReasonSeverity(reason),
                errorMsg,
                semconv::log::EventNameValues::kPuttingTapeDriveDown);
  m_scheduler->reportDriveStatus(m_driveInfo,
                                 common::dataStructures::MountType::NoMount,
                                 common::dataStructures::DriveStatus::Down,
                                 m_lc);
  common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  driveState.reason = errorMsg;
  m_scheduler->setDesiredDriveState(m_config.drive.name, driveState, m_lc);
}

std::unique_ptr<TapeMount> DriveHandler::getNextMount() {
  // TODO: add timeout?
  if (m_scheduler->getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
    return m_scheduler->getNextMount(m_driveInfo.logicalLibrary,
                                     m_driveInfo.driveName,
                                     m_lc,
                                     static_cast<uint64_t>(m_config.mounts.get_next_mount_timeout_secs) * 1000000);
  }
  return nullptr;
}

bool DriveHandler::registerDrive(bool putUpIfPossible) {
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!m_scheduler->checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    m_lc.log(log::CRIT, "Cannot register the drive: its name belongs to a different host or logical library.");
    return false;
  }

  common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (const Scheduler::NoSuchDrive&) {
    m_lc.log(log::INFO, "Drive has no existing catalogue entry. Creating one.");
  }

  common::dataStructures::DesiredDriveState driveState;
  driveState.comment = currentDesiredDriveState.comment;
  // Replace absent or clean-exit reasons with the startup reason. Preserve other reasons for being down.
  if (!currentDesiredDriveState.reason
      || common::dataStructures::isCleanDriveShutdownReason(*currentDesiredDriveState.reason)) {
    driveState.reason = common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
    driveState.up = putUpIfPossible;
  } else {
    driveState.reason = currentDesiredDriveState.reason;
  }

  common::dataStructures::SecurityIdentity securityIdentity;
  m_scheduler->createTapeDriveStatus(m_driveInfo,
                                     driveState,
                                     common::dataStructures::MountType::NoMount,
                                     common::dataStructures::DriveStatus::Down,
                                     securityIdentity,
                                     m_lc);
  m_scheduler->reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
  m_lc.log(log::INFO,
           "Drive registered with reported status down and desired state "
             + std::string(driveState.up ? "up." : "down."));
  return true;
}

bool DriveHandler::executeDataTransferSession(TapeMount& tapeMount) {
  if (m_tapeSessionTracker.mount() != &tapeMount) {
    throw exception::Exception(
      "In DriveHandler::executeDataTransferSession(): tracker does not reference the supplied tape mount");
  }

  DataTransferSession dataTransferSession(
    m_lc.logger(),
    m_sysWrapper,
    m_driveInfo,
    m_mediaChanger,
    tapeMount,
    m_tapeSessionTracker,
    m_config.transfers,
    m_config.mounts.tape_load_timeout_secs,  // TODO: DataTransferSession should not be responsible for tape loading
    *m_scheduler);
  // This is hacky; this whole end of session action stuff should be ripped out
  return dataTransferSession.execute() == Session::EndOfSessionAction::MARK_DRIVE_AS_UP;
}

bool DriveHandler::executeCleanerSession(const std::optional<std::string>& vid, bool waitMediaInDrive) {
  CleanerSession cleanerSession(m_mediaChanger,
                                m_lc.logger(),
                                m_driveInfo,
                                m_sysWrapper,
                                vid.value_or(""),
                                waitMediaInDrive,
                                m_config.mounts.tape_load_timeout_secs,
                                *m_catalogue,
                                *m_scheduler);

  // This is hacky; this whole end of session action stuff should be ripped out
  return cleanerSession.execute() == Session::EndOfSessionAction::MARK_DRIVE_AS_UP;
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

// TODO: handle lost database connections cleanly. No need to crash the whole thing on those
// We should have clearly defined behaviour there
int DriveHandler::run() {
  // TODO: wait for catalogue and scheduler to be reachable within a reasonable timeout

  // For a separate MR: add a config option for automatically putting the drive up on startup when possible
  if (!registerDrive(false)) {
    return 1;
  }

  // TODO: if the logical library does not exist (yet), what do we do? Just wait?

  // TODO: add graceful shutdown
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
        putDriveDown(common::dataStructures::DriveDownReason::TapeDetected,
                     emptyDriveProbe.getProbeErrorMsg().value_or(""));
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
    bool driveCanRemainUp = tapeMount != nullptr && executeDataTransferSession(*tapeMount);
    if (!driveCanRemainUp) {
      // TODO: we need a better reason here
      // TODO: preserve more specific drive-open or cleaning reasons already recorded by the session.
      putDriveDown(common::dataStructures::DriveDownReason::TransferSessionFailed);
      // After this, the loop will continue by waiting to be up again
    }
    // This is for another MR, but we should rip out the cleaner functionality from the transfer sessions and rely on CleanerSession only
  }

  // At this point, the drive is exiting. Start cleanup
  executeCleanerSession();
  // Put the drive down
  // TODO: this is not correct, because it may already be down
  putDriveDown(common::dataStructures::DriveDownReason::Shutdown);
  // TODO: correct exit code
  return 0;
}

}  // namespace cta::tape::daemon
