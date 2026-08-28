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
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "session/CleanerSession.hpp"
#include "session/DataTransferSession.hpp"
#include "session/EmptyDriveProbe.hpp"
#include "session/Session.hpp"

namespace cta::tape::daemon {

DriveHandler::DriveHandler(const TapedConfig& config, log::Logger& log)
    : m_config(config),
      m_lc(log),
      m_driveInfo(m_config.drive.name,
                  utils::getShortHostname(),
                  m_config.drive.logical_library_name,
                  m_config.drive.device,
                  m_config.drive.control_path) {
  mediachanger::RmcProxy rmcProxy(m_config.rmcd.host,
                                  m_config.rmcd.port,
                                  m_config.rmcd.request_timeout_secs,
                                  m_config.rmcd.request_attempts);
  mediachanger::MediaChangerFacade m_mediaChanger(rmcProxy, log);

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
  m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_schedDb, config.scheduler.backend_name);

  m_lc.log(log::INFO, "Scheduler and Catalogue initialised");
}

void DriveHandler::stop() {
  m_stopSource.request_stop();
}

void DriveHandler::waitForDriveToBeUp() {
  // TODO: stop token & graceful shutdown
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
        // TODO: graceful shutdown
        sleep(m_config.mounts.drive_state_poll_interval_secs);
      } else {
        m_lc.log(log::DEBUG, "Desired drive state is UP.");
        break;
      }
    } catch (Scheduler::NoSuchDrive& e) {
      // The object store does not even know about this drive. We will report our state
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
                                       m_config.mounts.get_next_mount_timeout_secs
                                         * 1000000);  // TODO: is this multiplication correct? (probably not)
    }
  } catch (exception::LostDatabaseConnection&) {
    // TODO: add retry mechanism
    m_lc.log(log::ERR, "Lost database error while scheduling new mount. Retrying.");
  }
  return nullptr;
}

int DriveHandler::run() {
  // TODO: telemetry drive state tracking

  // Needs to be done after the catalogue initialization
  // TODO: we probably don't need this polling anymore; add it to reportDriveStatus
  // [[maybe_unused]] ::daemon::DriveSessionTracker driveSessionTracker(m_catalogue, driveInfo.driveName); // TODO

  // Start by registering the drive in the catalogue. Drives start as down
  // If the drive already exists and it was down, we ensure we don't overwrite the reason

  // Start by running the cleaner to unload any possible tape
  executeCleanerSession();
  // Cleaner doesn't modify drive state, so if it fails, the drive is already down and we just proceed to the loop
  // where we wait for the drive to be put again (by an operator)

  // TODO: handle lost database connections cleanly. No need to crash the whole thing on those
  // We should have clearly defined behaviour there

  // TODO: if the catalogue/scheduler is not reachable, do we quit or do we idle until they become reachable?

  // TODO: add stop token here
  while (true) {
    if (driveNotUp()) {
      waitForDriveToBeUp();

      m_scheduler->reportDriveStatus(m_driveInfo,
                                     common::dataStructures::MountType::NoMount,
                                     common::dataStructures::DriveStatus::Probing,
                                     m_lc);
      EmptyDriveProbe emptyDriveProbe(m_lc.logger(), m_driveInfo, m_sysWrapper);
      m_lc.log(log::DEBUG, "Transition from down to up detected. Will check if a tape is in the drive.");
      if (!emptyDriveProbe.driveIsEmpty()) {
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
    utils::Timer t;
    try {
      tapeMount = getNextMount();
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

    // Now that we have a mount, execute the data transfer session
    bool success = executeDataTransferSession(std::move(tapeMount));
    if (!success) {
      // TODO: we need a better reason here
      putDriveDown("Data transfer session failed");
      // After this, the loop will continue by waiting to be up again
    }
    // Also, should we execute the cleaner here instead of having the sessions rely on doing this?
  }

  // At this point, the drive is exiting. Start cleanup
  executeCleanerSession();
  // TODO: correct exit code
  return 0;
}

void DriveHandler::executeCleanerSession() noexcept {}

bool DriveHandler::executeDataTransferSession(std::unique_ptr<TapeMount> tapeMount) {
  // This should not happen; just a double check
  if (tapeMount == nullptr) {
    // Something went wrong
    return false;
  }
  // TODO: these are not captured in the transferconfig
  // dataTransferConfig.useLbp = true;
  // dataTransferConfig.raoLtoAlgorithmOptions = "cost_heuristic_name:cta";  // Only option available
  // dataTransferConfig.tapeLoadTimeout = m_tapedConfig.mounts.tape_load_timeout_secs;
  // dataTransferConfig.xrootTimeout = 0;
  // dataTransferConfig.wdIdleSessionTimer = m_tapedConfig.mounts.idle_scheduling_interval_secs;
  // dataTransferConfig.driveStatePollIntervalSecs = m_tapedConfig.mounts.drive_state_poll_interval_secs;
  // dataTransferConfig.wdGetNextMountMaxSecs = m_tapedConfig.mounts.get_next_mount_timeout_secs;

  DataTransferSession dataTransferSession(utils::getShortHostname(),
                                          m_lc.logger(),
                                          m_sysWrapper,
                                          m_driveInfo,
                                          m_mediaChanger,
                                          m_config.transfers,
                                          *m_scheduler);
  // This is hacky; this whole end of session action stuff should be ripped out
  return dataTransferSession.execute() == EndOfSessionAction::MARK_DRIVE_AS_UP;
}

void DriveHandler::executeCleanerSession() {
  // TODO: refactor Cleanersession to figure out vid by itself?
  // Otherwise, take it fro tapeMount
  const auto cleanerSession = std::make_unique<CleanerSession>(m_mediaChanger,
                                                               m_lc.logger(),
                                                               m_driveInfo,
                                                               m_sysWrapper,
                                                               "",
                                                               true,
                                                               m_config.mounts.tape_load_timeout_secs,
                                                               "",
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