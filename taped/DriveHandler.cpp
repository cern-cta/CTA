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

#include <algorithm>
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

void DriveHandler::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (true) {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();
    const bool exists = std::any_of(libraries.begin(), libraries.end(), [this](const auto& library) {
      return library.name == m_driveInfo.logicalLibrary;
    });

    if (exists) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Logical library " + m_driveInfo.logicalLibrary + " is now available. Continuing startup.");
      }
      return;
    }

    if (!waitingLogged) {
      m_lc.log(log::WARNING,
               "Logical library " + m_driveInfo.logicalLibrary + " does not exist. Waiting for creation.");
      waitingLogged = true;
    }

    // Database failures propagate; only an absent library is retried here.
    // TODO (separate MR): make this startup wait interruptible by graceful shutdown.
    sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
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

int DriveHandler::run() {
  // TODO: handle startup exceptions separately from failures during an active session.
  // TODO: define bounded database recovery for each phase; helpers propagate operational failures.
  // TODO: wait for catalogue and scheduler to be reachable within a reasonable timeout.

  // Register the drive. It will put the drive down with a startup reason.
  // If the drive was already down with an existing reason, this reason will be carried over.
  // The only exception is if the down reason is a previous clean exit.
  // Registration conflicts prevent taped from using the drive.
  // TODO (separate MR): configure whether startup may request the drive to be up.
  if (!registerDrive(false)) {
    return 1;
  }

  // A drive may be defined with a logical library that does not exist (yet)
  // In which case scheduling will not work
  // The scheduler itself already handles this okay, but this is just to avoid spitting out a bunch
  // of repeated warnings.
  waitForLogicalLibrary();

  // TODO (separate MR): stop scheduling on shutdown and reach the final cleanup below.
  while (true) {
    // Honour the operator's desired state before scheduling another mount.
    // TODO: recover a missing drive here too; this lookup can fail before waitForDriveToBeUp().
    // TODO: handle desired-state lookup and status-publication failures at this phase boundary.
    // TODO: track whether probing is required locally so an early desired-state change cannot skip it.
    if (!m_scheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
      // Wait for an up request; the helper re-registers a missing drive as down.
      waitForDriveToBeUp();

      // Verify the drive is empty before allowing transfers.

      m_scheduler->reportDriveStatus(m_driveInfo,
                                     common::dataStructures::MountType::NoMount,
                                     common::dataStructures::DriveStatus::Probing,
                                     m_lc);
      EmptyDriveProbe emptyDriveProbe(m_lc.logger(), m_driveInfo, m_sysWrapper);
      m_lc.log(log::DEBUG, "Transition from down to up detected. Will check if a tape is in the drive.");

      // Startup recovery cleaning is currently disabled; an unknown VID lets the cleaner inspect loaded media.
      // TODO (separate MR): configure startup cleaning before probing.
      if (false) {
        // TODO: check the cleaner result and handle escaping exceptions before proceeding to the probe.
        executeCleanerSession();
      }

      // A non-empty or failed probe prevents scheduling and requires another operator up request.
      // TODO: handle probe exceptions without allowing transfers on an unverified drive.
      if (!emptyDriveProbe.driveIsEmpty()) {
        // TODO: distinguish a detected tape from a failed probe when reporting the drive-down reason.
        // TODO: log the probe outcome at warning severity.
        putDriveDown(common::dataStructures::DriveDownReason::TapeDetected,
                     emptyDriveProbe.getProbeErrorMsg().value_or(""));
        // Re-enter desired-state polling after successfully requesting down.
        continue;
      } else {
        m_lc.log(log::DEBUG, "No tape detected in the drive. Proceeding with scheduling.");
      }
    }

    // Advertise an idle drive with no active mount before asking the scheduler for work.
    m_scheduler->reportDriveStatus(m_driveInfo,
                                   common::dataStructures::MountType::NoMount,
                                   common::dataStructures::DriveStatus::Up,
                                   m_lc);
    // TODO: rip out session reporting
    // tapeSessionReporter.reportState(::session::SessionState::Scheduling,
    //                                 ::session::SessionType::Undetermined);

    // Retain mount ownership until the transfer and its reporting threads have finished.
    std::unique_ptr<TapeMount> tapeMount;

    // Clear the borrowed tracker reference before destroying the mount, including during unwinding.
    struct MountReferenceReset {
      TapeSessionTracker& tracker;

      ~MountReferenceReset() { tracker.setMount(nullptr); }
    } mountReferenceReset {m_tapeSessionTracker};

    // Acquire work; a scheduling timeout is recoverable by waiting and trying again.
    // TODO: handle other scheduling failures separately from transfer failures.
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
      // TODO: make idle waiting responsive to desired-state changes and graceful shutdown.
      sleep(m_config.mounts.idle_scheduling_interval_secs);
      // Recheck desired state before attempting scheduling again.
      continue;
    }

    // TODO: if no mount is available, wait and continue instead of treating nullptr as a transfer failure.
    // The session result describes hardware usability, not whether every file transferred successfully.
    // TODO: handle transfer exceptions here and establish safe hardware recovery before scheduling again.
    // TODO: ensure session workers and reporters are joined during exception unwinding.
    bool driveCanRemainUp = tapeMount != nullptr && executeDataTransferSession(*tapeMount);
    if (!driveCanRemainUp) {
      // TODO: preserve more specific drive-open or cleaning reasons already recorded by the session.
      // TODO: if putting the drive down fails, retain both the original failure and the reporting failure.
      putDriveDown(common::dataStructures::DriveDownReason::TransferSessionFailed);
      // Require another operator up request before attempting recovery and scheduling.
    }
    // TODO (separate MR): move transfer cleaning to CleanerSession, called after every transfer here.
  }

  // Final cleanup is currently unreachable because the scheduling loop never exits.
  // Use an unknown VID; final cleanup does not depend on a surviving transfer mount.
  // TODO: handle the cleaner result and exceptions while still attempting shutdown status publication.
  executeCleanerSession();
  // Publish a clean shutdown reason after cleanup.
  // TODO: preserve an existing failure reason and account for failed cleanup.
  putDriveDown(common::dataStructures::DriveDownReason::Shutdown);
  // TODO: select the exit code from the shutdown and cleanup outcomes.
  return 0;
}

}  // namespace cta::tape::daemon
