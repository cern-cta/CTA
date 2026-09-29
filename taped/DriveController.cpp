/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

#include "DriveSession.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"

#include <algorithm>
#include <exception>
#include <optional>

namespace cta::tape::daemon {

DriveController::DriveController(const TapedConfig& config, log::Logger& log, DriveOperations& operations)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_operations(operations) {}

void DriveController::stop() {
  // Always retain the exit request, even if catalogue publication fails.
  // Before registration completes, run() is responsible for publishing down on exit.
  if (!m_stopSource.request_stop() || !m_registered.load()) {
    return;
  }

  // The signal-reactor thread must not share the controller's scoped log parameters.
  log::LogContext lc(m_lc.logger());
  try {
    m_operations.requestDriveDown(lc);
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(lc);
    const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
    params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
    lc.log(log::ERR, "Failed to request drive down while stopping. Controller exit is still requested.");
  } catch (...) {
    lc.log(log::ERR, "Unknown failure requesting drive down while stopping. Controller exit is still requested.");
  }
}

bool DriveController::isLive() const {
  return isLive(std::chrono::steady_clock::now());
}

bool DriveController::isLive(std::chrono::steady_clock::time_point now) const {
  const auto snapshot = m_operations.tapeSessionLiveness();
  if (!snapshot || !snapshot->state) {
    return true;
  }

  auto lastActivity = snapshot->stateEnteredAt;
  uint32_t timeoutSecs;
  using enum cta::tape::session::TapeSessionState;
  // We are only allowed to spend a certain amount of time in each state
  // If we spend longer, it means we are stuck and isLive() should return false
  switch (*snapshot->state) {
    case Mounting:
      timeoutSecs = m_config.mounts.mount_timeout_secs;
      break;
    case Loading:
      timeoutSecs = m_config.mounts.tape_load_timeout_secs;
      break;
    case Unloading:
      timeoutSecs = m_config.mounts.tape_unload_timeout_secs;
      break;
    case Unmounting:
      timeoutSecs = m_config.mounts.unmount_timeout_secs;
      break;
    case Transferring:
      timeoutSecs = m_config.transfers.no_block_move_timeout_secs;
      lastActivity = std::max(lastActivity, snapshot->lastBlockMovement);
      break;
    case DrainingToDisk:
      timeoutSecs = m_config.transfers.retrieve.drain_to_disk_timeout_secs;
      break;
    case Preparing:
    case Finalizing:
    case Finished:
      return true;
    default:
      return true;
  }
  return now - lastActivity < std::chrono::seconds(timeoutSecs);
}

bool DriveController::isReady() const {
  // Taped is considered ready when the drive has been registered in the catalogue
  // One could argue that the logical library should also exist for it to be ready
  // But that currently presents problems with our system tests, where the
  // logical libraries are only created after the deployment is complete
  // (which requires all services to be ready)
  // Potentially something to improve in the future
  return m_registered.load();
}

int DriveController::run() {
  try {
    if (!registerDrive(false)) {
      return 1;
    }
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
    params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
    m_lc.log(log::ERR, "Drive registration failed. Exiting.");
    return 1;
  } catch (...) {
    m_lc.log(log::ERR, "Drive registration failed with an unknown exception. Exiting.");
    return 1;
  }

  bool failed = false;
  try {
    // An absent logical library can appear later, so wait before scheduling.
    // Scheduling can deal with a missing logical library just fine; this is just to reduce
    // the number of (transient) errors at startup
    waitForLogicalLibrary();
    // TODO (separate MR): graceful shutdown of active sessions and blocking operations.
    while (!m_stopSource.stop_requested()) {
      waitUntilDriveIsRequestedUp();
      if (m_stopSource.stop_requested()) {
        break;
      }
      if (auto session = DriveSession::create(m_config, m_lc.logger(), m_operations)) {
        session->run(m_stopSource.get_token());
      }
    }
  } catch (const Scheduler::NoSuchDrive& ex) {
    // A later daemon run will register the drive from scratch.
    m_registered.store(false);
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::ERR, "Drive is missing from the catalogue. Exiting.");
    return 1;
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
    params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
    m_lc.log(log::ERR, "Drive controller failed. Publishing down state before exit.");
    failed = true;
  } catch (...) {
    m_lc.log(log::ERR, "Drive controller failed with an unknown exception. Publishing down state before exit.");
    failed = true;
  }

  // Don't do drive cleanup here: a Down drive may be in use for other purposes
  // Tape sessions and drive-session recovery own physical cleanup.
  try {
    m_operations.scheduler().putDriveDown(m_driveInfo, common::dataStructures::DriveDownReason::Shutdown, m_lc);
  } catch (...) {
    // The helper logged each failure and attempted both down-state publications.
    failed = true;
  }
  // Don't remove our entry from the catalogue, but ensure the service is no longer ready
  m_registered.store(false);
  return failed ? 1 : 0;
}

void DriveController::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (!m_stopSource.stop_requested()) {
    const bool exists = m_operations.logicalLibraryExists();

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
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.logical_library_poll_interval_secs);
  }
}

void DriveController::waitUntilDriveIsRequestedUp() {
  auto& scheduler = m_operations.scheduler();
  bool waitingLogged = false;

  while (!m_stopSource.stop_requested()) {
    const auto desiredState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);

    if (desiredState.up) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive preparation.");
      }
      return;
    }

    if (!waitingLogged) {
      m_lc.log(log::INFO, "Waiting for the desired drive state to become up.");
      waitingLogged = true;
    }

    // Keep the catalogue timestamp fresh so a waiting drive is not shown as stale.
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::Down,
                                m_lc);
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

bool DriveController::registerDrive(bool putUpIfPossible) {
  m_registered.store(false);
  auto& scheduler = m_operations.scheduler();
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!scheduler.checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    m_lc.log(log::CRIT, "Cannot register the drive: its name belongs to a different host or logical library.");
    return false;
  }

  // Preserve an interrupted drive entry and its operator intent.
  const auto previous = m_operations.getDriveState();
  // Desired Up survives crashes and also represents an operator's pending up request.
  if (previous && previous->desiredUp) {
    // Keep the existing entry and operator intent. CleaningUp does not change desired-up.
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::CleaningUp,
                                m_lc);
    scheduler.reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
    m_lc.log(log::INFO, "Registered interrupted drive for recovery before scheduling.");
    m_registered.store(true);
    return true;
  }

  common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (const Scheduler::NoSuchDrive&) {
    m_lc.log(log::INFO, "Drive has no existing catalogue entry. Creating one.");
  }

  common::dataStructures::DesiredDriveState driveState;
  driveState.comment = currentDesiredDriveState.comment;
  // An up request may have arrived since the initial snapshot; preserve that intent too.
  if (currentDesiredDriveState.up) {
    driveState = currentDesiredDriveState;
  } else if (!currentDesiredDriveState.reason || currentDesiredDriveState.reason->empty()
             || common::dataStructures::isCleanDriveShutdownReason(*currentDesiredDriveState.reason)) {
    driveState.reason = common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
    driveState.up = putUpIfPossible;
  } else {
    driveState.reason = currentDesiredDriveState.reason;
  }

  common::dataStructures::SecurityIdentity securityIdentity;
  scheduler.createTapeDriveStatus(m_driveInfo,
                                  driveState,
                                  common::dataStructures::MountType::NoMount,
                                  common::dataStructures::DriveStatus::Down,
                                  securityIdentity,
                                  m_lc);
  scheduler.reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
  m_lc.log(log::INFO,
           "Drive registered with reported status down and desired state "
             + std::string(driveState.up ? "up." : "down."));
  m_registered.store(true);
  return true;
}

}  // namespace cta::tape::daemon
