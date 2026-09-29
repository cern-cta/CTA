/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "session/TapeSessionWorkerTeardownIncomplete.hpp"

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
  m_stopSource.request_stop();
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
      // Execute whatever we need on a Down -> Up transition
      if (!onDownToUpTransition()) {
        continue;
      }
      // The main loop while we are Up
      while (!m_stopSource.stop_requested()) {
        const auto result = runIteration();
        // This reset is only necessary for the objectstore; once this is removed we can just stick with a single
        // scheduler init similar to the catalogue.
        // The reason is that we need a new agent for every TapeSession to ensure correct garbage collection in the event of failures
        m_operations.resetScheduler();
        if (!result.driveReusable) {
          break;
        }
        // Don't sleep after successful sessions: that's just wasted time
        if (!result.successful && !m_stopSource.stop_requested()) {
          // TODO (separate MR): graceful shutdown should interrupt sleep.
          m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
        }
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
  // Either a tape is not loaded and we don't need to do cleanup anyway,
  // or a tape was loaded, in which case runIteration will have tried to clean it already
  try {
    putDriveDown(common::dataStructures::DriveDownReason::Shutdown, {}, true);
  } catch (...) {
    // The helper logged each failure and attempted both down-state publications.
    failed = true;
  }
  // Don't remove our entry from the catalogue, but ensure the service is no longer ready
  m_registered.store(false);
  return failed ? 1 : 0;
}

// This method has the following invariant:
// - There is no tape in the drive when it enters this method
// - There is no tape in the drive when it returns a reusable outcome
// - There may be a tape in the drive when it returns a non-reusable outcome
TapeSessionResult DriveController::runIteration() {
  if (m_stopSource.stop_requested()) {
    return {.driveReusable = false};
  }
  auto& scheduler = m_operations.scheduler();
  const auto desired = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  const auto reported = m_operations.getDriveState();
  if (!reported) {
    throw Scheduler::NoSuchDrive("Drive disappeared before scheduling");
  }
  // Check if we should go down
  if (!desired.up || reported->driveStatus == common::dataStructures::DriveStatus::Down) {
    // We goin downnn
    // Given the invariants above, no need to clean the drive here
    return {.driveReusable = false};
  }
  // Report that we are Up without a mount
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);

  std::unique_ptr<TapeMount> tapeMount;

  // Try to get a mount. A scheduling timeout is recoverable by waiting and trying again.
  utils::Timer t;
  try {
    tapeMount = m_operations.getNextMount();
  } catch (const exception::TimeoutException& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("totalScheduleMountTime", t.secs())
      .add("scheduleMountTimeoutSecs", m_config.mounts.get_next_mount_timeout_secs)
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::WARNING, "Scheduling timed out; waiting before retrying.");
  } catch (const Scheduler::NoSuchDrive&) {
    throw;
  } catch (const std::exception& ex) {
    // No transfer has started; retry within this up period after the idle delay.
    log::ScopedParamContainer params(m_lc);
    const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
    params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
    m_lc.log(log::ERR, "Scheduling failed unexpectedly; waiting before retrying.");
  }

  // Do another quick check to see if we should stop before committing to a tape session
  // Later on, the tape session internals will also react appropriately
  if (m_stopSource.stop_requested()) {
    return {.driveReusable = false};
  }

  // Idle polls and scheduling errors both request the ordinary retry delay.
  // The mount is destroyed before the caller resets the scheduler.
  return tapeMount ? runTapeSession(*tapeMount) : TapeSessionResult {.successful = false};
}

TapeSessionResult DriveController::runTapeSession(TapeMount& tapeMount) {
  m_cleanupVid = tapeMount.getVid();
  TapeSessionResult transferResult;
  try {
    // Run a tape session
    transferResult = m_operations.runTapeSession(tapeMount);
  } catch (const TapeSessionWorkerTeardownIncomplete&) {
    // Cleanup cannot establish safe reuse while a worker may still access the drive.
    throw;
  } catch (...) {
    // Log the original failure before recovery, without attaching its parameters to cleanup logs.
    try {
      throw;
    } catch (const std::exception& ex) {
      log::ScopedParamContainer params(m_lc);
      const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
      params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
      m_lc.log(log::ERR, "Tape session failed. Attempting drive recovery.");
    } catch (...) {
      m_lc.log(log::ERR, "Tape session failed with an unknown exception. Attempting drive recovery.");
    }

    // Down relinquishes hardware ownership, including when stopping after a failed session.
    return {.driveReusable =
              m_operations.scheduler().getDesiredDriveState(m_driveInfo.driveName, m_lc).up && onDownToUpTransition(),
            .successful = false};
  }

  if (!transferResult.driveReusable) {
    // Preserve specific session or operator reasons. Publication failures propagate.
    putDriveDown(common::dataStructures::DriveDownReason::SessionLeftDriveUnusable, {}, true);
  } else {
    m_cleanupVid.reset();
  }
  return transferResult;
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

void DriveController::putDriveDown(common::dataStructures::DriveDownReason reason,
                                   std::string_view detail,
                                   bool preserveExistingReason) {
  auto& scheduler = m_operations.scheduler();
  common::dataStructures::DesiredDriveState driveState;
  driveState.reason = common::dataStructures::formatDriveDownReason(reason, detail);
  // This allows us to rethrow only the first exception we encountered
  std::exception_ptr firstFailure;
  const auto recordFailure = [&](const char* message) {
    if (!firstFailure) {
      firstFailure = std::current_exception();
    }
    try {
      std::rethrow_exception(std::current_exception());
    } catch (const std::exception& ex) {
      log::ScopedParamContainer params(m_lc);
      const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
      params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
      m_lc.log(log::ERR, message);
    } catch (...) {
      m_lc.log(log::ERR, message);
    }
  };

  if (preserveExistingReason) {
    try {
      const auto currentState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
      const auto startupReason =
        common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
      if (!currentState.up && currentState.reason && !currentState.reason->empty()
          && *currentState.reason != startupReason
          && !common::dataStructures::isCleanDriveShutdownReason(*currentState.reason)) {
        // Leave the catalogue reason untouched, including operator and session failure reasons.
        driveState.reason.reset();
      }
    } catch (...) {
      driveState.reason.reset();
      recordFailure("Failed to read the existing drive-down reason.");
    }
  }

  if (driveState.reason) {
    m_lc.logEvent(common::dataStructures::driveDownReasonSeverity(reason),
                  *driveState.reason,
                  semconv::log::EventNameValues::kPuttingTapeDriveDown);
  }

  // Failure of one publication must not prevent attempting the other.
  try {
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::Down,
                                m_lc);
  } catch (...) {
    recordFailure("Failed to publish the reported down status.");
  }

  try {
    scheduler.setDesiredDriveState(m_driveInfo.driveName, driveState, m_lc);
  } catch (...) {
    recordFailure("Failed to publish the desired down state.");
  }

  if (firstFailure) {
    std::rethrow_exception(firstFailure);
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

  // Registration normally replaces the catalogue record, so capture recovery context first.
  const auto previous = m_operations.getDriveState();
  m_cleanupVid = previous ? previous->currentVid : std::nullopt;
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

bool DriveController::onDownToUpTransition() {
  auto& scheduler = m_operations.scheduler();
  const auto reported = m_operations.getDriveState();
  if (!reported) {
    throw Scheduler::NoSuchDrive("Drive disappeared before cleanup");
  }
  // A failed session's captured VID takes precedence over a stale catalogue snapshot.
  if (!m_cleanupVid && reported->currentVid && !reported->currentVid->empty()) {
    m_cleanupVid = reported->currentVid;
  }
  // Claim hardware ownership before cleanup; reported Down permits operator access.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::CleaningUp,
                              m_lc);
  m_lc.log(log::INFO, "Cleaning drive before allowing scheduling.");
  bool cleaned = false;
  std::string cleanupError;
  try {
    cleaned = m_operations.clean(m_cleanupVid, true);
  } catch (const cta::exception::Exception& ex) {
    cleanupError = ex.getMessageValue();
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, cleanupError);
    m_lc.log(log::ERR, "Drive recovery cleaning failed.");
  } catch (const std::exception& ex) {
    cleanupError = ex.what();
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, cleanupError);
    m_lc.log(log::ERR, "Drive recovery cleaning failed.");
  } catch (...) {
    cleanupError = "Unknown exception during drive cleanup";
    m_lc.log(log::ERR, "Drive recovery cleaning failed with an unknown exception.");
  }

  if (!cleaned) {
    putDriveDown(common::dataStructures::DriveDownReason::DriveCleanupFailed, cleanupError, true);
    return false;
  }

  m_cleanupVid.reset();

  // Cleaning takes time; an operator may have withdrawn the up request while it ran.
  // Never publish desired-up here. The catalogue also gates reported Up on current desired state.
  if (!scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    // Cleanup is complete. Honour the operator's down request without replacing its reason.
    m_operations.scheduler().reportDriveStatus(m_driveInfo,
                                               common::dataStructures::MountType::NoMount,
                                               common::dataStructures::DriveStatus::Down,
                                               m_lc);
    return false;
  }
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);
  return true;
}

}  // namespace cta::tape::daemon
