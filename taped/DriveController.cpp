/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

#include "SystemDriveOperations.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"

#include <exception>

namespace cta::tape::daemon {

namespace {
void logDriveFailure(log::LogContext& lc, const char* message, const std::exception& ex) {
  log::ScopedParamContainer params(lc);
  if (const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex)) {
    params.add(semconv::log::exceptionMessage, ctaException->getMessageValue());
  } else {
    params.add(semconv::log::exceptionMessage, ex.what());
  }
  lc.log(log::ERR, message);
}
}  // namespace

DriveController::DriveController(const TapedConfig& config, log::Logger& log)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_ownedOperations(makeSystemDriveOperations(config, log, m_driveInfo)),
      m_operations(*m_ownedOperations) {}

DriveController::DriveController(const TapedConfig& config, log::Logger& log, DriveOperations& operations)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_operations(operations) {}

DriveController::~DriveController() = default;

void DriveController::stop() {
  m_stopSource.request_stop();
}

bool DriveController::isLive() const {
  // TODO (separate MR): look into the timeouts and see if we have spent too much time in any given state
  // We don't ping the catalogue/scheduler here as that would just result in cascading failures
  // A restart won't fix things
  return true;
}

bool DriveController::isReady() const {
  // TODO (separate MR): ping catalogue and scheduler
  return true;
}

int DriveController::run() {
  // TODO (separate MR): configure whether startup may request the drive to be up.
  try {
    // Start by registering the drive in the catalogue
    if (!registerDrive(false)) {
      return 1;
    }

    // An absent logical library can appear later, so wait before scheduling.
    // Scheduling can deal with a missing logical library just fine; this is just to reduce
    // the number of (transient) errors at startup
    waitForLogicalLibrary();
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, "Drive startup failed.", ex);
    return 1;
  }

  // TODO (separate MR): graceful shutdown
  // This is the main loop
  while (true) {
    runIteration();
  }

  // Do a final drive cleaning to ensure we don't leave a cartridge behind
  return shutdownDrive();
}

void DriveController::runIteration() {
  // Ensure among other things that the drive is Up before we proceed
  if (!prepareDriveForScheduling()) {
    return;
  }

  std::unique_ptr<TapeMount> tapeMount;

  // The tracker has a reference/pointer to the tapeMount. This RAII structure ensures we clear it before the tapeMount is destroyed.
  struct MountReferenceReset {
    TapeSessionTracker& tracker;

    ~MountReferenceReset() { tracker.setMount(nullptr); }
  } mountReferenceReset {m_tapeSessionTracker};

  // Acquire work; a scheduling timeout is recoverable by waiting and trying again.
  utils::Timer t;
  try {
    tapeMount = m_operations.getNextMount();
  } catch (const exception::TimeoutException& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("totalScheduleMountTime", t.secs())
      .add("scheduleMountTimeoutSecs", m_config.mounts.get_next_mount_timeout_secs)
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::WARNING, "Scheduling timed out; waiting before retrying.");
  } catch (const exception::LostDatabaseConnection& ex) {
    // If we lose connection, we log an error, wait for the DB to be up again and continue with the next iteration.
    logDriveFailure(m_lc, "Scheduling lost its database connection.", ex);
    waitForBackendRecovery();
    return;
  } catch (const std::exception& ex) {
    // TODO: inventory unexpected getNextMount() errors and decide which are recoverable.
    logDriveFailure(m_lc, "Scheduling failed unexpectedly.", ex);
    throw;
  }

  // Not finding a mount is not an error; we just sleep and retry in the next iteration
  if (tapeMount == nullptr) {
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
    return;
  }

  // At this point we know we have a "proper" mount candidate
  m_tapeSessionTracker.setMount(tapeMount.get());

  // The session result describes hardware usability, not whether every file transferred successfully.
  TransferSessionResult transferResult;

  try {
    // The transfer session handles mounting and cleaning as this is intertwined with its internal logic.
    // For example, if an empty mount is discovered inside the session, it does not load the tape.
    transferResult = m_operations.transfer(*tapeMount, m_tapeSessionTracker);
  } catch (const exception::LostDatabaseConnection& ex) {
    logDriveFailure(m_lc, "Data transfer lost its database connection. Cleaning before retrying scheduling.", ex);
    const bool cleaningSucceeded =
      cleanDrive(tapeMount->getVid(), "Drive cleaning after a transfer failure threw an exception.");

    // Successful cleaning does not establish that the backends are available again.
    // Wait before publishing a down state or attempting another mount.
    waitForBackendRecovery();
    if (!cleaningSucceeded) {
      putDriveDown(common::dataStructures::DriveDownReason::CleanerFailed, {}, true);
      return;
    }
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
    return;
  } catch (const std::exception& ex) {
    // Similar to losing the connection, except we don't wait for backend recovery
    logDriveFailure(m_lc, "Data transfer session threw an exception. Cleaning before retrying scheduling.", ex);
    if (!cleanDrive(tapeMount->getVid(), "Drive cleaning after a transfer failure threw an exception.")) {
      putDriveDown(common::dataStructures::DriveDownReason::CleanerFailed, {}, true);
      return;
    }
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
    return;
  }

  // The session result tells us something about whether the hardware is safe to reuse.
  // This is because cleaning happens inside the session as well
  if (transferResult.driveUsability != DriveUsability::Reusable) {
    // Preserve specific session or operator reasons. Publication failures propagate.
    putDriveDown(common::dataStructures::DriveDownReason::TransferSessionFailed, {}, true);
  }
}

void DriveController::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (true) {
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

void DriveController::waitForBackendRecovery() {
  // Scheduler::ping checks both the catalogue and scheduler backend.
  while (true) {
    try {
      m_operations.scheduler().ping(m_lc);
      return;
    } catch (const exception::LostDatabaseConnection& ex) {
      logDriveFailure(m_lc, "Database is still unavailable; waiting before retrying.", ex);
      m_operations.sleep(m_config.mounts.backend_recovery_interval_secs);
    }
  }
}

void DriveController::waitUntilDriveIsRequestedUp() {
  auto& scheduler = m_operations.scheduler();
  bool waitingLogged = false;

  // TODO (separate MR): graceful shutdown
  while (true) {
    common::dataStructures::DesiredDriveState desiredState;
    try {
      desiredState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
    } catch (const Scheduler::NoSuchDrive&) {
      m_lc.log(log::WARNING, "Drive is missing from the catalogue. Attempting to register it as down.");
      if (!registerDrive(false)) {
        throw exception::Exception(
          "In DriveController::waitUntilDriveIsRequestedUp(): failed to register the missing drive");
      }
      m_lc.log(log::INFO, "Missing drive registered as down. Waiting for an operator up request.");
    }

    if (desiredState.up) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive probing.");
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
  const auto recordFailure = [&](const char* message, const std::exception& ex) {
    if (!firstFailure) {
      firstFailure = std::current_exception();
    }
    logDriveFailure(m_lc, message, ex);
  };

  if (preserveExistingReason) {
    try {
      const auto currentState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
      const auto startupReason =
        common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
      if (currentState.reason && !currentState.reason->empty() && *currentState.reason != startupReason
          && !common::dataStructures::isCleanDriveShutdownReason(*currentState.reason)) {
        // Leave the catalogue reason untouched, including operator and session failure reasons.
        driveState.reason.reset();
      }
    } catch (const std::exception& ex) {
      driveState.reason.reset();
      recordFailure("Failed to read the existing drive-down reason.", ex);
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
  } catch (const std::exception& ex) {
    recordFailure("Failed to publish the reported down status.", ex);
  }

  try {
    scheduler.setDesiredDriveState(m_driveInfo.driveName, driveState, m_lc);
  } catch (const std::exception& ex) {
    recordFailure("Failed to publish the desired down state.", ex);
  }

  if (firstFailure) {
    std::rethrow_exception(firstFailure);
  }
}

bool DriveController::registerDrive(bool putUpIfPossible) {
  auto& scheduler = m_operations.scheduler();
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!scheduler.checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    m_lc.log(log::CRIT, "Cannot register the drive: its name belongs to a different host or logical library.");
    return false;
  }

  common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
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
  return true;
}

bool DriveController::prepareDriveForScheduling() {
  auto& scheduler = m_operations.scheduler();

  // A drive must be up before we can schedule.
  waitUntilDriveIsRequestedUp();

  // Verify the drive is empty before every scheduling attempt, including after transfer exceptions.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Probing,
                              m_lc);
  m_lc.log(log::DEBUG, "Checking whether the drive is empty before scheduling.");

  // A non-empty or failed probe prevents scheduling and requires another operator up request.
  const auto [empty, probeError] = m_operations.probeDrive();
  if (!empty) {
    m_lc.log(log::WARNING, "Drive probe did not confirm an empty drive. Requesting the drive down.");
    putDriveDown(probeError ? common::dataStructures::DriveDownReason::DriveProbeFailed :
                              common::dataStructures::DriveDownReason::TapeDetected,
                 probeError.value_or(""));
    return false;
  }
  m_lc.log(log::DEBUG, "No tape detected in the drive. Proceeding with scheduling.");

  // Advertise an idle drive with no active mount before asking the scheduler for work.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);
  // The transfer session owns reporting once a mount has been acquired.
  m_tapeSessionTracker.reportState(session::SessionState::Scheduling, session::SessionType::Undetermined);

  return true;
}

bool DriveController::cleanDrive(const std::optional<std::string>& vid, const char* failureMessage) {
  try {
    return m_operations.clean(vid, true, m_tapeSessionTracker);
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, failureMessage, ex);
    return false;
  }
}

int DriveController::shutdownDrive() {
  // Use an unknown VID; final cleanup does not depend on a surviving transfer mount.
  int exitCode = 0;
  bool cleaningSucceeded = false;

  // Cleanup failure must not prevent attempting to publish the down state.
  try {
    cleaningSucceeded = m_operations.clean(std::nullopt, true, m_tapeSessionTracker);
    if (!cleaningSucceeded) {
      m_lc.log(log::ERR, "Final drive cleaning failed.");
      exitCode = 1;
    }
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, "Final drive cleaning threw an exception.", ex);
    exitCode = 1;
  }

  // Preserve existing reasons; publish a clean shutdown only when cleaning succeeded.
  try {
    putDriveDown(cleaningSucceeded ? common::dataStructures::DriveDownReason::Shutdown :
                                     common::dataStructures::DriveDownReason::CleanerFailed,
                 {},
                 true);
  } catch (const std::exception&) {
    // The helper has logged each failure and attempted both down-state publications.
    exitCode = 1;
  }

  return exitCode;
}

}  // namespace cta::tape::daemon
