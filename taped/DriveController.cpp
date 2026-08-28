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
#include "scheduler/TapeMount.hpp"

#include <exception>
#include <optional>

namespace cta::tape::daemon {

namespace {
/**
 * @brief Log a drive-lifecycle exception, using the CTA message when available.
 *
 * @param lc Log context for diagnostics.
 * @param message Context describing the failed drive operation.
 * @param ex Exception whose diagnostic is written to the log.
 */
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
  return m_registered.load();
}

int DriveController::run() {
  bool failed = false;
  try {
    if (!registerDrive(false)) {
      return 1;
    }

    // An absent logical library can appear later, so wait before scheduling.
    // Scheduling can deal with a missing logical library just fine; this is just to reduce
    // the number of (transient) errors at startup
    waitForLogicalLibrary();
    // TODO (separate MR): graceful shutdown
    while (true) {
      runIteration();
    }
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, "Drive controller failed. Publishing down state before exit.", ex);
    failed = true;
  } catch (...) {
    m_lc.log(log::ERR, "Drive controller failed with an unknown exception. Publishing down state before exit.");
    failed = true;
  }

  m_registered.store(false);
  // A partial registration still needs down publication, but never modify another drive's identity.
  const int shutdownResult = m_identityValidated ? shutdownDrive() : 0;
  return failed ? 1 : shutdownResult;
}

void DriveController::runIteration() {
  // Ensure among other things that the drive is Up before we proceed
  if (!prepareDriveForScheduling()) {
    return;
  }

  std::unique_ptr<TapeMount> tapeMount;

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
    // No transfer has started; retry through normal drive preparation after the idle delay.
    logDriveFailure(m_lc, "Scheduling failed unexpectedly; waiting before retrying.", ex);
  }

  // Wait before retrying when no mount was found or scheduling failed without acquiring one.
  if (tapeMount == nullptr) {
    // TODO (separate MR): graceful shutdown should interrupt sleep
    m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
    return;
  }

  // TapeSession handles recoverable failures; escaping exceptions are fatal and reach run().
  const auto transferResult = m_operations.runTapeSession(*tapeMount);

  if (transferResult.backendRecoveryRequired) {
    waitForBackendRecovery();
  }
  if (transferResult.driveUsability != DriveUsability::Reusable) {
    m_cleanupVid = tapeMount->getVid();
    // Preserve specific session or operator reasons. Publication failures propagate.
    putDriveDown(common::dataStructures::DriveDownReason::SessionLeftDriveUnusable, {}, true);
  }
  if (transferResult.retryDelayRequired && transferResult.driveUsability == DriveUsability::Reusable) {
    m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
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
        throw exception::Exception("Failed to register the missing drive");
      }
      m_lc.log(log::INFO, "Missing drive registered as down. Waiting for an operator up request.");
    }

    if (desiredState.up) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive probing.");
      }
      return;
    }

    // An operator may use the drive while it is down. Clean again on the next up request.
    if (!m_cleanBeforeScheduling) {
      const auto reported = m_operations.getDriveState();
      m_cleanupVid = reported ? reported->currentVid : std::nullopt;
    }
    m_cleanBeforeScheduling = true;

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
  m_cleanBeforeScheduling = true;
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
      logDriveFailure(m_lc, message, ex);
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
  m_identityValidated = false;
  auto& scheduler = m_operations.scheduler();
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!scheduler.checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    m_lc.log(log::CRIT, "Cannot register the drive: its name belongs to a different host or logical library.");
    return false;
  }

  m_identityValidated = true;
  m_cleanBeforeScheduling = true;
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

bool DriveController::prepareDriveForScheduling() {
  auto& scheduler = m_operations.scheduler();

  // A drive must be up before we can schedule.
  waitUntilDriveIsRequestedUp();

  const auto reported = m_operations.getDriveState();
  if (reported && reported->currentVid && !reported->currentVid->empty()) {
    m_cleanupVid = reported->currentVid;
  }
  if (!m_cleanBeforeScheduling) {
    // Session publication can become Down before a new up request, without the loop observing desired Down.
    m_cleanBeforeScheduling = reported && reported->driveStatus == common::dataStructures::DriveStatus::Down;
  }

  if (m_cleanBeforeScheduling) {
    // Mark the up transition before probing or cleaning; Down relinquishes hardware ownership.
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::CleaningUp,
                                m_lc);
  }

  // Probe once, before cleanup can remove unexpected media.
  m_lc.log(log::DEBUG, "Checking whether the drive is empty.");
  bool empty = false;
  std::optional<std::string> probeError;
  try {
    const auto result = m_operations.probeDrive();
    empty = result.first;
    probeError = result.second;
  } catch (...) {
    if (!m_cleanBeforeScheduling) {
      throw;
    }
    m_lc.log(log::WARNING, "Drive probe failed before preparation. Attempting drive cleanup.");
    probeError = "Drive probe failed before preparation";
  }

  if (m_cleanBeforeScheduling) {
    if (!empty && !probeError) {
      m_lc.log(log::WARNING, "Tape found in drive while preparing to bring it up. Attempting drive cleanup.");
    }
    // Successful cleanup establishes readiness even when the diagnostic probe failed.
    if (!cleanBeforeScheduling()) {
      return false;
    }
  } else if (!empty) {
    // Without cleanup, a non-empty or failed probe requires another operator up request.
    m_lc.log(log::WARNING, "Drive probe did not confirm an empty drive. Requesting the drive down.");
    putDriveDown(probeError ? common::dataStructures::DriveDownReason::DriveProbeFailed :
                              common::dataStructures::DriveDownReason::TapeDetected,
                 probeError.value_or(""));
    return false;
  }
  m_lc.log(log::DEBUG, "Drive is ready. Proceeding with scheduling.");

  // Advertise an idle drive with no active mount before asking the scheduler for work.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);

  return true;
}

bool DriveController::cleanBeforeScheduling() {
  m_lc.log(log::INFO, "Cleaning drive before allowing scheduling.");
  bool cleaned = false;
  std::string cleanupError;
  try {
    cleaned = m_operations.clean(m_cleanupVid, true);
  } catch (const cta::exception::Exception& ex) {
    cleanupError = ex.getMessageValue();
    logDriveFailure(m_lc, "Drive recovery cleaning failed.", ex);
  } catch (const std::exception& ex) {
    cleanupError = ex.what();
    logDriveFailure(m_lc, "Drive recovery cleaning failed.", ex);
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
  m_cleanBeforeScheduling = !m_operations.scheduler().getDesiredDriveState(m_driveInfo.driveName, m_lc).up;
  if (m_cleanBeforeScheduling) {
    // Cleanup is complete. Honour the operator's down request without replacing its reason.
    m_operations.scheduler().reportDriveStatus(m_driveInfo,
                                               common::dataStructures::MountType::NoMount,
                                               common::dataStructures::DriveStatus::Down,
                                               m_lc);
  }
  return !m_cleanBeforeScheduling;
}

int DriveController::shutdownDrive() {
  int exitCode = 0;

  // Sessions own tape cleanup; a down drive may be in use by an operator.
  try {
    putDriveDown(common::dataStructures::DriveDownReason::Shutdown, {}, true);
  } catch (...) {
    // The helper has logged each failure and attempted both down-state publications.
    exitCode = 1;
  }

  return exitCode;
}

}  // namespace cta::tape::daemon
