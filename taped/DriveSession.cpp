/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

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

namespace {
// Expected preparation outcome, caught only by create().
struct PreparationDeclined {};
}  // namespace

std::unique_ptr<DriveSession>
DriveSession::create(const TapedConfig& config, log::Logger& log, DriveOperations& operations) {
  try {
    return std::unique_ptr<DriveSession>(new DriveSession(config, log, operations));
  } catch (const PreparationDeclined&) {
    return nullptr;
  }
}

DriveSession::DriveSession(const TapedConfig& config, log::Logger& log, DriveOperations& operations)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_operations(operations) {
  try {
    if (!cleanDrive()) {
      throw PreparationDeclined {};
    }
  } catch (...) {
    // A failed constructor has no destructor; leave reported state down without masking the failure.
    release();
    throw;
  }
}

DriveSession::~DriveSession() noexcept {
  // TODO: review desired-down publication on DriveSession shutdown versus controller shutdown.
  // For now, preserve operator intent here so a new up request can start another session.
  // Future session-owned resources (such as a SCSI reservation) must be released here, before reporting Down.
  release();
}

void DriveSession::release() noexcept {
  try {
    if (m_operations.getDriveState()) {
      m_operations.scheduler().reportDriveStatus(m_driveInfo,
                                                 common::dataStructures::MountType::NoMount,
                                                 common::dataStructures::DriveStatus::Down,
                                                 m_lc);
    }
  } catch (...) {
    // Destruction and constructor rollback must preserve the original exception, even if logging fails.
    try {
      try {
        throw;
      } catch (const std::exception& ex) {
        log::ScopedParamContainer params(m_lc);
        const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
        params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
        m_lc.log(log::ERR, "Failed to publish Down while releasing drive session.");
      } catch (...) {
        m_lc.log(log::ERR, "Unknown failure publishing Down while releasing drive session.");
      }
    } catch (...) {}
  }
}

void DriveSession::run(std::stop_token stopToken) {
  while (!stopToken.stop_requested()) {
    const auto result = runIteration(stopToken);
    // The mount has been destroyed; object-store agents can now be retired safely.
    m_operations.resetScheduler();
    if (!result.driveReusable) {
      return;
    }
    if (!result.successful && !stopToken.stop_requested()) {
      // TODO (separate MR): interrupt sleeps and active transfers during graceful shutdown.
      m_operations.sleep(m_config.mounts.idle_scheduling_interval_secs);
    }
  }
}

// This method has the following invariant:
// - There is no tape in the drive when it enters this method
// - There is no tape in the drive when it returns a reusable outcome
// - There may be a tape in the drive when it returns a non-reusable outcome
TapeSessionResult DriveSession::runIteration(std::stop_token stopToken) {
  if (stopToken.stop_requested()) {
    return {.driveReusable = false};
  }
  auto& scheduler = m_operations.scheduler();
  const auto desired = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  const auto reported = m_operations.getDriveState();
  if (!reported) {
    throw Scheduler::NoSuchDrive("Drive disappeared before scheduling");
  }
  // TODO: track the end of hardware ownership explicitly instead of inferring it from catalogue status.
  // Keep the reported-Down check until then: desired UP may arrive before this loop observes desired DOWN.
  // That case must start a new drive session with preparation, rather than resume this one.
  if (!desired.up || reported->driveStatus == common::dataStructures::DriveStatus::Down) {
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
  if (stopToken.stop_requested()) {
    return {.driveReusable = false};
  }

  // Idle polls and scheduling errors both request the ordinary retry delay.
  // The mount is destroyed before the caller resets the scheduler.
  return tapeMount ? runTapeSession(*tapeMount) : TapeSessionResult {.successful = false};
}

TapeSessionResult DriveSession::runTapeSession(TapeMount& tapeMount) {
  const auto vid = tapeMount.getVid();
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

    // TODO: distinguish stop-induced desired-down from relinquished hardware ownership.
    // A session that throws after stop() publishes desired-down currently skips recovery cleanup.
    // Resolve this ownership distinction when adding session interruption.
    return {.driveReusable = cleanDrive(vid), .successful = false};
  }

  if (!transferResult.driveReusable) {
    // Preserve specific session or operator reasons. Publication failures propagate.
    m_operations.scheduler().putDriveDown(m_driveInfo,
                                          common::dataStructures::DriveDownReason::SessionLeftDriveUnusable,
                                          m_lc);
  }
  return transferResult;
}

bool DriveSession::cleanDrive(const std::optional<std::string>& vid) {
  auto& scheduler = m_operations.scheduler();
  // Desired Down relinquishes hardware access, including recovery after a failed tape session.
  if (!scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    return false;
  }
  const auto reported = m_operations.getDriveState();
  if (!reported) {
    throw Scheduler::NoSuchDrive("Drive disappeared before cleanup");
  }
  // The catalogue VID may be stale after external operations while Down.
  // Only a mount from this ownership period supplies a known recovery VID.
  // Claim hardware ownership before cleanup; reported Down permits operator access.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::CleaningUp,
                              m_lc);
  m_lc.log(log::INFO, "Cleaning drive before allowing scheduling.");
  bool cleaned = false;
  std::string cleanupError;
  try {
    cleaned = m_operations.clean(vid, true);
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
    scheduler.putDriveDown(m_driveInfo,
                           common::dataStructures::DriveDownReason::DriveCleanupFailed,
                           m_lc,
                           cleanupError);
    return false;
  }

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
