/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

#include "DriveStatePublication.hpp"
#include "SchedulerContext.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/ScopeExit.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "session/DriveCleaner.hpp"
#include "session/TapeSession.hpp"
#include "session/TapeSessionWorkerTeardownIncomplete.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>
#include <unistd.h>

namespace cta::tape::daemon {

std::unique_ptr<DriveSession>
DriveSession::create(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext) {
  auto session = std::unique_ptr<DriveSession>(new DriveSession(config, log, schedulerContext));
  try {
    if (!session->cleanDrive()) {
      session->m_hardwareOwnership.release();
      return nullptr;
    }
  } catch (...) {
    // Destruction releases the session without replacing the preparation failure.
    session->requestDownNoThrow(common::dataStructures::DriveDownReason::UnexpectedFailure, "Drive preparation failed");
    throw;
  }
  return session;
}

DriveSession::DriveSession(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_schedulerContext(schedulerContext),
      m_mediaChanger(mediachanger::RmcProxy(config.rmcd.host,
                                            config.rmcd.port,
                                            config.rmcd.request_timeout_secs,
                                            config.rmcd.request_attempts),
                     log),
      m_hardwareOwnership(m_driveInfo, m_lc, m_schedulerContext) {}

DriveSession::~DriveSession() noexcept = default;

void DriveSession::requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail) {
  requestDriveDown(m_schedulerContext.scheduler().getCatalogue(), m_driveInfo.driveName, reason, m_lc, detail);
}

void DriveSession::requestDownNoThrow(common::dataStructures::DriveDownReason reason,
                                      std::string_view detail) noexcept {
  try {
    requestDown(reason, detail);
  } catch (...) {
    try {
      m_lc.log(log::ERR, "Failed to request desired Down while handling a drive-session failure.");
    } catch (...) {}
  }
}

bool DriveSession::isLive() const {
  const auto now = std::chrono::steady_clock::now();
  const auto tracker = std::atomic_load(&m_activeTracker);
  if (!tracker) {
    return true;
  }
  const auto snapshot = tracker->livenessSnapshot();
  if (!snapshot.state) {
    return true;
  }

  auto lastActivity = snapshot.stateEnteredAt;
  uint32_t timeoutSecs;
  using enum cta::tape::session::TapeSessionState;
  // We are only allowed to spend a certain amount of time in each state
  // If we spend longer, it means we are stuck and isLive() should return false
  switch (*snapshot.state) {
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
      lastActivity = std::max(lastActivity, snapshot.lastBlockMovement);
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

void DriveSession::run(std::stop_token stopToken) {
  try {
    while (m_hardwareOwnership.ownsHardware() && !stopToken.stop_requested()) {
      const auto result = runIteration(stopToken);
      if (!result.driveReusable) {
        break;
      }
      if (!result.successful && !stopToken.stop_requested()) {
        // TODO (separate MR): interrupt sleeps and active transfers during graceful shutdown.
        ::sleep(m_config.mounts.idle_scheduling_interval_secs);
      }
    }
    m_hardwareOwnership.release();
  } catch (const TapeSessionWorkerTeardownIncomplete& ex) {
    // The daemon must exit without announcing that hardware access has stopped.
    m_hardwareOwnership.markUnsafe();
    requestDownNoThrow(common::dataStructures::DriveDownReason::SessionDidNotStopSafely, ex.what());
    throw;
  } catch (...) {
    requestDownNoThrow(common::dataStructures::DriveDownReason::UnexpectedFailure, "Drive session failed");
    throw;
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
  auto& scheduler = m_schedulerContext.scheduler();
  const auto desired = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  if (!desired.up) {
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
    // The dry run avoids constructing an assignment when no work is eligible.
    if (scheduler.getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
      tapeMount = scheduler.getNextMount(m_driveInfo.logicalLibrary,
                                         m_driveInfo.driveName,
                                         m_lc,
                                         static_cast<uint64_t>(m_config.mounts.get_next_mount_timeout_secs) * 1000000);
    }
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
  if (!tapeMount) {
    return {.successful = false};
  }

  const auto result = runTapeSession(*tapeMount);
  // Destroy the mount before retiring the scheduler resources it borrows.
  // This is only necessary in the objectstore
  tapeMount.reset();
  m_schedulerContext.retire();
  return result;
}

TapeSessionResult DriveSession::runTapeSession(TapeMount& tapeMount) {
  const auto vid = tapeMount.getVid();
  std::optional<TapeSessionResult> transferResult;
  try {
    TapeSession session(m_lc.logger(),
                        m_sysWrapper,
                        m_driveInfo,
                        m_mediaChanger,
                        tapeMount,
                        m_config.transfers,
                        m_config.mounts.tape_load_timeout_secs,
                        m_schedulerContext.scheduler());
    // Health readers retain the tracker without extending the borrowed mount's lifetime.
    std::atomic_store<const TapeSessionTracker>(&m_activeTracker, session.sharedTracker());
    const utils::ScopeExit clearActiveTracker(
      [this] { std::atomic_store<const TapeSessionTracker>(&m_activeTracker, nullptr); });
    transferResult = session.execute();
  } catch (const TapeSessionWorkerTeardownIncomplete&) {
    // Cleanup cannot establish safe reuse while a worker may still access the drive.
    throw;
  } catch (const exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::ERR, "Tape session failed. Attempting drive recovery.");
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.what());
    m_lc.log(log::ERR, "Tape session failed. Attempting drive recovery.");
  } catch (...) {
    m_lc.log(log::ERR, "Tape session failed with an unknown exception. Attempting drive recovery.");
  }

  if (!transferResult) {
    // The session and its diagnostic log parameters are gone before recovery starts.
    // Desired Down requests stopping work; ownership is retained until release().
    return {.driveReusable = cleanDrive(vid), .successful = false};
  }

  if (!transferResult->driveReusable) {
    // Preserve specific session or operator reasons. Publication failures propagate.
    requestDown(transferResult->downReason.value_or(common::dataStructures::DriveDownReason::SessionLeftDriveUnusable),
                transferResult->downDetail);
  }
  return *transferResult;
}

bool DriveSession::cleanDrive(const std::optional<std::string>& vid) {
  auto& scheduler = m_schedulerContext.scheduler();
  // A new ownership period needs permission; recovery retains access until explicit release.
  if (!m_hardwareOwnership.ownsHardware() && !scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    return false;
  }
  const auto reported = scheduler.getCatalogue().DriveState()->getTapeDrive(m_driveInfo.driveName);
  if (!reported) {
    throw Scheduler::NoSuchDrive("Drive disappeared before cleanup");
  }
  m_hardwareOwnership.acquire();
  m_lc.log(log::INFO, "Cleaning drive before allowing scheduling.");
  bool cleaned = false;
  std::string cleanupError;
  try {
    TapeSessionTracker tracker;
    DriveCleaner cleaner(m_mediaChanger,
                         m_lc.logger(),
                         m_driveInfo,
                         vid.value_or(""),
                         true,
                         m_config.mounts.tape_load_timeout_secs,
                         scheduler.getCatalogue(),
                         tracker);
    cleaned = cleaner.execute(m_sysWrapper);
    cleanupError = cleaner.errorMessage();
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
    requestDown(common::dataStructures::DriveDownReason::DriveCleanupFailed, cleanupError);
    return false;
  }

  // Cleaning takes time; an operator may have withdrawn the up request while it ran.
  // Never publish desired-up here; a withdrawn request ends this ownership period.
  if (!scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    // The caller releases ownership and reports Down without overwriting newer operator intent.
    return false;
  }
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);
  return true;
}

}  // namespace cta::tape::daemon
