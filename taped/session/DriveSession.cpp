/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

#include "TapeSession.hpp"
#include "TapeSessionReporter.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/ExceptionLogging.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/ScopeExit.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/SchedulerContext.hpp"
#include "taped/drive/DriveCleaner.hpp"
#include "taped/drive/EmptyDriveProbe.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>
#include <unistd.h>

namespace cta::tape::daemon {
std::unique_ptr<DriveSession>
DriveSession::create(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext) {
  return std::unique_ptr<DriveSession>(new DriveSession(config, log, schedulerContext));
}

std::unique_ptr<DriveSession> DriveSession::create(const TapedConfig& config,
                                                   log::Logger& log,
                                                   SchedulerContext& schedulerContext,
                                                   System::virtualWrapper& sysWrapper) {
  return std::unique_ptr<DriveSession>(new DriveSession(config, log, schedulerContext, &sysWrapper));
}

DriveSession::DriveSession(const TapedConfig& config,
                           log::Logger& log,
                           SchedulerContext& schedulerContext,
                           System::virtualWrapper* sysWrapper)
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
      m_sysWrapper(sysWrapper ? *sysWrapper : m_realSysWrapper),
      m_driveReservation(m_lc) {
  m_lc.push(log::Param("tapeDrive", m_driveInfo.driveName));
}

DriveSession::~DriveSession() noexcept {
  try {
    releaseAndReportDown();
  } catch (...) {
    log::logCurrentExceptionNoThrow(m_lc, "Failed to release drive session or publish Down.");
  }
}

void DriveSession::releaseAndReportDown() {
  // Ensure we don't report Down multiple times as we can call this in run() but also in the destructor.
  if (m_downPublicationComplete) {
    return;
  }
  // Release the hardware first
  m_driveReservation.release();
  auto& scheduler = m_schedulerContext.scheduler();
  // Only report: either the operator already put Desired Down or the daemon itself requested Desired Down.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Down,
                              m_lc);
  m_downPublicationComplete = true;
}

void DriveSession::requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail) {
  TapeDrivesCatalogueState(m_schedulerContext.scheduler().getCatalogue())
    .requestDriveDown(m_driveInfo.driveName, reason, m_lc, detail);
}

void DriveSession::requestDownNoThrow(common::dataStructures::DriveDownReason reason,
                                      std::string_view detail) noexcept {
  try {
    requestDown(reason, detail);
  } catch (...) {
    log::logCurrentExceptionNoThrow(m_lc, "Failed to request desired Down while handling a drive-session failure.");
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
  // We are only allowed to spend a certain amount of time in each state.
  // If we spend longer, it means we are stuck and isLive() should return false.
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
      timeoutSecs = m_config.mounts.preparing_timeout_secs;
      break;
    case Finalizing:
      timeoutSecs = m_config.mounts.finalizing_timeout_secs;
      break;
    case Finished:
      return true;
    default:
      return true;
  }
  return now - lastActivity < std::chrono::seconds(timeoutSecs);
}

void DriveSession::run(std::stop_token stopToken) {
  if (!stopToken.stop_requested()) {
    try {
      // Clean the drive (if configured to do so; otherwise exit and report Down when there is already a tape there)
      if (!prepareDrive()) {
        releaseAndReportDown();
        return;
      }
    } catch (...) {
      requestDownNoThrow(common::dataStructures::DriveDownReason::UnexpectedFailure, "Drive preparation failed");
      throw;
    }
  }

  try {
    std::string endReason = "Stop requested";
    // The main loop: run one or more tape sessions
    while (m_driveReservation.canUseHardware() && !stopToken.stop_requested()) {
      const auto result = runIteration(stopToken);
      if (result.action == IterationAction::EndOwnership) {
        endReason = result.endReason;
        break;
      }
      if (result.action == IterationAction::RetryAfterDelay && !stopToken.stop_requested()) {
        ::sleep(m_config.mounts.idle_scheduling_interval_secs);
      }
    }
    {
      log::ScopedParamContainer params(m_lc);
      params.add("endReason", endReason);
      m_lc.log(log::INFO, "Drive session ending.");
    }
    releaseAndReportDown();
  } catch (...) {
    requestDownNoThrow(common::dataStructures::DriveDownReason::UnexpectedFailure, "Drive session failed");
    throw;
  }
}

// This method has the following invariants:
// - There is no tape in the drive when it enters this method
// - Continue and RetryAfterDelay leave the drive empty and reusable
// - EndOwnership may leave a tape in the drive only after a failed cleanup
DriveSession::IterationResult DriveSession::runIteration(std::stop_token stopToken) {
  if (stopToken.stop_requested()) {
    return {IterationAction::EndOwnership, "Stop requested"};
  }
  auto& scheduler = m_schedulerContext.scheduler();
  const auto desired = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  if (!desired.up) {
    return {IterationAction::EndOwnership, "Desired drive state is Down"};
  }
  // Report that we are Up without a mount
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);

  std::unique_ptr<TapeMount> mountCandidate;

  // Try to get a mount candidate. A scheduling timeout is recoverable by waiting and trying again.
  utils::Timer t;
  try {
    // The dry run avoids constructing an assignment when no work is eligible.
    if (scheduler.getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
      mountCandidate =
        scheduler.getNextMount(m_driveInfo.logicalLibrary,
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
  } catch (const TapeDrivesCatalogueState::NoSuchDrive&) {
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
    return {IterationAction::EndOwnership, "Stop requested"};
  }

  // Idle polls and scheduling errors both request the ordinary retry delay.
  if (!mountCandidate) {
    return {IterationAction::RetryAfterDelay, {}};
  }

  // The tape is not yet mounted; this happens in the tape session.
  const auto result = runTapeSession(*mountCandidate);
  // At this point the tape session is over and the drive has already been cleaned.
  // Destroy the mount before retiring the scheduler resources it borrows.
  mountCandidate.reset();

  // Can happen if the cartridge got stuck and we could not clean the drive.
  if (!result.driveReusable) {
    return {IterationAction::EndOwnership,
            result.downDetail.empty() ? "Tape session left the drive unusable" : result.downDetail};
  }
  if (stopToken.stop_requested() || !scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    return {IterationAction::EndOwnership, "Stop requested or desired drive state is Down"};
  }
  // All session workers and synchronous phase publications have finished before making the drive available.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);
  // Retire the object-store agent for garbage collection only when continuing..
  // Renewal invalidates the scheduler reference; do not access it after this point.
  m_schedulerContext.renewObjectStoreAgent();
  // Don't add the delay when we are successful: there may be more work to do and we don't want
  // to sleep unnecessarily.
  return {result.successful ? IterationAction::Continue : IterationAction::RetryAfterDelay, {}};
}

TapeSessionResult DriveSession::runTapeSession(TapeMount& tapeMount) {
  const auto vid = tapeMount.getVid();
  log::ScopedParamContainer mountParams(m_lc);
  mountParams.add("tapeVid", vid);
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
    // Store a shared ptr to the session tracker for the liveness probe.
    std::atomic_store<const TapeSessionTracker>(&m_activeTracker, session.sharedTracker());
    // Ensure we clean this shared ptr reference again when the session finishes
    const utils::ScopeExit clearActiveTracker(
      [this] { std::atomic_store<const TapeSessionTracker>(&m_activeTracker, nullptr); });
    // Run the session
    transferResult = session.execute();
  } catch (const exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::ERR, "Tape session failed. Attempting drive cleaning.");
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.what());
    m_lc.log(log::ERR, "Tape session failed. Attempting drive cleaning.");
  } catch (...) {
    m_lc.log(log::ERR, "Tape session failed with an unknown exception. Attempting drive cleaning.");
  }

  if (!transferResult) {
    // Failed before we even got a transfer result.
    // Cleanup may not be necessary, but it's the only way we can establish whether the drive is safe.
    const bool reusable = prepareDrive(vid);
    return {.driveReusable = reusable,
            .successful = false,
            .downDetail = "Drive cleaning did not permit further scheduling"};
  }

  if (!transferResult->driveReusable) {
    // Can't use the drive (e.g. cleaning failed due to stuck cartridge).
    requestDown(transferResult->downReason.value_or(common::dataStructures::DriveDownReason::SessionLeftDriveUnusable),
                transferResult->downDetail);
  }
  return *transferResult;
}

bool DriveSession::prepareDrive(const std::optional<std::string>& vid) {
  auto& scheduler = m_schedulerContext.scheduler();
  // Track preparation for liveness; phase publication runs synchronously on this thread.
  auto tracker = std::make_shared<TapeSessionTracker>([this, &scheduler](session::TapeSessionState state) {
    const auto status = TapeSessionReporter::driveStatusForSessionState(state);
    // Starting is published explicitly below, where failure must prevent hardware acquisition.
    if (status && *status != common::dataStructures::DriveStatus::Starting) {
      scheduler.reportDriveStatus(m_driveInfo, common::dataStructures::MountType::NoMount, *status, m_lc);
    }
  });
  tracker->reportState(cta::tape::session::TapeSessionState::Preparing);
  std::atomic_store<const TapeSessionTracker>(&m_activeTracker, tracker);
  const utils::ScopeExit clearActiveTracker(
    [this] { std::atomic_store<const TapeSessionTracker>(&m_activeTracker, nullptr); });

  const bool recovering = m_driveReservation.canUseHardware();
  log::ScopedParamContainer cleanupParams(m_lc);
  cleanupParams.add("cleanupPhase", recovering ? "recovery" : "preparation");
  // Only exit early if we haven't touched the hardware yet.
  // If we did; we need to continue with probing/cleaning first.
  if (!recovering && !scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    return false;
  }
  // Publish preparation before acquiring hardware access.
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Starting,
                              m_lc);
  m_driveReservation.acquire();
  const bool clean = recovering || m_config.drive.clean_on_up;
  m_lc.log(log::INFO, clean ? "Cleaning drive for preparation or recovery." : "Probing drive before going Up.");
  bool prepared = false;
  std::string preparationError;
  auto downReason = clean ? common::dataStructures::DriveDownReason::DriveCleanupFailed :
                            common::dataStructures::DriveDownReason::DriveProbeFailed;
  try {
    if (clean) {
      tracker->setType(session::SessionType::Cleanup);
      DriveCleaner cleaner(m_mediaChanger,
                           m_lc.logger(),
                           m_driveInfo,
                           vid.value_or(""),
                           true,
                           m_config.mounts.tape_load_timeout_secs,
                           scheduler.getCatalogue(),
                           *tracker);
      const auto result = cleaner.cleanDrive(m_sysWrapper);
      prepared = result.driveReusable();
      preparationError = result.errorMessage;
    } else {
      const auto result = probeEmptyDrive(m_driveInfo, m_sysWrapper);
      prepared = result.status == EmptyDriveProbeResult::Status::Empty;
      preparationError = result.errorMessage;
      if (result.status == EmptyDriveProbeResult::Status::CartridgePresent) {
        downReason = common::dataStructures::DriveDownReason::TapeDetected;
      }
    }
  } catch (const cta::exception::Exception& ex) {
    preparationError = ex.getMessageValue();
  } catch (const std::exception& ex) {
    preparationError = ex.what();
  } catch (...) {
    preparationError = "Unknown exception during drive preparation";
  }
  // Hardware preparation has ended; only outcome reporting remains.
  tracker->reportState(cta::tape::session::TapeSessionState::Finalizing);

  if (!prepared) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, preparationError);
    m_lc.log(log::ERR, "Drive preparation or cleanup failed; ending drive session.");
    requestDown(downReason, preparationError);
    return false;
  }

  // Preparation takes time; an operator may have withdrawn the up request while it ran.
  // Never publish desired-up here; a withdrawn request ends this ownership period.
  if (!scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc).up) {
    // The caller releases ownership and reports Down.
    m_lc.log(log::INFO, "Desired drive state became Down during preparation; ending drive session.");
    return false;
  }
  scheduler.reportDriveStatus(m_driveInfo,
                              common::dataStructures::MountType::NoMount,
                              common::dataStructures::DriveStatus::Up,
                              m_lc);
  return true;
}

}  // namespace cta::tape::daemon
