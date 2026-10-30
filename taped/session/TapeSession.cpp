/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "taped/session/TapeSession.hpp"

#include "common/dataStructures/ArchiveDismountPolicy.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/ExceptionLogging.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/process/ProcessCap.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/ScopeExit.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/rao/RAOParams.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/session/ScopedTapeSessionReporter.hpp"
#include "taped/session/TapeSessionReporter.hpp"
#include "taped/session/VolumeInfo.hpp"
#include "taped/session/WorkerFailureHandling.hpp"
#include "taped/session/archive/DiskReadThreadPool.hpp"
#include "taped/session/archive/MigrationTaskInjector.hpp"
#include "taped/session/archive/TapeWriteSingleThread.hpp"
#include "taped/session/retrieve/DiskWriteThreadPool.hpp"
#include "taped/session/retrieve/RecallReportPacker.hpp"
#include "taped/session/retrieve/RecallTaskInjector.hpp"
#include "taped/session/retrieve/TapeReadSingleThread.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace cta::tape::daemon {

namespace {
/// Resolve the immutable session type before publishing its first activity.
session::SessionType sessionTypeForMount(common::dataStructures::MountType mountType) {
  using session::SessionType;
  using enum common::dataStructures::MountType;
  switch (mountType) {
    case Retrieve:
      return SessionType::Retrieve;
    case ArchiveForUser:
    case ArchiveForRepack:
      return SessionType::Archive;
    case Label:
      return SessionType::Label;
    default:
      throw std::logic_error("Unsupported tape mount type");
  }
}

constexpr bool c_useLbp = true;
constexpr uint16_t c_xrootTimeout = 0;
constexpr const char* c_raoLtoAlgorithmOptions = "cost_heuristic_name:cta";

/// @brief Log the failure and retain the first exception outside CTA Exception and std::runtime_error for rethrow.
///
/// Contain logging failures so finalization can continue.
void recordFailure(std::exception_ptr failure, std::exception_ptr& fatalFailure, log::LogContext& lc) {
  const char* message = "Unrecoverable tape session failure";
  try {
    std::rethrow_exception(failure);
  } catch (const exception::Exception& ex) {
    message = ex.what();
  } catch (const std::runtime_error& ex) {
    message = ex.what();
  } catch (...) {
    if (!fatalFailure) {
      fatalFailure = failure;
    }
  }
  // Logging must not interrupt the remaining cleanup or replace the original exception.
  try {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, message);
    lc.log(log::ERR, "Tape session operation failed");
  } catch (...) {}
}
}  // namespace

// Local to one execute() call; transfer reporting takes over completion after startup.
struct TapeSession::ExecutionState {
  TapeSessionResult result;
  // The pipeline completes the mount once started.
  // The tape thread handles unmounting; drive reuse also waits for disk delivery and reporting.
  bool completionDelegated = false;
};

TapeSession::TapeSession(log::Logger& log,
                         System::virtualWrapper& sysWrapper,
                         const common::dataStructures::DriveInfo& driveInfo,
                         mediachanger::MediaChangerFacade& mc,
                         TapeMount& tapeMount,
                         const TransfersConfig& transfersConfig,
                         uint32_t tapeLoadTimeoutSecs,
                         catalogue::Catalogue& catalogue)
    : m_tapeSessionTracker(std::make_shared<TapeSessionTracker>(
        [&tapeMount](session::TapeSessionState state) { tapeMount.reportTapeSessionState(state); },
        &log)),
      m_log(log),
      m_tapeMount(tapeMount),
      m_sysWrapper(sysWrapper),
      m_transfersConfig(transfersConfig),
      m_tapeLoadTimeoutSecs(tapeLoadTimeoutSecs),
      m_driveInfo(driveInfo),
      m_mediaChanger(mc),
      m_catalogue(catalogue) {}

TapeSessionResult TapeSession::execute() {
  telemetry::metrics::setMountType(common::dataStructures::MountType::NoMount);
  // Clear the active mount after session finalization and worker shutdown.
  const utils::ScopeExit mountScope(
    [] { telemetry::metrics::setMountType(common::dataStructures::MountType::NoMount); });
  log::LogContext lc(m_log);
  log::ScopedParamContainer params(lc);
  ExecutionState state;
  std::exception_ptr fatalFailure;
  TapeSessionReporter reporter(*m_tapeSessionTracker,
                               m_tapeMount,
                               lc,
                               std::chrono::seconds(m_transfersConfig.stats_report_interval_secs),
                               std::chrono::seconds(m_transfersConfig.no_block_move_timeout_secs));
  // Ensure reporting stops on scope exit (including error paths)
  ScopedTapeSessionReporter scopedReporter(reporter, lc);

  try {
    m_volInfo.mountType = m_tapeMount.getMountType();
    m_volInfo.vid = m_tapeMount.getVid();
    m_volInfo.nbFiles = m_tapeMount.getNbFiles();
    m_volInfo.mountId = m_tapeMount.getMountTransactionId();
    m_volInfo.labelFormat = m_tapeMount.getLabelFormat();
    m_volInfo.encryptionKeyName = m_tapeMount.getEncryptionKeyName();
    m_volInfo.tapePool = m_tapeMount.getPoolName();

    // Ensure we publish correctly what kind of mount type this session is for
    m_tapeSessionTracker->beginTapeSession(sessionTypeForMount(m_volInfo.mountType));
    telemetry::metrics::setMountType(m_volInfo.mountType);

    params.add("tapeVid", m_volInfo.vid)
      .add("mountId", m_volInfo.mountId)
      .add("vo", m_tapeMount.getVo())
      .add("tapePool", m_volInfo.tapePool);
    lc.log(log::INFO, "Got volume from client");

    // Keep periodic reporting active while the selected pipeline runs.
    switch (m_volInfo.mountType) {
      case common::dataStructures::MountType::Retrieve:
        scopedReporter.start();
        executeRead(lc, dynamic_cast<RetrieveMount&>(m_tapeMount), state);
        break;
      case common::dataStructures::MountType::ArchiveForUser:
      case common::dataStructures::MountType::ArchiveForRepack:
        scopedReporter.start();
        executeWrite(lc, dynamic_cast<ArchiveMount&>(m_tapeMount), state);
        break;
      case common::dataStructures::MountType::Label:
        throw exception::NotImplementedException();
      default:
        throw std::logic_error("Unsupported tape mount type");
    }
  } catch (...) {
    // Preserve a more specific pipeline failure when one has already been recorded.
    m_tapeSessionTracker->recordFailureIfNone(TapeSessionFailure::UnexpectedSession);
    recordFailure(std::current_exception(), fatalFailure, lc);
  }

  m_tapeSessionTracker->reportState(session::TapeSessionState::Finalizing);

  // It is the responsibility of the transfer pipeline to complete the mount.
  // However, in case of failures, that hand-off may not have completed,
  // so this ensures we still do the completion in those cases
  if (!state.completionDelegated) {
    try {
      m_tapeMount.complete();
    } catch (...) {
      // Failed to complete the mount
      m_tapeSessionTracker->recordFailure(TapeSessionFailure::UnexpectedSession);
      recordFailure(std::current_exception(), fatalFailure, lc);
    }
  }

  // Stop periodic reporting before publishing the final statistics.
  scopedReporter.finish();
  // Send the final session report even if startup failed before periodic reporting began.
  try {
    reporter.reportSessionFinished();
  } catch (...) {
    m_tapeSessionTracker->recordFailure(TapeSessionFailure::Reporting);
    log::logCurrentExceptionNoThrow(lc, "Failed to send final tape session statistics", log::WARNING);
  }
  // Only after final reporting are we done
  m_tapeSessionTracker->reportState(session::TapeSessionState::Finished);
  // Final reporting can turn an otherwise successful session into a failure.
  state.result.successful = !m_tapeSessionTracker->hasFailures();
  if (fatalFailure) {
    std::rethrow_exception(fatalFailure);
  }
  return state.result;
}

void TapeSession::executeRead(log::LogContext& logContext, RetrieveMount& retrieveMount, ExecutionState& state) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.

  // For retrieve sessions, we need a way to understand how full the disk buffer is for backpressure.
  // Right now, this happens through an external bash scripts that operators can modify accordingly.
  // Ideally in the future we have a less hacky solution to this...
  retrieveMount.setExternalFreeDiskSpaceScript(m_transfersConfig.retrieve.external_free_disk_space_script);

  // Drive discovery
  auto drive = findDrive(logContext, state);

  // We can now start instantiating all the components of the data path
  // Allocate all the elements of the memory management (in proper order
  // to refer them to each other)

  RecallReportPacker reportPacker(&retrieveMount, logContext, *m_tapeSessionTracker);
  reportPacker.disableBulk();  //no bulk needed anymore
  RecallMemoryManager memoryManager(m_transfersConfig.buffer_count, m_transfersConfig.buffer_size_bytes, logContext);
  // TODO: rename these variables to something a bit more accurate
  TapeReadSingleThread readSingleThread(*drive,
                                        m_mediaChanger,
                                        *m_tapeSessionTracker,
                                        m_volInfo,
                                        m_transfersConfig.retrieve.fetch_max_files,
                                        logContext,
                                        reportPacker,
                                        c_useLbp,
                                        m_transfersConfig.retrieve.rao.enabled,
                                        m_transfersConfig.encryption.enabled,
                                        m_transfersConfig.encryption.external_key_script,
                                        retrieveMount,
                                        m_tapeLoadTimeoutSecs,
                                        m_catalogue);

  DiskWriteThreadPool threadPool(m_transfersConfig.disk_io_threads,
                                 reportPacker,
                                 *m_tapeSessionTracker,
                                 logContext,
                                 c_xrootTimeout);
  RecallTaskInjector taskInjector(memoryManager,
                                  readSingleThread,
                                  threadPool,
                                  retrieveMount,
                                  m_transfersConfig.retrieve.fetch_max_files,
                                  m_transfersConfig.retrieve.fetch_max_bytes,
                                  *m_tapeSessionTracker,
                                  logContext);

  // TODO: do we really need it like this?
  taskInjector.setDriveInterface(readSingleThread.getDriveReference());

  // We are now ready to put everything in motion. First step is to check
  // we get any concrete job to be done from the client (via the task injector)
  utils::Timer timer;

  // The RecallTaskInjector and the TapeReadSingleThread share the promise
  if (m_transfersConfig.retrieve.rao.enabled) {
    configureRAO(logContext, taskInjector);
  }
  bool noFilesToRecall = false;
  bool fetchResult = false;
  bool reservationResult = false;
  // Do an initial fetch to see if there is something to retrieve in the first place (might be an empty mount)
  try {
    fetchResult = taskInjector.synchronousFetch(noFilesToRecall);
  } catch (...) {
    m_tapeSessionTracker->recordFailure(TapeSessionFailure::TaskInjection);
    throw;
  }
  if (fetchResult) {
    // Check disk space reservation before committing to a tape mount.
    reservationResult = taskInjector.testDiskSpaceReservationWorking();
  }
  // Only mount the tape if we can confirm that we will do some work, otherwise do an empty mount
  if (fetchResult && reservationResult) {
    // We got something to retrieve. Time to start the machinery
    readSingleThread.setWaitForInstructionsTime(timer.secs());

    // We still haven't obtained a physical mount at this point: that will happen when we start the tape thread.
    // However, at this point we are sure we are at least attempting to physically mount a tape.
    m_tapeSessionTracker->setMountAttempted(true);
    // Do not unwind pipeline resources unless every worker has joined successfully.
    runOrExitOnWorkerFailure(logContext, [&] {
      readSingleThread.startThreadsWithInjector(taskInjector);
      threadPool.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      state.completionDelegated = true;
      // This thread is now going to be idle until the system unwinds at the end of the session
      // All client notifications are done by the report packer, including the end of session
      taskInjector.waitThreads();
      threadPool.waitThreads();
      readSingleThread.waitThreads();
      reportPacker.waitThread();
    });
    // Use the tape thread's cleanup result to decide whether the drive can be reused.
    state.result.driveReusable = readSingleThread.isDriveReusable();
    if (!state.result.driveReusable) {
      state.result.downReason = common::dataStructures::DriveDownReason::DriveCleanupFailed;
      state.result.downDetail = readSingleThread.cleanupError();
    }
    return;
  } else {
    if (!noFilesToRecall && !fetchResult) {
      m_tapeSessionTracker->recordFailureIfNone(TapeSessionFailure::TaskInjection);
    }
    // Nothing transferred
    m_tapeSessionTracker->updateTapeTransferStats({});
    if (noFilesToRecall) {
      m_tapeSessionTracker->recordEvent(TapeSessionEvent::NoFilesToRecall);
    }
    // Couldn't reserve free disk space
    if (fetchResult && !reservationResult) {
      m_tapeSessionTracker->recordEvent(TapeSessionEvent::DiskSpaceReservationTestFailure);
    }
    m_tapeSessionTracker->recordEvent(TapeSessionEvent::EmptyMount);
    logContext.log(log::WARNING, "Aborting recall mount startup: empty mount");
  }
}

void TapeSession::executeWrite(log::LogContext& logContext, ArchiveMount& archiveMount, ExecutionState& state) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.

  // Drive discovery
  auto drive = findDrive(logContext, state);

  // We can now start instantiating all the components of the data path
  // Allocate all the elements of the memory management (in proper order
  // to refer them to each other)
  MigrationMemoryManager memoryManager(m_transfersConfig.buffer_count, m_transfersConfig.buffer_size_bytes, logContext);
  MigrationReportPacker reportPacker(&archiveMount, logContext, *m_tapeSessionTracker);
  // TODO: rename these variables to something a bit more accurate
  TapeWriteSingleThread writeSingleThread(*drive,
                                          m_mediaChanger,
                                          *m_tapeSessionTracker,
                                          m_volInfo,
                                          logContext,
                                          reportPacker,
                                          m_transfersConfig.archive.flush_max_files,
                                          m_transfersConfig.archive.flush_max_bytes,
                                          c_useLbp,
                                          m_transfersConfig.encryption.enabled,
                                          m_transfersConfig.encryption.external_key_script,
                                          archiveMount,
                                          m_tapeLoadTimeoutSecs,
                                          m_catalogue);

  DiskReadThreadPool threadPool(m_transfersConfig.disk_io_threads,
                                m_transfersConfig.archive.fetch_max_files,
                                m_transfersConfig.archive.fetch_max_bytes,
                                *m_tapeSessionTracker,
                                logContext,
                                c_xrootTimeout);

  // Configure when the injector should stop waiting for more archive work.
  const auto& underfill = m_transfersConfig.archive.underfill;
  const common::dataStructures::ArchiveDismountPolicy archiveDismountPolicy(underfill.watch_period_secs,
                                                                            underfill.minimum_samples,
                                                                            underfill.start_threshold_percent,
                                                                            underfill.recovery_threshold_percent);
  MigrationTaskInjector taskInjector(memoryManager,
                                     threadPool,
                                     writeSingleThread,
                                     archiveMount,
                                     m_transfersConfig.archive.fetch_max_files,
                                     m_transfersConfig.archive.fetch_max_bytes,
                                     archiveDismountPolicy,
                                     *m_tapeSessionTracker,
                                     logContext);
  writeSingleThread.setTaskInjector(&taskInjector);
  // We are now ready to put everything in motion. First step is to check
  // we get any concrete job to be done from the client (via the task injector)
  utils::Timer timer;
  bool noFilesToMigrate = false;
  bool injected = false;
  // Do an initial fetch to see if there is something to archive in the first place (might be an empty mount)
  try {
    injected = taskInjector.synchronousInjection(noFilesToMigrate);
  } catch (...) {
    m_tapeSessionTracker->recordFailure(TapeSessionFailure::TaskInjection);
    throw;
  }
  // Only mount the tape if we can confirm that we will do some work, otherwise do an empty mount
  if (injected) {
    logContext.log(log::DEBUG, "After if (taskInjector.synchronousInjection())");
    const uint64_t firstFseqFromClient = taskInjector.firstFseqToWrite();

    // The last fseq written on the tape is the first file's fseq minus one
    writeSingleThread.setlastFseq(firstFseqFromClient - 1);

    // We got something to archive. Time to start the machinery

    // We still haven't obtained a physical mount at this point: that will happen when we start the tape thread.
    // However, at this point we are sure we are at least attempting to physically mount a tape.
    m_tapeSessionTracker->setMountAttempted(true);
    // Do not unwind pipeline resources unless every worker has joined successfully.
    runOrExitOnWorkerFailure(logContext, [&] {
      memoryManager.startThreads();
      threadPool.startThreads(taskInjector);
      writeSingleThread.setWaitForInstructionsTime(timer.secs());
      writeSingleThread.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      state.completionDelegated = true;
      // This thread is now going to be idle until the system unwinds at the end of the session
      // All client notifications are done by the report packer, including the end of session
      taskInjector.waitThreads();
      writeSingleThread.waitThreads();
      threadPool.waitThreads();
      memoryManager.waitThreads();
      reportPacker.waitThread();
    });

    // Use the tape thread's cleanup result to decide whether the drive can be reused.
    state.result.driveReusable = writeSingleThread.isDriveReusable();
    if (!state.result.driveReusable) {
      state.result.downReason = common::dataStructures::DriveDownReason::DriveCleanupFailed;
      state.result.downDetail = writeSingleThread.cleanupError();
    }
    return;
  } else {
    if (!noFilesToMigrate) {
      m_tapeSessionTracker->recordFailureIfNone(TapeSessionFailure::TaskInjection);
    }
    // Nothing transferred
    m_tapeSessionTracker->updateTapeTransferStats({});
    m_tapeSessionTracker->recordEvent(TapeSessionEvent::NoFilesToMigrate);
    m_tapeSessionTracker->recordEvent(TapeSessionEvent::EmptyMount);
    logContext.log(log::WARNING, "Aborting migration mount startup: empty mount");
  }
}

std::unique_ptr<drive::DriveInterface> TapeSession::findDrive(log::LogContext& logContext, ExecutionState& state) {
  constexpr auto reason = common::dataStructures::DriveDownReason::SessionDriveAccessFailed;
  std::string_view stage = "Drive discovery failed";
  try {
    SCSI::DeviceVector devices(m_sysWrapper);
    stage = "Configured drive lookup failed";
    const auto driveInfo = devices.findBySymlink(m_driveInfo.devFilename);
    stage = "Drive opening failed";
    auto drive = drive::createDrive(driveInfo, m_sysWrapper);
    if (!drive) {
      throw exception::Exception("Drive creation returned no drive");
    }
    drive->info = m_driveInfo;
    return drive;
  } catch (...) {
    // Record the operation stage and cause before propagating the original exception.
    std::string detail(stage);
    detail += ": ";
    detail += exception::currentExceptionMessage();
    state.result.downReason = reason;
    state.result.downDetail = detail;
    state.result.driveReusable = false;
    logContext.log(common::dataStructures::driveDownReasonSeverity(reason),
                   common::dataStructures::formatDriveDownReason(reason, detail));
    throw;
  }
}

void TapeSession::configureRAO(log::LogContext& logContext, RecallTaskInjector& taskInjector) {
  using LabelFormat = common::dataStructures::Label::Format;
  if (m_volInfo.labelFormat == LabelFormat::Enstore || m_volInfo.labelFormat == LabelFormat::EnstoreLarge) {
    // No RAO for Enstore
    LabelFormat format = static_cast<LabelFormat>(m_volInfo.labelFormat);
    std::ostringstream format_str;
    format_str << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
               << static_cast<unsigned int>(format);
    log::ScopedParamContainer params(logContext);
    params.add("tapeVid", m_volInfo.vid).add("mountId", m_volInfo.mountId).add("labelFormat", format_str.str());
    logContext.log(log::INFO, "TapeSession::executeRead Tape LabelFormat incompatible with RAO. Setting RAO false.");
  } else {
    rao::RAOParams raoDataConfig(m_transfersConfig.retrieve.rao.enabled,
                                 m_transfersConfig.retrieve.rao.lto_algorithm,
                                 c_raoLtoAlgorithmOptions,
                                 m_volInfo.vid);
    taskInjector.initRAO(raoDataConfig, &m_catalogue);
  }
}

}  // namespace cta::tape::daemon
