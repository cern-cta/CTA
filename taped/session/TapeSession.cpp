/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSession.hpp"

#include "DiskReadThreadPool.hpp"
#include "DiskWriteThreadPool.hpp"
#include "EmptyDriveProbe.hpp"
#include "MigrationTaskInjector.hpp"
#include "RecallReportPacker.hpp"
#include "RecallTaskInjector.hpp"
#include "TapeReadSingleThread.hpp"
#include "TapeSessionReporter.hpp"
#include "TapeWriteSingleThread.hpp"
#include "common/dataStructures/ArchiveDismountPolicy.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/process/ProcessCap.hpp"
#include "common/semconv/Attributes.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/rao/RAOParams.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/session/VolumeInfo.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {
constexpr bool c_useLbp = true;
constexpr uint16_t c_xrootTimeout = 0;
constexpr const char* c_raoLtoAlgorithmOptions = "cost_heuristic_name:cta";

// Keep the active mount visible until all session finalization and worker shutdown have finished.
class ScopedMountType {
public:
  ScopedMountType() { cta::telemetry::metrics::setMountType(cta::common::dataStructures::MountType::NoMount); }

  ~ScopedMountType() { cta::telemetry::metrics::setMountType(cta::common::dataStructures::MountType::NoMount); }

  ScopedMountType(const ScopedMountType&) = delete;
  ScopedMountType& operator=(const ScopedMountType&) = delete;
};

// The reporter has no pipeline dependencies: stopping it only wakes its own wait loop.
class ScopedReporter {
public:
  /**
   * @brief Borrow a reporter without starting its thread.
   *
   * @param reporter Borrowed reporter whose thread is managed by this guard.
   */
  explicit ScopedReporter(cta::tape::daemon::TapeSessionReporter& reporter) : m_reporter(reporter) {}

  /**
   * @brief Disallow copying the owner of a reporter thread.
   */
  ScopedReporter(const ScopedReporter&) = delete;

  /**
   * @brief Disallow assigning ownership of a reporter thread.
   */
  ScopedReporter& operator=(const ScopedReporter&) = delete;

  /**
   * @brief Stop and join a started reporter; terminate if joining fails to protect borrowed session state.
   */
  ~ScopedReporter() noexcept {
    if (m_started) {
      try {
        finish();
      } catch (...) {
        // Never destroy a reporter that may still reference the session's stack.
        std::terminate();
      }
    }
  }

  /**
   * @brief Start reporting and record that this guard must join the thread.
   */
  void start() {
    m_reporter.startThreads();
    m_started = true;
  }

  /**
   * @brief Request shutdown and join the reporter if it was started.
   */
  void finish() {
    if (!m_started) {
      return;
    }
    m_reporter.finish();
    m_reporter.waitThreads();
    m_started = false;
  }

private:
  cta::tape::daemon::TapeSessionReporter& m_reporter;
  bool m_started = false;
};

/**
 * @brief Record recovery decisions while preserving the first fatal failure for later propagation.
 *
 * Logging failures are contained so remaining finalization operations can still run.
 *
 * @param result Session recovery decisions updated to reflect the failure.
 * @param failure Exception captured from the failed session operation.
 * @param fatalFailure First fatal exception retained for propagation after finalization.
 * @param lc Log context for diagnostics.
 */
void recordFailure(cta::tape::daemon::TapeSessionResult& result,
                   std::exception_ptr failure,
                   std::exception_ptr& fatalFailure,
                   cta::log::LogContext& lc) {
  result.retryDelayRequired = true;
  const char* message = "Unrecoverable tape session failure";
  try {
    std::rethrow_exception(failure);
  } catch (const cta::exception::Exception& ex) {
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
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::exceptionMessage, message);
    lc.log(cta::log::ERR, "Tape session operation failed");
  } catch (...) {}
}
}  // namespace

// Local to one execute() call; transfer reporting takes over completion after startup.
struct cta::tape::daemon::TapeSession::ExecutionState {
  TapeSessionResult result;
  bool completionOwned = true;
  bool workersRunning = false;
  bool driveOpened = false;
  std::optional<std::string> downReason;
};

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
cta::tape::daemon::TapeSession::TapeSession(cta::log::Logger& log,
                                            System::virtualWrapper& sysWrapper,
                                            const cta::common::dataStructures::DriveInfo& driveInfo,
                                            cta::mediachanger::MediaChangerFacade& mc,
                                            cta::TapeMount& tapeMount,
                                            const TransfersConfig& transfersConfig,
                                            uint32_t tapeLoadTimeoutSecs,
                                            cta::Scheduler& scheduler)
    : m_log(log),
      m_tapeMount(tapeMount),
      m_sysWrapper(sysWrapper),
      m_transfersConfig(transfersConfig),
      m_tapeLoadTimeoutSecs(tapeLoadTimeoutSecs),
      m_driveInfo(driveInfo),
      m_mediaChanger(mc),
      m_scheduler(scheduler) {
  m_tapeSessionTracker.setMount(&m_tapeMount);
}

//------------------------------------------------------------------------------
//TapeSession::execute
//------------------------------------------------------------------------------
cta::tape::daemon::TapeSessionResult cta::tape::daemon::TapeSession::execute() {
  const ScopedMountType mountScope;
  m_tapeSessionTracker.beginTapeSession();
  m_tapeSessionTracker.setMountAttempted(false);
  cta::log::LogContext lc(m_log);
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeDrive", m_driveInfo.driveName);
  ExecutionState state;
  std::exception_ptr fatalFailure;
  TapeSessionReporter reporter(m_tapeSessionTracker,
                               lc,
                               std::chrono::seconds(m_transfersConfig.stats_report_interval_secs),
                               std::chrono::seconds(m_transfersConfig.no_block_move_timeout_secs));
  ScopedReporter reporterScope(reporter);

  try {
    m_volInfo.vid = m_tapeMount.getVid();
    m_volInfo.mountType = m_tapeMount.getMountType();
    cta::telemetry::metrics::setMountType(m_volInfo.mountType);
    m_volInfo.nbFiles = m_tapeMount.getNbFiles();
    m_volInfo.mountId = m_tapeMount.getMountTransactionId();
    m_volInfo.labelFormat = m_tapeMount.getLabelFormat();
    m_volInfo.encryptionKeyName = m_tapeMount.getEncryptionKeyName();
    m_volInfo.tapePool = m_tapeMount.getPoolName();
    m_tapeMount.setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    params.add("tapeVid", m_volInfo.vid)
      .add("mountId", m_volInfo.mountId)
      .add("vo", m_tapeMount.getVo())
      .add("tapePool", m_volInfo.tapePool);
    lc.log(cta::log::INFO, "Got volume from client");

    switch (m_volInfo.mountType) {
      case cta::common::dataStructures::MountType::Retrieve:
        m_tapeSessionTracker.setType(cta::tape::session::SessionType::Retrieve);
        reporterScope.start();
        executeRead(lc, dynamic_cast<cta::RetrieveMount&>(m_tapeMount), state);
        break;
      case cta::common::dataStructures::MountType::ArchiveForUser:
      case cta::common::dataStructures::MountType::ArchiveForRepack:
        m_tapeSessionTracker.setType(cta::tape::session::SessionType::Archive);
        reporterScope.start();
        executeWrite(lc, dynamic_cast<cta::ArchiveMount&>(m_tapeMount), state);
        break;
      case cta::common::dataStructures::MountType::Label:
        m_tapeSessionTracker.setType(cta::tape::session::SessionType::Label);
        throw cta::exception::NotImplementedException();
        break;
      default:
        throw std::logic_error("Unsupported tape mount type");
    }
  } catch (...) {
    m_tapeSessionTracker.setOutcome(TapeSessionOutcome::Failure);
    // Partial startup and unexpected worker termination still require a separate lifecycle repair.
    // The reporter guard is not a worker shutdown mechanism; do not claim a finished session here.
    if (state.workersRunning) {
      throw;
    }
    recordFailure(state.result, std::current_exception(), fatalFailure, lc);
  }

  // One failed publication must not skip mount completion or another state update.
  const auto finalize = [&](auto&& operation) {
    try {
      operation();
    } catch (...) {
      m_tapeSessionTracker.incrementError(TapeSessionError::Reporting);
      recordFailure(state.result, std::current_exception(), fatalFailure, lc);
    }
  };
  m_tapeSessionTracker.reportState(cta::tape::session::TapeSessionState::Finalizing);
  if (state.completionOwned) {
    state.completionOwned = false;
    finalize([&] { m_tapeMount.complete(); });
    if (state.driveOpened) {
      finalize([&] {
        m_scheduler.reportDriveStatus(m_driveInfo,
                                      cta::common::dataStructures::MountType::NoMount,
                                      cta::common::dataStructures::DriveStatus::Up,
                                      lc);
      });
    }
  }
  if (state.downReason) {
    finalize([&] {
      m_scheduler.reportDriveStatus(m_driveInfo,
                                    cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Down,
                                    lc);
    });
    finalize([&] {
      cta::common::dataStructures::DesiredDriveState desired;
      desired.up = false;
      desired.forceDown = false;
      desired.reason = *state.downReason;
      m_scheduler.setDesiredDriveState(m_driveInfo.driveName, desired, lc);
    });
  }

  m_tapeSessionTracker.reportState(cta::tape::session::TapeSessionState::Finished);
  reporterScope.finish();
  state.result.retryDelayRequired |= m_tapeSessionTracker.outcome() == TapeSessionOutcome::Failure;
  if (fatalFailure) {
    std::rethrow_exception(fatalFailure);
  }
  return state.result;
}

//------------------------------------------------------------------------------
//TapeSession::executeRead
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeSession::executeRead(cta::log::LogContext& logContext,
                                                 cta::RetrieveMount& retrieveMount,
                                                 ExecutionState& state) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  retrieveMount.setExternalFreeDiskSpaceScript(m_transfersConfig.retrieve.external_free_disk_space_script);
  auto drive = findDrive(logContext, state);

  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker reportPacker(&retrieveMount, logContext);
    reportPacker.disableBulk();  //no bulk needed anymore
    RecallMemoryManager memoryManager(m_transfersConfig.buffer_count, m_transfersConfig.buffer_size_bytes, logContext);

    TapeReadSingleThread readSingleThread(*drive,
                                          m_mediaChanger,
                                          m_tapeSessionTracker,
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
                                          m_scheduler.getCatalogue());

    DiskWriteThreadPool threadPool(m_transfersConfig.disk_io_threads,
                                   reportPacker,
                                   m_tapeSessionTracker,
                                   logContext,
                                   c_xrootTimeout);
    RecallTaskInjector taskInjector(memoryManager,
                                    readSingleThread,
                                    threadPool,
                                    retrieveMount,
                                    m_transfersConfig.retrieve.fetch_max_files,
                                    m_transfersConfig.retrieve.fetch_max_bytes,
                                    m_tapeSessionTracker,
                                    logContext);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type of the client
    readSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setTapeSessionTracker(m_tapeSessionTracker);

    taskInjector.setDriveInterface(readSingleThread.getDriveReference());

    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    cta::utils::Timer timer;

    // The RecallTaskInjector and the TapeReadSingleThread share the promise
    if (m_transfersConfig.retrieve.rao.enabled) {
      using LabelFormat = cta::common::dataStructures::Label::Format;
      if (m_volInfo.labelFormat == LabelFormat::Enstore || m_volInfo.labelFormat == LabelFormat::EnstoreLarge) {
        LabelFormat format = static_cast<LabelFormat>(m_volInfo.labelFormat);
        std::ostringstream format_str;
        format_str << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                   << static_cast<unsigned int>(format);
        cta::log::ScopedParamContainer params(logContext);
        params.add("tapeVid", m_volInfo.vid).add("mountId", m_volInfo.mountId).add("labelFormat", format_str.str());
        logContext.log(cta::log::INFO,
                       "TapeSession::executeRead Tape LabelFormat incompatible with RAO. Setting RAO false.");
      } else {
        cta::tape::rao::RAOParams raoDataConfig(m_transfersConfig.retrieve.rao.enabled,
                                                m_transfersConfig.retrieve.rao.lto_algorithm,
                                                c_raoLtoAlgorithmOptions,
                                                m_volInfo.vid);
        taskInjector.initRAO(raoDataConfig, &m_scheduler.getCatalogue());
      }
    }
    bool noFilesToRecall = false;
    bool fetchResult = false;
    bool reservationResult = false;
    fetchResult = taskInjector.synchronousFetch(noFilesToRecall);
    if (fetchResult) {
      reservationResult = taskInjector.testDiskSpaceReservationWorking();
    }
    //only mount the tape if we can confirm that we will do some work, otherwise do an empty mount
    if (fetchResult && reservationResult) {
      // We got something to recall. Time to start the machinery
      readSingleThread.setWaitForInstructionsTime(timer.secs());
      m_tapeSessionTracker.setMountAttempted(true);
      state.workersRunning = true;
      readSingleThread.startThreads();
      threadPool.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      state.completionOwned = false;
      // TODO: join every started worker if a later start or wait throws, before finalizing the mount.
      // This thread is now going to be idle until the system unwinds at the end of the session
      // All client notifications are done by the report packer, including the end of session
      taskInjector.waitThreads();
      threadPool.waitThreads();
      readSingleThread.waitThreads();
      reportPacker.waitThread();
      state.workersRunning = false;
      state.result.driveUsability = readSingleThread.getHardwareStatus();
      // If disk delivery finished last, return the drive from DrainingToDisk to Up.
      if (state.result.driveUsability == DriveUsability::Reusable
          && m_scheduler.getDriveStatus(m_driveInfo.driveName, &logContext)
               == cta::common::dataStructures::DriveStatus::DrainingToDisk) {
        m_scheduler.reportDriveStatus(m_driveInfo,
                                      cta::common::dataStructures::MountType::NoMount,
                                      cta::common::dataStructures::DriveStatus::Up,
                                      logContext);
      }
      return;
    } else {
      m_tapeSessionTracker.setOutcome(noFilesToRecall ? TapeSessionOutcome::Success : TapeSessionOutcome::Failure);
      m_tapeSessionTracker.setMountAttempted(false);
      m_tapeSessionTracker.updateTapeTransferStats({});
      if (fetchResult && !reservationResult) {
        m_tapeSessionTracker.incrementError(TapeSessionError::DiskSpaceReservationTestFailure);
      }
      m_tapeSessionTracker.incrementError(TapeSessionError::EmptyMount);
      logContext.log(cta::log::WARNING, "Aborting recall mount startup: empty mount");
    }
  }
}

//------------------------------------------------------------------------------
//TapeSession::executeWrite
//------------------------------------------------------------------------------
void cta::tape::daemon::TapeSession::executeWrite(cta::log::LogContext& logContext,
                                                  cta::ArchiveMount& archiveMount,
                                                  ExecutionState& state) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  auto drive = findDrive(logContext, state);
  // Once we got hold of the drive, we can run the session
  {
    MigrationMemoryManager memoryManager(m_transfersConfig.buffer_count,
                                         m_transfersConfig.buffer_size_bytes,
                                         logContext);
    MigrationReportPacker reportPacker(&archiveMount, logContext);
    TapeWriteSingleThread writeSingleThread(*drive,
                                            m_mediaChanger,
                                            m_tapeSessionTracker,
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
                                            m_scheduler.getCatalogue());

    DiskReadThreadPool threadPool(m_transfersConfig.disk_io_threads,
                                  m_transfersConfig.archive.fetch_max_files,
                                  m_transfersConfig.archive.fetch_max_bytes,
                                  m_tapeSessionTracker,
                                  logContext,
                                  c_xrootTimeout);

    const auto& underfill = m_transfersConfig.archive.underfill;
    const cta::common::dataStructures::ArchiveDismountPolicy archiveDismountPolicy(
      underfill.watch_period_secs,
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
                                       logContext);
    threadPool.setTaskInjector(&taskInjector);
    writeSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setTapeSessionTracker(m_tapeSessionTracker);
    cta::utils::Timer timer;
    bool noFilesToMigrate = false;
    if (taskInjector.synchronousInjection(noFilesToMigrate)) {
      logContext.log(cta::log::DEBUG, "After if (taskInjector.synchronousInjection())");
      const uint64_t firstFseqFromClient = taskInjector.firstFseqToWrite();

      // The last fseq written on the tape is the first file's fseq minus one
      writeSingleThread.setlastFseq(firstFseqFromClient - 1);

      // We have something to do: start the session by starting all the threads.
      m_tapeSessionTracker.setMountAttempted(true);
      state.workersRunning = true;
      memoryManager.startThreads();
      threadPool.startThreads();
      writeSingleThread.setWaitForInstructionsTime(timer.secs());
      writeSingleThread.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      state.completionOwned = false;
      // TODO: join every started worker if a later start or wait throws, before finalizing the mount.
      // Synchronise with end of threads
      taskInjector.waitThreads();
      writeSingleThread.waitThreads();
      threadPool.waitThreads();
      memoryManager.waitThreads();
      reportPacker.waitThread();
      state.workersRunning = false;

      state.result.driveUsability = writeSingleThread.getHardwareStatus();
      return;
    } else {
      m_tapeSessionTracker.setOutcome(noFilesToMigrate ? TapeSessionOutcome::Success : TapeSessionOutcome::Failure);
      m_tapeSessionTracker.setMountAttempted(false);
      m_tapeSessionTracker.updateTapeTransferStats({});
      m_tapeSessionTracker.incrementError(TapeSessionError::NoFilesToMigrate);
      m_tapeSessionTracker.incrementError(TapeSessionError::EmptyMount);
      logContext.log(cta::log::WARNING, "Aborting migration mount startup: empty mount");
    }
  }
}

//------------------------------------------------------------------------------
//TapeSession::findDrive
//------------------------------------------------------------------------------
std::unique_ptr<cta::tape::drive::DriveInterface>
cta::tape::daemon::TapeSession::findDrive(cta::log::LogContext& logContext, ExecutionState& state) {
  constexpr auto reason = common::dataStructures::DriveDownReason::SessionDriveAccessFailed;
  std::string_view stage = "Drive discovery failed";
  try {
    cta::tape::SCSI::DeviceVector devices(m_sysWrapper);
    stage = "Configured drive lookup failed";
    const auto driveInfo = devices.findBySymlink(m_driveInfo.devFilename);
    stage = "Drive opening failed";
    auto drive = cta::tape::drive::createDrive(driveInfo, m_sysWrapper);
    if (!drive) {
      throw cta::exception::Exception("Drive creation returned no drive");
    }
    drive->info = m_driveInfo;
    state.driveOpened = true;
    return drive;
  } catch (...) {
    // Record the operation stage and cause before propagating the original exception.
    std::string detail(stage);
    detail += ": ";
    try {
      throw;
    } catch (const cta::exception::Exception& ex) {
      detail += ex.getMessageValue();
    } catch (const std::exception& ex) {
      detail += ex.what();
    } catch (...) {
      detail += "Unknown exception";
    }
    state.downReason = common::dataStructures::formatDriveDownReason(reason, detail);
    state.result.driveUsability = DriveUsability::MustRemainDown;
    logContext.log(common::dataStructures::driveDownReasonSeverity(reason), *state.downReason);
    throw;
  }
}
