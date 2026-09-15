/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DataTransferSession.hpp"

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
#include "common/exception/LostDatabaseConnection.hpp"
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

#include <chrono>
#include <memory>
#include <string>
#include <utility>

namespace {
constexpr bool c_useLbp = true;
constexpr uint16_t c_xrootTimeout = 0;
constexpr const char* c_raoLtoAlgorithmOptions = "cost_heuristic_name:cta";
}  // namespace

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
cta::tape::daemon::DataTransferSession::DataTransferSession(cta::log::Logger& log,
                                                            System::virtualWrapper& sysWrapper,
                                                            const cta::common::dataStructures::DriveInfo& driveInfo,
                                                            cta::mediachanger::MediaChangerFacade& mc,
                                                            cta::TapeMount& tapeMount,
                                                            cta::tape::daemon::TapeSessionTracker& tapeSessionTracker,
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
      m_tapeSessionTracker(tapeSessionTracker),
      m_scheduler(scheduler) {}

//------------------------------------------------------------------------------
//DataTransferSession::execute
//------------------------------------------------------------------------------
/**
 * Function's synopsis
 * 1) Prepare the logging environment
 *  Create a sticky thread name, which will be overridden by the other threads
 * 2a) Get initial information from the client
 * 2b) Log The result
 * Then branch to the right execution
 */
cta::tape::daemon::TransferSessionResult cta::tape::daemon::DataTransferSession::execute() {
  // 1) Prepare the logging environment
  cta::log::LogContext lc(m_log);

  cta::utils::Timer t;

  m_volInfo.vid = m_tapeMount.getVid();
  m_volInfo.mountType = m_tapeMount.getMountType();
  m_volInfo.nbFiles = m_tapeMount.getNbFiles();
  m_volInfo.mountId = m_tapeMount.getMountTransactionId();
  m_volInfo.labelFormat = m_tapeMount.getLabelFormat();
  m_volInfo.encryptionKeyName = m_tapeMount.getEncryptionKeyName();
  m_volInfo.tapePool = m_tapeMount.getPoolName();
  // Report drive status and mount info through tapeMount interface
  m_tapeMount.setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeDrive", m_driveInfo.driveName)
    .add("tapeVid", m_volInfo.vid)
    .add("mountId", m_volInfo.mountId)
    .add("vo", m_tapeMount.getVo())
    .add("tapePool", m_tapeMount.getPoolName());
  {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", m_volInfo.mountId).add("mountType", toCamelCaseString(m_volInfo.mountType));
    lc.log(cta::log::INFO, "Got volume from client");
  }

  // Depending on the type of session, branch into the right execution
  switch (m_volInfo.mountType) {
    case cta::common::dataStructures::MountType::Retrieve:
      return executeRead(lc, dynamic_cast<cta::RetrieveMount*>(&m_tapeMount));
    case cta::common::dataStructures::MountType::ArchiveForUser:
    case cta::common::dataStructures::MountType::ArchiveForRepack:
      return executeWrite(lc, dynamic_cast<cta::ArchiveMount*>(&m_tapeMount));
    case cta::common::dataStructures::MountType::Label:
      return executeLabel(lc, dynamic_cast<cta::LabelMount*>(&m_tapeMount));
    default:
      break;
  }

  TransferSessionResult result;
  result.vid = m_volInfo.vid;
  return result;
}

//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
cta::tape::daemon::TransferSessionResult
cta::tape::daemon::DataTransferSession::executeRead(cta::log::LogContext& logContext,
                                                    cta::RetrieveMount* retrieveMount) {
  TransferSessionResult result;
  result.vid = m_volInfo.vid;

  m_tapeSessionTracker.reportState(cta::tape::session::SessionState::Scheduling,
                                   cta::tape::session::SessionType::Retrieve);
  TapeSessionReporter reporter(m_tapeSessionTracker,
                               logContext,
                               std::chrono::seconds(15),
                               std::chrono::seconds(m_transfersConfig.no_block_move_timeout_secs));
  reporter.startThreads();
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A nullptr is returned on failure
  retrieveMount->setExternalFreeDiskSpaceScript(m_transfersConfig.retrieve.external_free_disk_space_script);
  std::unique_ptr<cta::tape::drive::DriveInterface> drive(findDrive(logContext, retrieveMount));

  if (!drive) {
    result.transferOutcome = TransferSessionResult::Outcome::Failure;
    m_tapeSessionTracker.setOutcome(TapeSessionOutcome::Failure);
    m_tapeSessionTracker.setMountAttempted(false);
    reporter.finish();
    reporter.waitThreads();
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  }

  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker reportPacker(retrieveMount, logContext);
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
                                          *retrieveMount,
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
                                    *retrieveMount,
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
                       "DataTransferSession::executeRead Tape LabelFormat incompatible with RAO. Setting RAO false.");
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
      readSingleThread.startThreads();
      threadPool.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      // This thread is now going to be idle until the system unwinds at the end of the session
      // All client notifications are done by the report packer, including the end of session
      taskInjector.waitThreads();
      threadPool.waitThreads();
      readSingleThread.waitThreads();
      reportPacker.waitThread();
      reporter.finish();
      reporter.waitThreads();

      // If the disk thread finished the last, it leaves the drive in DrainingToDisk state
      // Return the drive back to UP state
      if (m_scheduler.getDriveStatus(m_driveInfo.driveName, &logContext)
          == cta::common::dataStructures::DriveStatus::DrainingToDisk) {
        m_scheduler.reportDriveStatus(m_driveInfo,
                                      cta::common::dataStructures::MountType::NoMount,
                                      cta::common::dataStructures::DriveStatus::Up,
                                      logContext);
      }

      result.driveUsability = readSingleThread.getHardwareStatus();
      result.loadingAttempted = readSingleThread.loadingAttempted();
      return result;
    } else {
      // If the first pop from the queue fails, just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      if (noFilesToRecall) {
        // If empty mount because the queue contained no jobs log warning and set success
        priority = cta::log::WARNING;
      }

      result.transferOutcome =
        noFilesToRecall ? TransferSessionResult::Outcome::Success : TransferSessionResult::Outcome::Failure;
      result.hardwareCleanupOutcome = TransferSessionResult::Outcome::NotRequired;
      logContext.log(priority, "Aborting recall mount startup: empty mount");

      std::string mountId = retrieveMount->getMountTransactionId();

      cta::log::Param errorMessageParam(cta::semconv::log::errorMessage, "Aborted: empty recall mount");

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        retrieveMount->complete();
        m_tapeSessionTracker.updateTapeTransferStats({});
        if (!reservationResult) {
          m_tapeSessionTracker.incrementError(TapeSessionError::DiskSpaceReservationTestFailure);
        }
        if (!noFilesToRecall) {
          m_tapeSessionTracker.incrementError(TapeSessionError::NoFilesToRecall);
        }
        m_tapeSessionTracker.incrementError(TapeSessionError::EmptyMount);
        m_tapeSessionTracker.setOutcome(noFilesToRecall ? TapeSessionOutcome::Success : TapeSessionOutcome::Failure);
        m_tapeSessionTracker.setMountAttempted(false);
        cta::log::LogContext::ScopedParam sp08(logContext, cta::log::Param("MountTransactionId", mountId));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        result.reportingFinalizationOutcome = TransferSessionResult::Outcome::Failure;
        cta::log::LogContext::ScopedParam sp12(
          logContext,
          cta::log::Param(cta::semconv::log::exceptionMessage, ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware is OK
      m_scheduler.reportDriveStatus(m_driveInfo,
                                    cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Up,
                                    logContext);
      reporter.finish();
      reporter.waitThreads();
      return result;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeWrite
//------------------------------------------------------------------------------
cta::tape::daemon::TransferSessionResult
cta::tape::daemon::DataTransferSession::executeWrite(cta::log::LogContext& logContext,
                                                     cta::ArchiveMount* archiveMount) {
  TransferSessionResult result;
  result.vid = m_volInfo.vid;

  m_tapeSessionTracker.reportState(cta::tape::session::SessionState::Scheduling,
                                   cta::tape::session::SessionType::Archive);
  TapeSessionReporter reporter(m_tapeSessionTracker,
                               logContext,
                               std::chrono::seconds(15),
                               std::chrono::seconds(m_transfersConfig.no_block_move_timeout_secs));
  reporter.startThreads();
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::unique_ptr<cta::tape::drive::DriveInterface> drive(findDrive(logContext, archiveMount));
  if (!drive) {
    result.transferOutcome = TransferSessionResult::Outcome::Failure;
    m_tapeSessionTracker.setOutcome(TapeSessionOutcome::Failure);
    m_tapeSessionTracker.setMountAttempted(false);
    reporter.finish();
    reporter.waitThreads();
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  }
  // Once we got hold of the drive, we can run the session
  {
    //dereferencing configLine is safe, because if configLine were not valid,
    //then findDrive would have return nullptr and we would have not end up there
    MigrationMemoryManager memoryManager(m_transfersConfig.buffer_count,
                                         m_transfersConfig.buffer_size_bytes,
                                         logContext);
    MigrationReportPacker reportPacker(archiveMount, logContext);
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
                                            *archiveMount,
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
                                       *archiveMount,
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
      memoryManager.startThreads();
      threadPool.startThreads();
      writeSingleThread.setWaitForInstructionsTime(timer.secs());
      writeSingleThread.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      // Synchronise with end of threads
      taskInjector.waitThreads();
      writeSingleThread.waitThreads();
      threadPool.waitThreads();
      memoryManager.waitThreads();
      reportPacker.waitThread();
      reporter.finish();
      reporter.waitThreads();

      result.driveUsability = writeSingleThread.getHardwareStatus();
      result.loadingAttempted = writeSingleThread.loadingAttempted();
      return result;
    } else {
      // Just log this was an empty mount and that's it. The memory management will be deallocated automatically.
      int priority = cta::log::ERR;
      if (noFilesToMigrate) {
        priority = cta::log::WARNING;
      }
      result.transferOutcome =
        noFilesToMigrate ? TransferSessionResult::Outcome::Success : TransferSessionResult::Outcome::Failure;
      result.hardwareCleanupOutcome = TransferSessionResult::Outcome::NotRequired;
      logContext.log(priority, "Aborting migration mount startup: empty mount");

      std::string mountId = archiveMount->getMountTransactionId();
      cta::log::Param errorMessageParam(cta::semconv::log::errorMessage, "Aborted: empty migration mount");

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        archiveMount->complete();
        m_tapeSessionTracker.updateTapeTransferStats({});
        if (noFilesToMigrate) {
          m_tapeSessionTracker.incrementError(TapeSessionError::NoFilesToMigrate);
        }
        m_tapeSessionTracker.incrementError(TapeSessionError::EmptyMount);
        m_tapeSessionTracker.setOutcome(noFilesToMigrate ? TapeSessionOutcome::Success : TapeSessionOutcome::Failure);
        m_tapeSessionTracker.setMountAttempted(false);
        cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("MountTransactionId", mountId));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        result.reportingFinalizationOutcome = TransferSessionResult::Outcome::Failure;
        cta::log::LogContext::ScopedParam sp12(
          logContext,
          cta::log::Param(cta::semconv::log::exceptionMessage, ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware safe
      m_scheduler.reportDriveStatus(m_driveInfo,
                                    cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Up,
                                    logContext);
      reporter.finish();
      reporter.waitThreads();
      return result;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeLabel
//------------------------------------------------------------------------------
cta::tape::daemon::TransferSessionResult
cta::tape::daemon::DataTransferSession::executeLabel([[maybe_unused]] cta::log::LogContext& logContext,
                                                     [[maybe_unused]] cta::LabelMount* labelMount) const {
  throw cta::exception::NotImplementedException();
  // TODO
}

//------------------------------------------------------------------------------
//DataTransferSession::findDrive
//------------------------------------------------------------------------------
/*
 * Function synopsis  :
 *  1) Get hold of the drive and check it.
 *  --- Check If we did not find the configured drive, we have a problem
 *  2) Try to find the drive
 *    Log if we do not find it
 *  3) Try to open it, log if we fail
 */
/**
 * Try to find the drive that is described by m_request.driveUnit
 * @param logContext For logging purpose
 * @return the drive if found, nullptr otherwise
 */
cta::tape::drive::DriveInterface* cta::tape::daemon::DataTransferSession::findDrive(cta::log::LogContext& logContext,
                                                                                    cta::TapeMount* mount) {
  // Find the drive in the system's SCSI devices
  cta::tape::SCSI::DeviceVector dv(m_sysWrapper);
  cta::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(m_driveInfo.devFilename);
  } catch (cta::tape::SCSI::DeviceVector::NotFound&) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown(common::dataStructures::DriveDownReason::DriveNotFound, mount, logContext);
    return nullptr;
  } catch (cta::exception::Exception& ex) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown(common::dataStructures::DriveDownReason::DriveDiscoveryFailed,
                 mount,
                 logContext,
                 ex.getMessageValue());
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown(common::dataStructures::DriveDownReason::DriveDiscoveryFailed, mount, logContext);
    return nullptr;
  }
  try {
    auto drive = cta::tape::drive::createDrive(driveInfo, m_sysWrapper);
    if (drive) {
      drive->info = m_driveInfo;
    }
    return drive.release();
  } catch (cta::exception::Exception& ex) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown(common::dataStructures::DriveDownReason::DriveOpenFailed, mount, logContext, ex.getMessageValue());
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown(common::dataStructures::DriveDownReason::DriveOpenFailed, mount, logContext);
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Get drive down with reason
//------------------------------------------------------------------------------
void cta::tape::daemon::DataTransferSession::putDriveDown(common::dataStructures::DriveDownReason reason,
                                                          cta::TapeMount* mount,
                                                          cta::log::LogContext& logContext,
                                                          std::string_view detail) {
  const auto headerErrMsg = common::dataStructures::formatDriveDownReason(reason, detail);
  cta::log::ScopedParamContainer params(logContext);
  params.add("devFilename", m_driveInfo.devFilename).add(cta::semconv::log::errorMessage, headerErrMsg);

  if (mount) {
    mount->complete();
    params.add("tapebridgeTransId", mount->getMountTransactionId())
      .add("mountType", mount->getMountType())
      .add("pool", mount->getPoolName())
      .add("VO", mount->getVo());
  }

  logContext.log(common::dataStructures::driveDownReasonSeverity(reason), headerErrMsg);

  m_scheduler.reportDriveStatus(m_driveInfo,
                                cta::common::dataStructures::MountType::NoMount,
                                cta::common::dataStructures::DriveStatus::Down,
                                logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  driveState.reason = headerErrMsg;
  m_scheduler.setDesiredDriveState(m_driveInfo.driveName, driveState, logContext);

  logContext.log(cta::log::ERR, "Notified client of end session with error");
}
