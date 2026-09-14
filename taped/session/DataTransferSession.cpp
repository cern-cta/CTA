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
#include "common/process/threading/System.hpp"
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

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
cta::tape::daemon::DataTransferSession::DataTransferSession([[maybe_unused]] const std::string& hostname,
                                                            cta::log::Logger& log,
                                                            System::virtualWrapper& sysWrapper,
                                                            const cta::common::dataStructures::DriveInfo& driveInfo,
                                                            cta::mediachanger::MediaChangerFacade& mc,
                                                            std::unique_ptr<cta::TapeMount> tapeMount,
                                                            cta::tape::daemon::TapeSessionTracker& tapeSessionTracker,
                                                            const DataTransferConfig& dataTransferConfig,
                                                            cta::Scheduler& scheduler)
    : m_log(log),
      m_tapeMount(std::move(tapeMount)),
      m_sysWrapper(sysWrapper),
      m_dataTransferConfig(dataTransferConfig),
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
cta::tape::daemon::Session::EndOfSessionAction cta::tape::daemon::DataTransferSession::execute() {
  // 1) Prepare the logging environment
  cta::log::LogContext lc(m_log);

  cta::utils::Timer t;

  m_volInfo.vid = m_tapeMount->getVid();
  m_volInfo.mountType = m_tapeMount->getMountType();
  m_volInfo.nbFiles = m_tapeMount->getNbFiles();
  m_volInfo.mountId = m_tapeMount->getMountTransactionId();
  m_volInfo.labelFormat = m_tapeMount->getLabelFormat();
  m_volInfo.encryptionKeyName = m_tapeMount->getEncryptionKeyName();
  m_volInfo.tapePool = m_tapeMount->getPoolName();
  // Report drive status and mount info through tapeMount interface
  m_tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeVid", m_volInfo.vid)
    .add("mountId", m_volInfo.mountId)
    .add("vo", m_tapeMount->getVo())
    .add("tapePool", m_tapeMount->getPoolName());
  {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", m_volInfo.mountId).add("mountType", toCamelCaseString(m_volInfo.mountType));
    lc.log(cta::log::INFO, "Got volume from client");
  }

  // Depending on the type of session, branch into the right execution
  switch (m_volInfo.mountType) {
    case cta::common::dataStructures::MountType::Retrieve:
      return executeRead(lc, dynamic_cast<cta::RetrieveMount*>(m_tapeMount.get()));
    case cta::common::dataStructures::MountType::ArchiveForUser:
    case cta::common::dataStructures::MountType::ArchiveForRepack:
      return executeWrite(lc, dynamic_cast<cta::ArchiveMount*>(m_tapeMount.get()));
    case cta::common::dataStructures::MountType::Label:
      return executeLabel(lc, dynamic_cast<cta::LabelMount*>(m_tapeMount.get()));
    default:
      return MARK_DRIVE_AS_UP;
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
cta::tape::daemon::Session::EndOfSessionAction
cta::tape::daemon::DataTransferSession::executeRead(cta::log::LogContext& logContext,
                                                    cta::RetrieveMount* retrieveMount) {
  m_tapeSessionTracker.reportState(cta::tape::session::SessionState::Scheduling,
                                   cta::tape::session::SessionType::Retrieve);
  TapeSessionReporter reporter(m_tapeSessionTracker,
                               *retrieveMount,
                               logContext,
                               std::chrono::seconds(15),
                               std::chrono::seconds(m_dataTransferConfig.wdNoBlockMoveMaxSecs));
  reporter.startThreads();
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A nullptr is returned on failure
  retrieveMount->setExternalFreeDiskSpaceScript(m_dataTransferConfig.externalFreeDiskSpaceScript);
  std::unique_ptr<cta::tape::drive::DriveInterface> drive(findDrive(logContext, retrieveMount));

  if (!drive) {
    m_tapeSessionTracker.setOutcome(TapeSessionOutcome::Failure);
    m_tapeSessionTracker.setMountAttempted(false);
    reporter.finish();
    reporter.waitThreads();
    return MARK_DRIVE_AS_DOWN;
  }

  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker reportPacker(retrieveMount, logContext);
    reportPacker.disableBulk();  //no bulk needed anymore
    RecallMemoryManager memoryManager(m_dataTransferConfig.nbBufs, m_dataTransferConfig.bufsz, logContext);

    TapeReadSingleThread readSingleThread(*drive,
                                          m_mediaChanger,
                                          m_tapeSessionTracker,
                                          m_volInfo,
                                          m_dataTransferConfig.bulkRequestRecallMaxFiles,
                                          logContext,
                                          reportPacker,
                                          m_dataTransferConfig.useLbp,
                                          m_dataTransferConfig.useRAO,
                                          m_dataTransferConfig.useEncryption,
                                          m_dataTransferConfig.externalEncryptionKeyScript,
                                          *retrieveMount,
                                          m_dataTransferConfig.tapeLoadTimeout,
                                          m_scheduler.getCatalogue());

    DiskWriteThreadPool threadPool(m_dataTransferConfig.nbDiskThreads,
                                   reportPacker,
                                   m_tapeSessionTracker,
                                   logContext,
                                   m_dataTransferConfig.xrootTimeout);
    RecallTaskInjector taskInjector(memoryManager,
                                    readSingleThread,
                                    threadPool,
                                    *retrieveMount,
                                    m_dataTransferConfig.bulkRequestRecallMaxFiles,
                                    m_dataTransferConfig.bulkRequestRecallMaxBytes,
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
    if (m_dataTransferConfig.useRAO) {
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
        cta::tape::rao::RAOParams raoDataConfig(m_dataTransferConfig.useRAO,
                                                m_dataTransferConfig.raoLtoAlgorithm,
                                                m_dataTransferConfig.raoLtoAlgorithmOptions,
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

      return readSingleThread.getHardwareStatus();
    } else {
      // If the first pop from the queue fails, just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      if (noFilesToRecall) {
        // If empty mount because the queue contained no jobs log warning and set success
        priority = cta::log::WARNING;
      }

      logContext.log(priority, "Aborting recall mount startup: empty mount");

      std::string mountId = retrieveMount->getMountTransactionId();

      cta::log::Param errorMessageParam(cta::semconv::log::errorMessage, "Aborted: empty recall mount");

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        retrieveMount->complete();
        m_tapeSessionTracker.updateTapeStats({});
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
      return MARK_DRIVE_AS_UP;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeWrite
//------------------------------------------------------------------------------
cta::tape::daemon::Session::EndOfSessionAction
cta::tape::daemon::DataTransferSession::executeWrite(cta::log::LogContext& logContext,
                                                     cta::ArchiveMount* archiveMount) {
  m_tapeSessionTracker.reportState(cta::tape::session::SessionState::Scheduling,
                                   cta::tape::session::SessionType::Archive);
  TapeSessionReporter reporter(m_tapeSessionTracker,
                               *archiveMount,
                               logContext,
                               std::chrono::seconds(15),
                               std::chrono::seconds(m_dataTransferConfig.wdNoBlockMoveMaxSecs));
  reporter.startThreads();
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::unique_ptr<cta::tape::drive::DriveInterface> drive(findDrive(logContext, archiveMount));
  if (!drive) {
    m_tapeSessionTracker.setOutcome(TapeSessionOutcome::Failure);
    m_tapeSessionTracker.setMountAttempted(false);
    reporter.finish();
    reporter.waitThreads();
    return MARK_DRIVE_AS_DOWN;
  }
  // Once we got hold of the drive, we can run the session
  {
    //dereferencing configLine is safe, because if configLine were not valid,
    //then findDrive would have return nullptr and we would have not end up there
    MigrationMemoryManager memoryManager(m_dataTransferConfig.nbBufs, m_dataTransferConfig.bufsz, logContext);
    MigrationReportPacker reportPacker(archiveMount, logContext);
    TapeWriteSingleThread writeSingleThread(*drive,
                                            m_mediaChanger,
                                            m_tapeSessionTracker,
                                            m_volInfo,
                                            logContext,
                                            reportPacker,
                                            m_dataTransferConfig.maxFilesBeforeFlush,
                                            m_dataTransferConfig.maxBytesBeforeFlush,
                                            m_dataTransferConfig.useLbp,
                                            m_dataTransferConfig.useEncryption,
                                            m_dataTransferConfig.externalEncryptionKeyScript,
                                            *archiveMount,
                                            m_dataTransferConfig.tapeLoadTimeout,
                                            m_scheduler.getCatalogue());

    DiskReadThreadPool threadPool(m_dataTransferConfig.nbDiskThreads,
                                  m_dataTransferConfig.bulkRequestMigrationMaxFiles,
                                  m_dataTransferConfig.bulkRequestMigrationMaxBytes,
                                  m_tapeSessionTracker,
                                  logContext,
                                  m_dataTransferConfig.xrootTimeout);

    MigrationTaskInjector taskInjector(memoryManager,
                                       threadPool,
                                       writeSingleThread,
                                       *archiveMount,
                                       m_dataTransferConfig.bulkRequestMigrationMaxFiles,
                                       m_dataTransferConfig.bulkRequestMigrationMaxBytes,
                                       m_dataTransferConfig.archiveDismountPolicy,
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

      return writeSingleThread.getHardwareStatus();
    } else {
      // Just log this was an empty mount and that's it. The memory management will be deallocated automatically.
      int priority = cta::log::ERR;
      if (noFilesToMigrate) {
        priority = cta::log::WARNING;
      }
      logContext.log(priority, "Aborting migration mount startup: empty mount");

      std::string mountId = archiveMount->getMountTransactionId();
      cta::log::Param errorMessageParam(cta::semconv::log::errorMessage, "Aborted: empty migration mount");

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        archiveMount->complete();
        m_tapeSessionTracker.updateTapeStats({});
        if (noFilesToMigrate) {
          m_tapeSessionTracker.incrementError(TapeSessionError::NoFilesToMigrate);
        }
        m_tapeSessionTracker.incrementError(TapeSessionError::EmptyMount);
        m_tapeSessionTracker.setOutcome(noFilesToMigrate ? TapeSessionOutcome::Success : TapeSessionOutcome::Failure);
        m_tapeSessionTracker.setMountAttempted(false);
        cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("MountTransactionId", mountId));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
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
      return MARK_DRIVE_AS_UP;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeLabel
//------------------------------------------------------------------------------
cta::tape::daemon::Session::EndOfSessionAction
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
    putDriveDown("Drive not found on this path", mount, logContext);
    return nullptr;
  } catch (cta::exception::Exception&) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Error looking for path to tape drive", mount, logContext);
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Unexpected exception while looking for drive", mount, logContext);
    return nullptr;
  }
  try {
    auto drive = cta::tape::drive::createDrive(driveInfo, m_sysWrapper);
    if (drive) {
      drive->info = m_driveInfo;
    }
    return drive.release();
  } catch (cta::exception::Exception&) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Error opening tape drive", mount, logContext);
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Unexpected exception while opening drive", mount, logContext);
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Get drive down with reason
//------------------------------------------------------------------------------
void cta::tape::daemon::DataTransferSession::putDriveDown(const std::string& headerErrMsg,
                                                          cta::TapeMount* mount,
                                                          cta::log::LogContext& logContext) {
  cta::log::ScopedParamContainer params(logContext);
  params.add("devFilename", m_driveInfo.devFilename).add(cta::semconv::log::errorMessage, headerErrMsg);

  if (mount) {
    mount->complete();
    params.add("tapebridgeTransId", mount->getMountTransactionId())
      .add("mountType", mount->getMountType())
      .add("pool", mount->getPoolName())
      .add("VO", mount->getVo());
  }

  logContext.log(cta::log::ERR, headerErrMsg);

  m_scheduler.reportDriveStatus(m_driveInfo,
                                cta::common::dataStructures::MountType::NoMount,
                                cta::common::dataStructures::DriveStatus::Down,
                                logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  driveState.setReasonFromLogMsg(cta::log::ERR, headerErrMsg);
  m_scheduler.setDesiredDriveState(m_driveInfo.driveName, driveState, logContext);

  logContext.log(cta::log::ERR, "Notified client of end session with error");
}

cta::tape::daemon::DataTransferSession::~DataTransferSession() noexcept = default;
