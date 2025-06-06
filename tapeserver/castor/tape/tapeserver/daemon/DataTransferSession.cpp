/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/threading/System.hpp"
#include "castor/tape/tapeserver/daemon/EmptyDriveProbe.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeoutException.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "castor/tape/tapeserver/RAO/RAOParams.hpp"
#include "TapeSessionReporter.hpp"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <string>

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(const std::string& hostname,
                                                                           cta::log::Logger& log,
                                                                           System::virtualWrapper& sysWrapper,
                                                                           const cta::tape::daemon::DriveConfigEntry& driveConfig,
                                                                           cta::mediachanger::MediaChangerFacade& mc,
                                                                           cta::tape::daemon::TapedProxy& initialProcess,

                                                                           const DataTransferConfig& dataTransferConfig,
                                                                           cta::Scheduler& scheduler) :
  m_log(log),
  m_sysWrapper(sysWrapper),
  m_driveConfig(driveConfig),
  m_dataTransferConfig(dataTransferConfig),
  m_driveInfo({driveConfig.unitName, cta::utils::getShortHostname(), driveConfig.logicalLibrary}),
  m_mediaChanger(mc),
  m_initialProcess(initialProcess),
  m_scheduler(scheduler) {
}

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
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
castor::tape::tapeserver::daemon::DataTransferSession::execute() {
  // 1) Prepare the logging environment
  cta::log::LogContext lc(m_log);
  // Create a sticky thread name, which will be overridden by the other threads
  lc.pushOrReplace(cta::log::Param("thread", "MainThread"));
  lc.pushOrReplace(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  // Make effective the raw I/O process capability.
  try {
    cta::server::ProcessCap::setProcText("cap_sys_rawio+ep");
    cta::log::LogContext::ScopedParam sp(lc, cta::log::Param("capabilities",
                                             cta::server::ProcessCap::getProcText()));
    lc.log(cta::log::INFO, "DataTransferSession made effective raw I/O capabilty to the tape");
  } catch (const cta::exception::Exception &ex) {
    lc.log(cta::log::ERR, "DataTransferSession failed to make effective raw I/O capabilty to use tape");
  }

  TapeSessionReporter tapeServerReporter(m_initialProcess, m_driveConfig, m_hostname, lc);

  std::unique_ptr<cta::TapeMount> tapeMount;
  cta::utils::Timer t;
  bool nextMountTimeout = false;

  // 2a) Determine if we want to mount at all (for now)
  // This variable will allow us to see if we switched from down to up and start an
  // empty drive probe session if so.
  bool downUpTransition = false;

  // -----------------------------------------------------------------------------------
  // Scheduling loop
  while (true) {
    // Down-up transition loop
    while (true) {
      try {
        lc.log(cta::log::DEBUG, "Transition from down to up starting.");
        auto desiredState = m_scheduler.getDesiredDriveState(m_driveConfig.unitName, lc);
        if (!desiredState.up) {
          lc.log(cta::log::DEBUG, "Desired drive state is NOT UP, setting it DOWN");
          downUpTransition = true;
          // Refresh the status to trigger the timeout update
          m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                        cta::common::dataStructures::DriveStatus::Down, lc);

          // We wait a bit before polling the scheduler again.
          // TODO: parametrize the duration?
          sleep(5);
        } else {
          lc.log(cta::log::DEBUG, "Desired drive state is UP.");
          break;
        }
      } catch (cta::Scheduler::NoSuchDrive &e) {
        // The object store does not even know about this drive. We will report our state
        // (default status is down).
        putDriveDown(e.getMessageValue(), nullptr, lc);
        return MARK_DRIVE_AS_DOWN;
      }
    }

    // If we get here after seeing a down desired state, we are transitioning  from
    // down to up. In such a case, we will run an empty
    if (downUpTransition) {
      downUpTransition = false;
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Probing, lc);
      castor::tape::tapeserver::daemon::EmptyDriveProbe emptyDriveProbe(m_log, m_driveConfig, m_sysWrapper);
      lc.log(cta::log::INFO, "Transition from down to up detected. Will check if a tape is in the drive.");
      if (!emptyDriveProbe.driveIsEmpty()) {
        std::string errorMsg = "A tape was detected in the drive. Putting the drive down.";
        std::optional<std::string> probeErrorMsg = emptyDriveProbe.getProbeErrorMsg();
        if (probeErrorMsg) {
          errorMsg = probeErrorMsg.value();
        }
        putDriveDown(errorMsg, nullptr, lc);
        continue;
      } else {
        lc.log(cta::log::INFO, "No tape detected in the drive. Proceeding with scheduling.");
      }
    }
    // 2b) Get initial mount information
    // As getting next mount could be long, we report the drive as up immediately.
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                  cta::common::dataStructures::DriveStatus::Up, lc);
    tapeServerReporter.reportState(cta::tape::session::SessionState::Scheduling,
                                   cta::tape::session::SessionType::Undetermined);

    if (!nextMountTimeout) {
      t.reset();
    }

    nextMountTimeout = false;
    try {
      if (m_scheduler.getNextMountDryRun(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc)) {
        tapeMount = m_scheduler.getNextMount(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc,
                                             m_dataTransferConfig.wdGetNextMountMaxSecs * 1000000);
      }
    } catch (cta::exception::TimeoutException &e) {
      // Print warning and try again, after refreshing the tape drive states
      cta::log::ScopedParamContainer params(lc);
      params.add("totalScheduleMountTime", std::to_string(t.secs()));
      lc.log(cta::log::WARNING,
             "Timed out while scheduling new mount. Could not acquire global scheduler lock in " + std::to_string(m_dataTransferConfig.wdGetNextMountMaxSecs) + " seconds.");

      // We found a mount but got timedout while trying to acquire the global lock.
      nextMountTimeout = true;
    } catch (cta::exception::Exception &e) {
      lc.log(cta::log::ERR, "Error while scheduling new mount. Putting the drive down. Stack trace follows.");
      lc.logBacktrace(cta::log::INFO, e.backtrace());
      putDriveDown(e.getMessageValue(), nullptr, lc);
      return MARK_DRIVE_AS_DOWN;
    }

    // No mount to be done found, that was fast...
    if (!tapeMount) {
      // Refresh the status to trigger the timeout update
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Up, lc);

      // Only sleep if we didn't found work to do.
      if (!nextMountTimeout) {
        lc.log(cta::log::DEBUG, "No new mount found. (sleeping " + std::to_string(m_dataTransferConfig.wdIdleSessionTimer) + " seconds)");
        sleep(m_dataTransferConfig.wdIdleSessionTimer);
      }
      continue;
    }
    break;
  }
  // end of scheduling loop
  // -----------------------------------------------------------------------------------

  m_volInfo.vid = tapeMount->getVid();
  m_volInfo.mountType = tapeMount->getMountType();
  m_volInfo.nbFiles = tapeMount->getNbFiles();
  m_volInfo.mountId = tapeMount->getMountTransactionId();
  m_volInfo.labelFormat = tapeMount->getLabelFormat();
  m_volInfo.encryptionKeyName = tapeMount->getEncryptionKeyName();
  m_volInfo.tapePool = tapeMount->getPoolName();
  tapeServerReporter.setVolInfo(m_volInfo);
  // Report drive status and mount info through tapeMount interface
  tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeVid", m_volInfo.vid)
        .add("mountId", m_volInfo.mountId)
        .add("vo", tapeMount->getVo())
        .add("tapePool", tapeMount->getPoolName());
  {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", m_volInfo.mountId)
               .add("mountType", toCamelCaseString(m_volInfo.mountType));
    lc.log(cta::log::INFO, "Got volume from client");
  }

  // Depending on the type of session, branch into the right execution
  switch (m_volInfo.mountType) {
    case cta::common::dataStructures::MountType::Retrieve:
      return executeRead(lc, dynamic_cast<cta::RetrieveMount *>(tapeMount.get()), tapeServerReporter);
    case cta::common::dataStructures::MountType::ArchiveForUser:
    case cta::common::dataStructures::MountType::ArchiveForRepack:
      return executeWrite(lc, dynamic_cast<cta::ArchiveMount *>(tapeMount.get()), tapeServerReporter);
    case cta::common::dataStructures::MountType::Label:
      return executeLabel(lc, dynamic_cast<cta::LabelMount *>(tapeMount.get()));
    default:
      return MARK_DRIVE_AS_UP;
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
castor::tape::tapeserver::daemon::DataTransferSession::executeRead(cta::log::LogContext& logContext,
                                                                   cta::RetrieveMount *retrieveMount,
                                                                   TapeSessionReporter& reporter) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A nullptr is returned on failure
  retrieveMount->setExternalFreeDiskSpaceScript(m_dataTransferConfig.externalFreeDiskSpaceScript);
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(logContext, retrieveMount));

  if (!drive) {
    reporter.bailout();
    return MARK_DRIVE_AS_DOWN;
  }
  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker reportPacker(retrieveMount, logContext);
    reportPacker.disableBulk(); //no bulk needed anymore
    RecallWatchDog watchDog(15, m_dataTransferConfig.wdNoBlockMoveMaxSecs, m_initialProcess, *retrieveMount,
                            m_driveConfig.unitName, logContext);

    RecallMemoryManager memoryManager(m_dataTransferConfig.nbBufs, m_dataTransferConfig.bufsz, logContext);

    TapeReadSingleThread readSingleThread(*drive, m_mediaChanger, reporter, m_volInfo,
                                          m_dataTransferConfig.bulkRequestRecallMaxFiles,  watchDog, logContext,
                                          reportPacker,
                                          m_dataTransferConfig.useLbp, m_dataTransferConfig.useRAO, m_dataTransferConfig.useEncryption,
                                          m_dataTransferConfig.externalEncryptionKeyScript, *retrieveMount,
                                          m_dataTransferConfig.tapeLoadTimeout,
                                          m_scheduler.getCatalogue());

    DiskWriteThreadPool threadPool(m_dataTransferConfig.nbDiskThreads,
                                   reportPacker,
                                   watchDog,
                                   logContext,
                                   m_dataTransferConfig.xrootTimeout);
    RecallTaskInjector taskInjector(memoryManager, readSingleThread, threadPool, *retrieveMount,
                                    m_dataTransferConfig.bulkRequestRecallMaxFiles,
                                    m_dataTransferConfig.bulkRequestRecallMaxBytes,
                                    watchDog,
                                    logContext);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type of the client
    readSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setWatchdog(watchDog);

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
        format_str << std::showbase << std::internal << std::setfill('0')
                   << std::hex << std::setw(4) << static_cast<unsigned int>(format);
        cta::log::ScopedParamContainer params(logContext);
        params.add("tapeVid", m_volInfo.vid)
              .add("mountId", m_volInfo.mountId)
              .add("labelFormat", format_str.str());
        logContext.log(cta::log::INFO, "DataTransferSession::executeRead Tape LabelFormat incompatible with RAO. Setting RAO false.");
      } else {
        castor::tape::tapeserver::rao::RAOParams raoDataConfig(m_dataTransferConfig.useRAO, m_dataTransferConfig.raoLtoAlgorithm,
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
      watchDog.startThread();
      readSingleThread.startThreads();
      threadPool.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      reporter.startThreads();
      // This thread is now going to be idle until the system unwinds at the end of the session
      // All client notifications are done by the report packer, including the end of session
      taskInjector.waitThreads();
      threadPool.waitThreads();
      readSingleThread.waitThreads();
      reportPacker.waitThread();
      reporter.waitThreads();
      watchDog.stopAndWaitThread();

      // If the disk thread finished the last, it leaves the drive in DrainingToDisk state
      // Return the drive back to UP state
      if (m_scheduler.getDriveState(m_driveInfo.driveName, &logContext)->driveStatus ==
          cta::common::dataStructures::DriveStatus::DrainingToDisk) {
        m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                      cta::common::dataStructures::DriveStatus::Up, logContext);
      }

      return readSingleThread.getHardwareStatus();
    }
    else {
      // If the first pop from the queue fails, just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "failure";
      if (noFilesToRecall) {
        // If empty mount because the queue contained no jobs log warning and set success
        priority = cta::log::WARNING;
        status = "success";
      }

      logContext.log(priority, "Aborting recall mount startup: empty mount");

      std::string mountId = retrieveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toCamelCaseString(retrieveMount->getMountType());

      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty recall mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType", mountType);
      cta::log::Param statusParam("status", status);
      cta::log::Param mountAttemptedParam("mountAttempted", 0);

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        retrieveMount->complete();
        watchDog.updateStats(TapeSessionStats());
        watchDog.reportStats();
        if (!reservationResult) {
          watchDog.addToErrorCount("Info_diskSpaceReservationTestFailure");
        }
        if (!noFilesToRecall) {
          watchDog.addToErrorCount("Info_noFilesToRecall");
        }
        watchDog.addToErrorCount("Info_emptyMount");
        watchDog.reportParams();
        std::list<cta::log::Param> paramList {errorMessageParam, mountIdParam, mountTypeParam, statusParam,
                                              mountAttemptedParam};
        m_initialProcess.addLogParams(paramList);
        cta::log::LogContext::ScopedParam sp08(logContext, cta::log::Param("MountTransactionId", mountId));
        cta::log::LogContext::ScopedParam sp11(logContext,
                                               cta::log::Param("errorMessage", "Aborted: empty recall mount"));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        cta::log::LogContext::ScopedParam sp12(logContext, cta::log::Param("notificationError", ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware is OK
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Up, logContext);
      return MARK_DRIVE_AS_UP;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeWrite
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
castor::tape::tapeserver::daemon::DataTransferSession::executeWrite(cta::log::LogContext& logContext,
                                                                    cta::ArchiveMount *archiveMount,
                                                                    TapeSessionReporter& reporter) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(logContext, archiveMount));
  if (!drive) {
    reporter.bailout();
    return MARK_DRIVE_AS_DOWN;
  }
  // Once we got hold of the drive, we can run the session
  {
    //dereferencing configLine is safe, because if configLine were not valid,
    //then findDrive would have return nullptr and we would have not end up there
    MigrationMemoryManager memoryManager(m_dataTransferConfig.nbBufs,
                                         m_dataTransferConfig.bufsz, logContext);
    MigrationReportPacker reportPacker(archiveMount, logContext);
    MigrationWatchDog watchDog(15, m_dataTransferConfig.wdNoBlockMoveMaxSecs, m_initialProcess, *archiveMount,
                               m_driveConfig.unitName, logContext);
    TapeWriteSingleThread writeSingleThread(*drive,
                                            m_mediaChanger,
                                            reporter,
                                            watchDog,
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
                                  watchDog,
                                  logContext,
                                  m_dataTransferConfig.xrootTimeout);
    MigrationTaskInjector taskInjector(memoryManager, threadPool, writeSingleThread, *archiveMount,
                                       m_dataTransferConfig.bulkRequestMigrationMaxFiles,
                                       m_dataTransferConfig.bulkRequestMigrationMaxBytes, logContext);
    threadPool.setTaskInjector(&taskInjector);
    writeSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setWatchdog(watchDog);
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
      watchDog.startThread();
      writeSingleThread.setWaitForInstructionsTime(timer.secs());
      writeSingleThread.startThreads();
      reportPacker.startThreads();
      taskInjector.startThreads();
      reporter.startThreads();
      // Synchronise with end of threads
      taskInjector.waitThreads();
      writeSingleThread.waitThreads();
      threadPool.waitThreads();
      memoryManager.waitThreads();
      reportPacker.waitThread();
      reporter.waitThreads();
      watchDog.stopAndWaitThread();

      return writeSingleThread.getHardwareStatus();
    }
    else {
      // Just log this was an empty mount and that's it. The memory management will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "failure";
      if (noFilesToMigrate) {
        priority = cta::log::WARNING;
        status = "success";
      }
      logContext.log(priority, "Aborting migration mount startup: empty mount");

      std::string mountId = archiveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toCamelCaseString(archiveMount->getMountType());
      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty migration mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType", mountType);
      cta::log::Param statusParam("status", status);
      cta::log::Param mountAttemptedParam("mountAttempted", 0);

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        archiveMount->complete();
        watchDog.updateStats(TapeSessionStats());
        watchDog.reportStats();
        if (noFilesToMigrate) {
          watchDog.addToErrorCount("Info_noFilesToMigrate");
        }
        watchDog.addToErrorCount("Info_emptyMount");
        watchDog.reportParams();
        std::list<cta::log::Param> paramList {errorMessageParam, mountIdParam, mountTypeParam, statusParam,
                                              mountAttemptedParam};
        m_initialProcess.addLogParams(paramList);
        cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("MountTransactionId", mountId));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        cta::log::LogContext::ScopedParam sp12(logContext, cta::log::Param("notificationError", ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware safe
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                    cta::common::dataStructures::DriveStatus::Up, logContext);
      return MARK_DRIVE_AS_UP;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeLabel
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
castor::tape::tapeserver::daemon::DataTransferSession::executeLabel(cta::log::LogContext& logContext,
                                                                    cta::LabelMount *labelMount) {
  throw cta::exception::Exception("In DataTransferSession::executeLabel(): not implemented");
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
castor::tape::tapeserver::drive::DriveInterface *
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(cta::log::LogContext &logContext,
                                                                 cta::TapeMount *mount) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(m_driveConfig.devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound& e) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Drive not found on this path", mount, logContext);
    return nullptr;
  } catch (cta::exception::Exception& e) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Error looking for path to tape drive", mount, logContext);
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    putDriveDown("Unexpected exception while looking for drive", mount, logContext);
    return nullptr;
  }
  try {
    std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive;
    drive.reset(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));
    if (drive) { drive->config = m_driveConfig; }
    return drive.release();
  } catch (cta::exception::Exception& e) {
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
void castor::tape::tapeserver::daemon::DataTransferSession::putDriveDown(const std::string& headerErrMsg,
                                                                         cta::TapeMount *mount,
                                                                         cta::log::LogContext &logContext) {
  cta::log::ScopedParamContainer params(logContext);
  params.add("devFilename", m_driveConfig.devFilename)
        .add("errorMessage", headerErrMsg);

  if (mount) {
    mount->complete();
    params.add("tapebridgeTransId", mount->getMountTransactionId())
      .add("mountType", mount->getMountType())
      .add("pool", mount->getPoolName())
      .add("VO", mount->getVo());
  }

  logContext.log(cta::log::ERR, headerErrMsg);

  m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount,
                                cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::SecurityIdentity cliId;
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  driveState.setReasonFromLogMsg(cta::log::ERR, headerErrMsg);
  m_scheduler.setDesiredDriveState(cliId, m_driveConfig.unitName, driveState, logContext);

  logContext.log(cta::log::ERR, "Notified client of end session with error");
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::~DataTransferSession() noexcept {
  try {
    google::protobuf::ShutdownProtobufLibrary();
  } catch (...) {
    m_log(cta::log::ERR, "google::protobuf::ShutdownProtobufLibrary() threw an"
                         " unexpected exception");
  }
}
