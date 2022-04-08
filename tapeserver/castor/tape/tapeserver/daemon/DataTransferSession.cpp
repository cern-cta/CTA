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
#include "common/exception/Exception.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "castor/tape/tapeserver/RAO/RAOParams.hpp"
#include "TapeServerReporter.hpp"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <string>

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(const std::string& hostname, cta::log::Logger& log,
                                                                           System::virtualWrapper& sysWrapper,
                                                                           const cta::tape::daemon::TpconfigLine& driveConfig,
                                                                           cta::mediachanger::MediaChangerFacade& mc,
                                                                           cta::tape::daemon::TapedProxy& initialProcess,
                                                                           cta::server::ProcessCap& capUtils, const DataTransferConfig& castorConf,
                                                                           cta::Scheduler& scheduler) :
  m_log(log),
  m_sysWrapper(sysWrapper),
  m_driveConfig(driveConfig),
  m_castorConf(castorConf),
  m_driveInfo({driveConfig.unitName, cta::utils::getShortHostname(), driveConfig.logicalLibrary}),
  m_mediaChanger(mc),
  m_initialProcess(initialProcess),
  m_capUtils(capUtils),
  m_scheduler(scheduler) {
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
/**
 * This function will try to set the cap_sys_rawio capability that is needed
 * for by tape thread to access /dev/nst
 */
void castor::tape::tapeserver::daemon::DataTransferSession::setProcessCapabilities(
  const std::string& capabilities) {
  cta::log::LogContext lc(m_log);
  try {
    m_capUtils.setProcText(capabilities);
    cta::log::LogContext::ScopedParam sp(lc,
                                         cta::log::Param("capabilities", m_capUtils.getProcText()));
    lc.log(cta::log::INFO, "Set process capabilities for using tape");
  } catch (const cta::exception::Exception& ne) {
    lc.log(cta::log::ERR,
           "Failed to set process capabilities for using the tape ");
  }
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

  setProcessCapabilities("cap_sys_rawio+ep");

  TapeServerReporter tapeServerReporter(m_initialProcess, m_driveConfig, m_hostname, m_volInfo, lc);

  // 2a) Determine if we want to mount at all (for now)
  // This variable will allow us to see if we switched from down to up and start an
  // empty drive probe session if so.
  bool downUpTransition = false;
  schedule:
  while (true) {
    try {
      auto desiredState = m_scheduler.getDesiredDriveState(m_driveConfig.unitName, lc);
      if (!desiredState.up) {
        downUpTransition = true;
        // We wait a bit before polling the scheduler again.
        // TODO: parametrize the duration?
        m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down,
                                      lc);
        sleep(5);
      }
      else {
        break;
      }
    } catch (cta::Scheduler::NoSuchDrive& e) {
      // The object store does not even know about this drive. We will report our state
      // (default status is down).
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    }
  }
  // If we get here after seeing a down desired state, we are transitioning  from
  // down to up. In such a case, we will run an empty
  if (downUpTransition) {
    downUpTransition = false;
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Probing,
                                  lc);
    castor::tape::tapeserver::daemon::EmptyDriveProbe emptyDriveProbe(m_log, m_driveConfig, m_sysWrapper);
    lc.log(cta::log::INFO, "Transition from down to up detected. Will check if a tape is in the drive.");
    if (!emptyDriveProbe.driveIsEmpty()) {
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
      cta::common::dataStructures::SecurityIdentity securityIdentity;
      cta::common::dataStructures::DesiredDriveState driveState;
      driveState.up = false;
      driveState.forceDown = false;
      std::string errorMsg = "A tape was detected in the drive. Putting the drive down.";
      std::optional<std::string> probeErrorMsg = emptyDriveProbe.getProbeErrorMsg();
      if (probeErrorMsg)
        errorMsg = probeErrorMsg.value();
      int logLevel = cta::log::ERR;
      driveState.setReasonFromLogMsg(logLevel, errorMsg);
      m_scheduler.setDesiredDriveState(securityIdentity, m_driveConfig.unitName, driveState, lc);
      lc.log(logLevel, errorMsg);
      goto schedule;
    }
    else {
      lc.log(cta::log::INFO, "No tape detected in the drive. Proceeding with scheduling.");
    }
  }
  // 2b) Get initial mount information
  std::unique_ptr<cta::TapeMount> tapeMount;
  // As getting next mount could be long, we report the drive as up immediately.
  m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
  tapeServerReporter.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);

  try {
    if (m_scheduler.getNextMountDryRun(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc))
      tapeMount = m_scheduler.getNextMount(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc);
  } catch (cta::exception::Exception& e) {
    cta::log::ScopedParamContainer localParams(lc);
    std::string exceptionMsg = e.getMessageValue();
    int logLevel = cta::log::ERR;
    localParams.add("errorMessage", exceptionMsg);
    lc.log(logLevel, "Error while scheduling new mount. Putting the drive down. Stack trace follows.");
    lc.logBacktrace(logLevel, e.backtrace());
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    cta::common::dataStructures::SecurityIdentity cliId;
    cta::common::dataStructures::DesiredDriveState driveState;
    driveState.up = false;
    driveState.forceDown = false;
    driveState.setReasonFromLogMsg(cta::log::ERR, exceptionMsg);
    m_scheduler.setDesiredDriveState(cliId, m_driveConfig.unitName, driveState, lc);
    return MARK_DRIVE_AS_DOWN;
  }
  // No mount to be done found, that was fast...
  if (!tapeMount) {
    lc.log(cta::log::DEBUG, "No new mount found. (sleeping 10 seconds)");
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    sleep(10);
    goto schedule;
    // return MARK_DRIVE_AS_UP;
  }
  m_volInfo.vid = tapeMount->getVid();
  m_volInfo.mountType = tapeMount->getMountType();
  m_volInfo.nbFiles = tapeMount->getNbFiles();
  m_volInfo.mountId = tapeMount->getMountTransactionId();
  m_volInfo.labelFormat = tapeMount->getLabelFormat();
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeVid", m_volInfo.vid)
        .add("mountId", tapeMount->getMountTransactionId());
  {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", tapeMount->getMountTransactionId())
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
                                                                   TapeServerReporter& reporter) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A nullptr is returned on failure
  retrieveMount->setFetchEosFreeSpaceScript(m_castorConf.fetchEosFreeSpaceScript);
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig, logContext, retrieveMount));

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
    RecallWatchDog watchDog(15, 60 * 10, m_initialProcess, *retrieveMount, m_driveConfig.unitName, logContext);

    RecallMemoryManager memoryManager(m_castorConf.nbBufs, m_castorConf.bufsz, logContext);

    //we retrieved the detail from the client in execute, so at this point
    //we can already report !
    reporter.reportState(cta::tape::session::SessionState::Mounting,
                           cta::tape::session::SessionType::Retrieve);

    TapeReadSingleThread readSingleThread(*drive, m_mediaChanger, reporter, m_volInfo,
                                          m_castorConf.bulkRequestRecallMaxFiles, m_capUtils, watchDog, logContext, reportPacker,
                                          m_castorConf.useLbp, m_castorConf.useRAO, m_castorConf.useEncryption,
                                          m_castorConf.externalEncryptionKeyScript, *retrieveMount, m_castorConf.tapeLoadTimeout);

    DiskWriteThreadPool threadPool(m_castorConf.nbDiskThreads,
                                   reportPacker,
                                   watchDog,
                                   logContext,
                                   m_castorConf.xrootPrivateKey,
                                   m_castorConf.xrootTimeout);
    RecallTaskInjector taskInjector(memoryManager, readSingleThread, threadPool, *retrieveMount,
                                    m_castorConf.bulkRequestRecallMaxFiles,
                                    m_castorConf.bulkRequestRecallMaxBytes, logContext);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type
    // of the client
    readSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setWatchdog(watchDog);

    taskInjector.setDriveInterface(readSingleThread.getDriveReference());

    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    cta::utils::Timer timer;

    // The RecallTaskInjector and the TapeReadSingleThread share the promise
    if (m_castorConf.useRAO) {
      castor::tape::tapeserver::rao::RAOParams raoDataConfig(m_castorConf.useRAO, m_castorConf.raoLtoAlgorithm, m_castorConf.raoLtoAlgorithmOptions,
                                                             m_volInfo.vid);
      taskInjector.initRAO(raoDataConfig, &m_scheduler.getCatalogue());
    }
    bool noFilesToRecall = false;
    bool fetchResult = false;
    bool reservationResult = false;
    fetchResult = taskInjector.synchronousFetch(noFilesToRecall);
    if (fetchResult) {
      reservationResult = taskInjector.reserveSpaceForNextJobBatch(false);
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
      // This thread is now going to be idle until the system unwinds at the end
      // of the session
      // All client notifications are done by the report packer, including the
      // end of session
      taskInjector.waitThreads();
      threadPool.waitThreads();
      reportPacker.setDiskDone();
      readSingleThread.waitThreads();
      reportPacker.setTapeDone();
      reportPacker.waitThread();
      reporter.waitThreads();
      watchDog.stopAndWaitThread();
      return readSingleThread.getHardwareStatus();
    }
    else {
      // If the first pop from the queue fails, just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "success";
      if (!fetchResult || !reservationResult) {
        // If this is an empty mount because no files have been popped from the queue
        // or because disk reservation failed, it is just a warning
        priority = cta::log::WARNING;
        status = "failure";
      }

      logContext.log(priority, "Aborting recall mount startup: empty mount");

      std::string mountId = retrieveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toString(retrieveMount->getMountType());

      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty recall mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType", mountType);
      cta::log::Param statusParam("status", status);

      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        retrieveMount->abort("Aborted: empty recall mount");
        watchDog.updateStats(TapeSessionStats());
        watchDog.reportStats();
        std::list<cta::log::Param> paramList{errorMessageParam, mountIdParam, mountTypeParam, statusParam};
        m_initialProcess.addLogParams(m_driveConfig.unitName, paramList);
        cta::log::LogContext::ScopedParam sp08(logContext, cta::log::Param("MountTransactionId", mountId));
        cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("errorMessage", "Aborted: empty recall mount"));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        cta::log::LogContext::ScopedParam sp12(logContext, cta::log::Param("notificationError", ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware is OK
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
                                                                    TapeServerReporter& reporter) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig, logContext, archiveMount));
  if (!drive) return MARK_DRIVE_AS_DOWN;
  // Once we got hold of the drive, we can run the session
  {
    //dereferencing configLine is safe, because if configLine were not valid,
    //then findDrive would have return nullptr and we would have not end up there
    MigrationMemoryManager memoryManager(m_castorConf.nbBufs,
                                         m_castorConf.bufsz, logContext);
    MigrationReportPacker reportPacker(archiveMount, logContext);
    MigrationWatchDog watchDog(15, 60 * 10, m_initialProcess, *archiveMount, m_driveConfig.unitName, logContext);
    TapeWriteSingleThread writeSingleThread(*drive,
                                            m_mediaChanger,
                                            reporter,
                                            watchDog,
                                            m_volInfo,
                                            logContext,
                                            reportPacker,
                                            m_capUtils,
                                            m_castorConf.maxFilesBeforeFlush,
                                            m_castorConf.maxBytesBeforeFlush,
                                            m_castorConf.useLbp,
                                            m_castorConf.useEncryption,
                                            m_castorConf.externalEncryptionKeyScript,
                                            *archiveMount,
                                            m_castorConf.tapeLoadTimeout);


    DiskReadThreadPool threadPool(m_castorConf.nbDiskThreads,
                                  m_castorConf.bulkRequestMigrationMaxFiles,
                                  m_castorConf.bulkRequestMigrationMaxBytes,
                                  watchDog,
                                  logContext,
                                  m_castorConf.xrootPrivateKey,
                                  m_castorConf.xrootTimeout);
    MigrationTaskInjector taskInjector(memoryManager, threadPool, writeSingleThread, *archiveMount,
                                       m_castorConf.bulkRequestMigrationMaxFiles,
                                       m_castorConf.bulkRequestMigrationMaxBytes, logContext);
    threadPool.setTaskInjector(&taskInjector);
    writeSingleThread.setTaskInjector(&taskInjector);
    reportPacker.setWatchdog(watchDog);
    cta::utils::Timer timer;

    bool noFilesToMigrate = false;
    if (taskInjector.synchronousInjection(noFilesToMigrate)) {
      const uint64_t firstFseqFromClient = taskInjector.firstFseqToWrite();

      //the last fseq written on the tape is the first file's fseq minus one
      writeSingleThread.setlastFseq(firstFseqFromClient - 1);

      //we retrieved the detail from the client in execute, so at this point
      //we can report we will mount the tape.
      // TODO: create a "StartingSession" state as the mounting will happen in
      // the to-be-created tape thread.
      try {
        reporter.reportState(cta::tape::session::SessionState::Mounting,
                               cta::tape::session::SessionType::Archive);
      } catch (cta::exception::Exception& e) {
        cta::log::LogContext::ScopedParam sp1(logContext, cta::log::Param("errorMessage", e.getMessage().str()));
        logContext.log(cta::log::INFO, "Aborting the session after problem with mount details. Notifying the client.");
        reportPacker.synchronousReportEndWithErrors(e.getMessageValue(), 666, logContext);
        return MARK_DRIVE_AS_UP;
      } catch (...) {
        logContext.log(cta::log::INFO, "Aborting the session after problem with mount details (unknown exception). Notifying the client.");
        reportPacker.synchronousReportEndWithErrors("Unknown exception while checking session parameters with VMGR", 666, logContext);
        return MARK_DRIVE_AS_UP;
      }

      // We have something to do: start the session by starting all the
      // threads.
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
      reportPacker.waitThread();
      writeSingleThread.waitThreads();
      threadPool.waitThreads();
      memoryManager.waitThreads();
      reporter.waitThreads();
      watchDog.stopAndWaitThread();

      return writeSingleThread.getHardwareStatus();
    }
    else {
      // Just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "failure";
      if (noFilesToMigrate) {
        priority = cta::log::WARNING;
        status = "success";
      }
      logContext.log(priority, "Aborting migration mount startup: empty mount");

      std::string mountId = archiveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toString(archiveMount->getMountType());
      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty migration mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType", mountType);
      cta::log::Param statusParam("status", status);
      cta::log::LogContext::ScopedParam sp1(logContext, errorMessageParam);
      try {
        archiveMount->complete();
        watchDog.updateStats(TapeSessionStats());
        watchDog.reportStats();
        std::list<cta::log::Param> paramList{errorMessageParam, mountIdParam, mountTypeParam, statusParam};
        m_initialProcess.addLogParams(m_driveConfig.unitName, paramList);
        cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("MountTransactionId", mountId));
        logContext.log(priority, "Notified client of end session with error");
      } catch (cta::exception::Exception& ex) {
        cta::log::LogContext::ScopedParam sp12(logContext, cta::log::Param("notificationError", ex.getMessageValue()));
        logContext.log(cta::log::ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware safe
      return MARK_DRIVE_AS_UP;
    }
  }
}

//------------------------------------------------------------------------------
//DataTransferSession::executeLabel
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
castor::tape::tapeserver::daemon::DataTransferSession::executeLabel(cta::log::LogContext& logContext, cta::LabelMount *labelMount) {
  throw cta::exception::Exception("In DataTransferSession::executeLabel(): not implemented");
  // TODO
}


//------------------------------------------------------------------------------
//DataTransferSession::findDrive
//------------------------------------------------------------------------------
/*
 * Function synopsis  :
 *  1) Get hold of the drive and check it.
 *  --- Check If we did not find the drive in the tpConfig, we have a problem
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
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(const cta::tape::daemon::TpconfigLine& driveConfig, cta::log::LogContext& logContext,
                                                                 cta::TapeMount *mount) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(driveConfig.devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound& e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(logContext, cta::log::Param("devFilename", driveConfig.devFilename));
    logContext.log(cta::log::ERR, "Drive not found on this path");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Drive not found on this path";
    errMsg << headerErrMsg << logContext;
    mount->abort(headerErrMsg + e.getMessageValue());
    cta::log::LogContext::ScopedParam sp10(logContext, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(logContext, cta::log::Param("errorMessage", errMsg.str()));
    logContext.log(cta::log::ERR, "Notified client of end session with error");
    return nullptr;
  } catch (cta::exception::Exception& e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(logContext, cta::log::Param("devFilename", driveConfig.devFilename));
    cta::log::LogContext::ScopedParam sp10(logContext, cta::log::Param("errorMessage", e.getMessageValue()));
    logContext.log(cta::log::ERR, "Error looking to path to tape drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Error looking to path to tape drive: ";
    errMsg << headerErrMsg << logContext;
    mount->abort(headerErrMsg + e.getMessageValue());
    cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(logContext, cta::log::Param("errorMessage", errMsg.str()));
    logContext.log(cta::log::ERR, "Notified client of end session with error");
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(logContext, cta::log::Param("devFilename", driveConfig.devFilename));
    logContext.log(cta::log::ERR, "Unexpected exception while looking for drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Unexpected exception while looking for drive";
    errMsg << headerErrMsg << logContext;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp10(logContext, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(logContext, cta::log::Param("errorMessage", errMsg.str()));
    logContext.log(cta::log::ERR, "Notified client of end session with error");
    return nullptr;
  }
  try {
    std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive;
    drive.reset(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));
    if (drive) drive->config = driveConfig;
    return drive.release();
  } catch (cta::exception::Exception& e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(logContext, cta::log::Param("devFilename", driveConfig.devFilename));
    cta::log::LogContext::ScopedParam sp10(logContext, cta::log::Param("errorMessage", e.getMessageValue()));
    logContext.log(cta::log::ERR, "Error opening tape drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Error opening tape drive";
    errMsg << headerErrMsg << logContext;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp11(logContext, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(logContext, cta::log::Param("errorMessage", errMsg.str()));
    logContext.log(cta::log::ERR, "Notified client of end session with error");
    return nullptr;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(logContext, cta::log::Param("devFilename", driveConfig.devFilename));
    logContext.log(cta::log::ERR, "Unexpected exception while opening drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Unexpected exception while opening drive";
    errMsg << headerErrMsg << logContext;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp10(logContext, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(logContext, cta::log::Param("errorMessage", errMsg.str()));
    logContext.log(cta::log::ERR, "Notified client of end session with error");
    return nullptr;
  }
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
