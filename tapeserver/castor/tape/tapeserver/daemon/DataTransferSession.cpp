/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "castor/tape/tapeserver/RAO/RAOParams.hpp"

#include <google/protobuf/stubs/common.h>
#include <memory>
#include <string>

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(
    const std::string & hostname,
    cta::log::Logger & log,
    System::virtualWrapper & sysWrapper,
    const cta::tape::daemon::TpconfigLine & driveConfig,
    cta::mediachanger::MediaChangerFacade & mc,
    cta::tape::daemon::TapedProxy & initialProcess,
    cta::server::ProcessCap & capUtils,
    const DataTransferConfig & castorConf,
    cta::Scheduler & scheduler):
    m_log(log),
    m_sysWrapper(sysWrapper),
    m_driveConfig(driveConfig),
    m_castorConf(castorConf),
    m_driveInfo({driveConfig.unitName, cta::utils::getShortHostname(), driveConfig.logicalLibrary}),
    m_mc(mc),
    m_intialProcess(initialProcess),
    m_capUtils(capUtils),
    m_scheduler(scheduler)
    {
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
/**
 * This function will try to set the cap_sys_rawio capability that is needed
 * for by tape thread to access /dev/nst
 */
void castor::tape::tapeserver::daemon::DataTransferSession::setProcessCapabilities(
  const std::string &capabilities){
  cta::log::LogContext lc(m_log);
  try {
    m_capUtils.setProcText(capabilities);
    cta::log::LogContext::ScopedParam sp(lc,
      cta::log::Param("capabilities", m_capUtils.getProcText()));
    lc.log(cta::log::INFO, "Set process capabilities for using tape");
  } catch(const cta::exception::Exception &ne) {
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

  // 2a) Determine if we want to mount at all (for now)
  // This variable will allow us to see if we switched from down to up and start a
  // empty drive probe session if so.
  bool downUpTransition = false;
schedule:
  while (true) {
    try {
      auto desiredState = m_scheduler.getDesiredDriveState(m_driveConfig.unitName, lc);
      if (!desiredState.up) {
        downUpTransition  = true;
        // We wait a bit before polling the scheduler again.
        // TODO: parametrize the duration?
        m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
        sleep (5);
      } else {
        break;
      }
    } catch (cta::Scheduler::NoSuchDrive & e) {
      // The object store does not even know about this drive. We will report our state
      // (default status is down).
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    }
  }
  // If we get here after seeing a down desired state, we are transitioning  from
  // down to up. In such a case, we will run an empty
  if (downUpTransition) {
    downUpTransition = false;
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Probing, lc);
    castor::tape::tapeserver::daemon::EmptyDriveProbe emptyDriveProbe(m_log, m_driveConfig, m_sysWrapper);
    lc.log(cta::log::INFO, "Transition from down to up detected. Will check if a tape is in the drive.");
    if (!emptyDriveProbe.driveIsEmpty()) {
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
      cta::common::dataStructures::SecurityIdentity securityIdentity;
      cta::common::dataStructures::DesiredDriveState driveState;
      driveState.up = false;
      driveState.forceDown = false;
      std::string errorMsg = "A tape was detected in the drive. Putting the drive down.";
      cta::optional<std::string> probeErrorMsg = emptyDriveProbe.getProbeErrorMsg();
      if(probeErrorMsg)
        errorMsg = probeErrorMsg.value();
      int logLevel = cta::log::ERR;
      driveState.setReasonFromLogMsg(logLevel,errorMsg);
      m_scheduler.setDesiredDriveState(securityIdentity, m_driveConfig.unitName, driveState, lc);
      lc.log(logLevel, errorMsg);
      goto schedule;
    } else {
      lc.log(cta::log::INFO, "No tape detected in the drive. Proceeding with scheduling.");
    }
  }
  // 2b) Get initial mount information
  std::unique_ptr<cta::TapeMount> tapeMount;
  // As getting next mount could be long, we report the drive as up immediately.
  m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
  try {
    if (m_scheduler.getNextMountDryRun(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc))
      tapeMount.reset(m_scheduler.getNextMount(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc).release());
  } catch (cta::exception::Exception & e) {
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
    driveState.setReasonFromLogMsg(cta::log::ERR,exceptionMsg);
    m_scheduler.setDesiredDriveState(cliId, m_driveConfig.unitName, driveState, lc);
    return MARK_DRIVE_AS_DOWN;
  }
  // No mount to be done found, that was fast...
  if (!tapeMount.get()) {
    lc.log(cta::log::DEBUG, "No new mount found. (sleeping 10 seconds)");
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    sleep (10);
    goto schedule;
    // return MARK_DRIVE_AS_UP;
  }
  m_volInfo.vid=tapeMount->getVid();
  m_volInfo.mountType=tapeMount->getMountType();
  m_volInfo.nbFiles=tapeMount->getNbFiles();
  m_volInfo.mountId=tapeMount->getMountTransactionId();
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("tapeVid", m_volInfo.vid)
        .add("mountId", tapeMount->getMountTransactionId());
  {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", tapeMount->getMountTransactionId())
               .add("mountType", mountTypeToString(m_volInfo.mountType));
    lc.log(cta::log::INFO, "Got volume from client");
  }

  // Depending on the type of session, branch into the right execution
  switch(m_volInfo.mountType) {
  case cta::common::dataStructures::MountType::Retrieve:
    return executeRead(lc, dynamic_cast<cta::RetrieveMount *>(tapeMount.get()));
  case cta::common::dataStructures::MountType::ArchiveForUser:
  case cta::common::dataStructures::MountType::ArchiveForRepack:
    return executeWrite(lc, dynamic_cast<cta::ArchiveMount *>(tapeMount.get()));


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
 castor::tape::tapeserver::daemon::DataTransferSession::executeRead(cta::log::LogContext & lc, cta::RetrieveMount *retrieveMount) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A NULL pointer is returned on failure
  retrieveMount->setFetchEosFreeSpaceScript(m_castorConf.fetchEosFreeSpaceScript);
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig,lc,retrieveMount));
  if(!drive.get()) return MARK_DRIVE_AS_DOWN;
  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker rrp(retrieveMount, lc);
    rrp.disableBulk(); //no bulk needed anymore
    RecallWatchDog rwd(15,60*10,m_intialProcess,*retrieveMount,m_driveConfig.unitName,lc);

    RecallMemoryManager mm(m_castorConf.nbBufs, m_castorConf.bufsz,lc);
    TapeServerReporter tsr(m_intialProcess, m_driveConfig,
            m_hostname, m_volInfo, lc);
    //we retrieved the detail from the client in execute, so at this point
    //we can already report !
    tsr.reportState(cta::tape::session::SessionState::Mounting,
      cta::tape::session::SessionType::Retrieve);

    TapeReadSingleThread trst(*drive, m_mc, tsr, m_volInfo,
        m_castorConf.bulkRequestRecallMaxFiles,m_capUtils,rwd,lc,rrp,
        m_castorConf.useLbp, m_castorConf.useRAO, m_castorConf.useEncryption, m_castorConf.externalEncryptionKeyScript,*retrieveMount, m_castorConf.tapeLoadTimeout);
    DiskWriteThreadPool dwtp(m_castorConf.nbDiskThreads,
        rrp,
        rwd,
        lc,
        m_castorConf.xrootPrivateKey,
        m_castorConf.xrootTimeout);
    RecallTaskInjector rti(mm, trst, dwtp, *retrieveMount,
            m_castorConf.bulkRequestRecallMaxFiles,
            m_castorConf.bulkRequestRecallMaxBytes,lc);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type
    // of the client
    trst.setTaskInjector(&rti);
    rrp.setWatchdog(rwd);

    rti.setDriveInterface(trst.getDriveReference());

    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    cta::utils::Timer timer;

    // The RecallTaskInjector and the TapeReadSingleThread share the promise
    if (m_castorConf.useRAO) {
      castor::tape::tapeserver::rao::RAOParams raoDataConfig(m_castorConf.useRAO,m_castorConf.raoLtoAlgorithm, m_castorConf.raoLtoAlgorithmOptions,m_volInfo.vid);
      rti.initRAO(raoDataConfig, &m_scheduler.getCatalogue());
    }
    bool noFilesToRecall = false;
    bool fetchResult = false;
    bool reservationResult = false;
    fetchResult = rti.synchronousFetch(noFilesToRecall);
    if (fetchResult) {
    reservationResult = rti.reserveSpaceForNextJobBatch();
    }
    //only mount the tape if we can confirm that we will do some work, otherwise do an empty mount
    if (fetchResult && reservationResult) { 
      // We got something to recall. Time to start the machinery
      trst.setWaitForInstructionsTime(timer.secs());
      rwd.startThread();
      trst.startThreads();
      dwtp.startThreads();
      rrp.startThreads();
      rti.startThreads();
      tsr.startThreads();
      // This thread is now going to be idle until the system unwinds at the end
      // of the session
      // All client notifications are done by the report packer, including the
      // end of session
      rti.waitThreads();
      dwtp.waitThreads();
      rrp.setDiskDone();
      trst.waitThreads();
      rrp.setTapeDone();
      rrp.waitThread();
      tsr.waitThreads();
      rwd.stopAndWaitThread();
      return trst.getHardwareStatus();
    } else {
      // If the first pop from the queue fails, just log this was an empty mount and that's it.
      // The memory management will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "success";
      if(!fetchResult || !reservationResult){
        //If this is an empty mount because no files have been popped from the queue 
        //or because disk reservation failed, it is just a warning
        priority = cta::log::WARNING;
        status = "failure";
      }

      lc.log(priority, "Aborting recall mount startup: empty mount");

      std::string mountId = retrieveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toString(retrieveMount->getMountType());

      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty recall mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType", mountType);
      cta::log::Param statusParam("status",status);

      cta::log::LogContext::ScopedParam sp1(lc, errorMessageParam);
      try {
        retrieveMount->abort("Aborted: empty recall mount");
        rwd.updateStats(TapeSessionStats());
        rwd.reportStats();
        std::list<cta::log::Param> paramList { errorMessageParam, mountIdParam, mountTypeParam, statusParam };
        m_intialProcess.addLogParams(m_driveConfig.unitName,paramList);
        cta::log::LogContext::ScopedParam sp08(lc, cta::log::Param("MountTransactionId", mountId));
        cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("errorMessage", "Aborted: empty recall mount"));
        lc.log(priority, "Notified client of end session with error");
      } catch(cta::exception::Exception & ex) {
        cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("notificationError", ex.getMessageValue()));
        lc.log(cta::log::ERR, "Failed to notified client of end session with error");
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
  castor::tape::tapeserver::daemon::DataTransferSession::executeWrite(
  cta::log::LogContext & lc, cta::ArchiveMount *archiveMount) {
  // We are ready to start the session. We need to create the whole machinery
  // in order to get the task injector ready to check if we actually have a
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig,lc,archiveMount));
  if (!drive.get()) return MARK_DRIVE_AS_DOWN;
  // Once we got hold of the drive, we can run the session
  {
    //dereferencing configLine is safe, because if configLine were not valid,
    //then findDrive would have return NULL and we would have not end up there
    TapeServerReporter tsr(m_intialProcess, m_driveConfig, m_hostname,m_volInfo,lc);

    MigrationMemoryManager mm(m_castorConf.nbBufs,
        m_castorConf.bufsz,lc);
    MigrationReportPacker mrp(archiveMount, lc);
    MigrationWatchDog mwd(15,60*10,m_intialProcess,*archiveMount,m_driveConfig.unitName,lc);
    TapeWriteSingleThread twst(*drive,
        m_mc,
        tsr,
        mwd,
        m_volInfo,
        lc,
        mrp,
        m_capUtils,
        m_castorConf.maxFilesBeforeFlush,
        m_castorConf.maxBytesBeforeFlush,
        m_castorConf.useLbp,
        m_castorConf.useEncryption,
        m_castorConf.externalEncryptionKeyScript,
        *archiveMount,
        m_castorConf.tapeLoadTimeout);

    DiskReadThreadPool drtp(m_castorConf.nbDiskThreads,
        m_castorConf.bulkRequestMigrationMaxFiles,
        m_castorConf.bulkRequestMigrationMaxBytes,
        mwd,
        lc,
        m_castorConf.xrootPrivateKey,
        m_castorConf.xrootTimeout);
    MigrationTaskInjector mti(mm, drtp, twst, *archiveMount,
            m_castorConf.bulkRequestMigrationMaxFiles,
            m_castorConf.bulkRequestMigrationMaxBytes,lc);
    drtp.setTaskInjector(&mti);
    twst.setTaskInjector(&mti);
    mrp.setWatchdog(mwd);
    cta::utils::Timer timer;

    bool noFilesToMigrate = false;
    if (mti.synchronousInjection(noFilesToMigrate)) {
      const uint64_t firstFseqFromClient = mti.firstFseqToWrite();

      //the last fseq written on the tape is the first file's fseq minus one
      twst.setlastFseq(firstFseqFromClient-1);

      //we retrieved the detail from the client in execute, so at this point
      //we can report we will mount the tape.
      // TODO: create a "StartingSession" state as the mounting will happen in
      // the to-be-created tape thread.
      try {
        tsr.reportState(cta::tape::session::SessionState::Mounting,
          cta::tape::session::SessionType::Archive);
      } catch (cta::exception::Exception & e) {
        cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("errorMessage", e.getMessage().str()));
        lc.log(cta::log::INFO, "Aborting the session after problem with mount details. Notifying the client.");
        mrp.synchronousReportEndWithErrors(e.getMessageValue(), 666, lc);
        return MARK_DRIVE_AS_UP;
      } catch (...) {
        lc.log(cta::log::INFO, "Aborting the session after problem with mount details (unknown exception). Notifying the client.");
        mrp.synchronousReportEndWithErrors("Unknown exception while checking session parameters with VMGR", 666, lc);
        return MARK_DRIVE_AS_UP;
      }

      // We have something to do: start the session by starting all the
      // threads.
      mm.startThreads();
      drtp.startThreads();
      mwd.startThread();
      twst.setWaitForInstructionsTime(timer.secs());
      twst.startThreads();
      mrp.startThreads();
      mti.startThreads();
      tsr.startThreads();

      // Synchronise with end of threads
      mti.waitThreads();
      mrp.waitThread();
      twst.waitThreads();
      drtp.waitThreads();
      mm.waitThreads();
      tsr.waitThreads();
      mwd.stopAndWaitThread();

      return twst.getHardwareStatus();
    } else {
      // Just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      int priority = cta::log::ERR;
      std::string status = "failure";
      if(noFilesToMigrate){
        priority = cta::log::WARNING;
        status = "success";
      }
      lc.log(priority, "Aborting migration mount startup: empty mount");

      std::string mountId = archiveMount->getMountTransactionId();
      std::string mountType = cta::common::dataStructures::toString(archiveMount->getMountType());
      cta::log::Param errorMessageParam("errorMessage", "Aborted: empty migration mount");
      cta::log::Param mountIdParam("mountId", mountId);
      cta::log::Param mountTypeParam("mountType",mountType);
      cta::log::Param statusParam("status", status);
      cta::log::LogContext::ScopedParam sp1(lc, errorMessageParam);
      try {
        archiveMount->complete();
        mwd.updateStats(TapeSessionStats());
        mwd.reportStats();
        std::list<cta::log::Param> paramList { errorMessageParam, mountIdParam, mountTypeParam, statusParam };
        m_intialProcess.addLogParams(m_driveConfig.unitName,paramList);
        cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("MountTransactionId", mountId));
        lc.log(priority, "Notified client of end session with error");
      } catch(cta::exception::Exception & ex) {
        cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("notificationError", ex.getMessageValue()));
        lc.log(cta::log::ERR, "Failed to notified client of end session with error");
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
  castor::tape::tapeserver::daemon::DataTransferSession::executeLabel(cta::log::LogContext& lc, cta::LabelMount* labelMount) {
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
 * @param lc For logging purpose
 * @return the drive if found, NULL otherwise
 */
castor::tape::tapeserver::drive::DriveInterface *
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(const cta::tape::daemon::TpconfigLine
  &driveConfig, cta::log::LogContext&  lc, cta::TapeMount *mount) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(driveConfig.devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    lc.log(cta::log::ERR, "Drive not found on this path");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Drive not found on this path";
    errMsg << headerErrMsg << lc;
    mount->abort(headerErrMsg + e.getMessageValue());
    cta::log::LogContext::ScopedParam sp10(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  } catch (cta::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    cta::log::LogContext::ScopedParam sp10(lc, cta::log::Param("errorMessage", e.getMessageValue()));
    lc.log(cta::log::ERR, "Error looking to path to tape drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Error looking to path to tape drive: ";
    errMsg << headerErrMsg << lc;
    mount->abort(headerErrMsg + e.getMessageValue());
    cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    lc.log(cta::log::ERR, "Unexpected exception while looking for drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Unexpected exception while looking for drive";
    errMsg << headerErrMsg << lc;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp10(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  }
  try {
    std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive;
    drive.reset(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));
    if (drive.get()) drive->config = driveConfig;
    return drive.release();
  } catch (cta::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    cta::log::LogContext::ScopedParam sp10(lc, cta::log::Param("errorMessage", e.getMessageValue()));
    lc.log(cta::log::ERR, "Error opening tape drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Error opening tape drive";
    errMsg << headerErrMsg << lc;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    lc.log(cta::log::ERR, "Unexpected exception while opening drive");

    std::stringstream errMsg;
    const std::string headerErrMsg = "Unexpected exception while opening drive";
    errMsg << headerErrMsg << lc;
    mount->abort(headerErrMsg);
    cta::log::LogContext::ScopedParam sp10(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp13(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::~DataTransferSession()
  throw () {
  try {
    google::protobuf::ShutdownProtobufLibrary();
  } catch(...) {
    m_log(cta::log::ERR, "google::protobuf::ShutdownProtobufLibrary() threw an"
      " unexpected exception");
  }
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DataTransferSession::
  mountTypeToString(const cta::common::dataStructures::MountType mountType) const throw() {
  switch(mountType) {
  case cta::common::dataStructures::MountType::Retrieve: return "Retrieve";
  case cta::common::dataStructures::MountType::ArchiveForUser : return "ArchiveForUser";
  case cta::common::dataStructures::MountType::ArchiveForRepack : return "ArchiveForRepack";
  case cta::common::dataStructures::MountType::Label: return "Label";
  default                      : return "UNKNOWN";
  }
}
