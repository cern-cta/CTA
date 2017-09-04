/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/common/CastorConfiguration.hpp"
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

#include <google/protobuf/stubs/common.h>
#include <memory>

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
  lc.pushOrReplace(cta::log::Param("unitName", m_driveConfig.unitName));
  
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
    castor::tape::tapeserver::daemon::EmptyDriveProbe emptyDriveProbe(m_log, m_driveConfig, m_sysWrapper);
    lc.log(cta::log::INFO, "Transition from down to up detected. Will check if a tape is in the drive.");
    if (!emptyDriveProbe.driveIsEmpty()) {
      m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
      cta::common::dataStructures::SecurityIdentity securityIdentity;
      m_scheduler.setDesiredDriveState(securityIdentity, m_driveConfig.unitName, false, false, lc);
      lc.log(cta::log::ERR, "A tape was detected in the drive. Putting the drive back down.");
      goto schedule;
    } else {
      lc.log(cta::log::INFO, "No tape detected in the drive. Proceeding with scheduling.");
    }
  }
  // 2b) Get initial mount information
  std::unique_ptr<cta::TapeMount> tapeMount;
  try {
    tapeMount.reset(m_scheduler.getNextMount(m_driveConfig.logicalLibrary, m_driveConfig.unitName, lc).release());
  } catch (cta::exception::Exception & e) {
    cta::log::ScopedParamContainer localParams(lc);
    localParams.add("errorMessage", e.getMessageValue());
    lc.log(cta::log::ERR, "Error while scheduling new mount. Putting the drive down. Stack trace follows.");
    lc.logBacktrace(cta::log::ERR, e.backtrace());
    m_scheduler.reportDriveStatus(m_driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    cta::common::dataStructures::SecurityIdentity cliId;
    m_scheduler.setDesiredDriveState(cliId, m_driveConfig.unitName, false, false, lc);
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
  // 2c) ... and log.
  // Make the DGN and TPVID parameter permanent.
  cta::log::ScopedParamContainer params(lc);
  params.add("vid", m_volInfo.vid)
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
  case cta::common::dataStructures::MountType::Archive:
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
        m_castorConf.useLbp, m_castorConf.useRAO, m_castorConf.externalEncryptionKeyScript);
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
      rti.initRAO();
    }

    if (rti.synchronousFetch()) {  //adapt the recall task injector (starting from synchronousFetch)
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
      // Just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      lc.log(cta::log::ERR, "Aborting recall mount startup: empty mount");
      cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("errorMessage", "Aborted: empty recall mount"));
      try {
        retrieveMount->abort();
        cta::log::LogContext::ScopedParam sp08(lc, cta::log::Param("MountTransactionId", retrieveMount->getMountTransactionId()));
        cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("errorMessage", "Aborted: empty recall mount"));
        lc.log(cta::log::ERR, "Notified client of end session with error");
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
  if (!drive.get()) return MARK_DRIVE_AS_UP;
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
        m_castorConf.externalEncryptionKeyScript);
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

    if (mti.synchronousInjection()) {
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
      lc.log(cta::log::ERR, "Aborting migration mount startup: empty mount");
      cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("errorMessage", "Aborted: empty migration mount"));
      try {
        archiveMount->complete();
        cta::log::LogContext::ScopedParam sp1(lc, cta::log::Param("MountTransactionId", archiveMount->getMountTransactionId()));
        lc.log(cta::log::ERR, "Notified client of end session with error");
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
    errMsg << "Drive not found on this path" << lc;
    mount->abort();
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
    errMsg << "Error looking to path to tape drive: " << lc;
    mount->abort();
    cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    lc.log(cta::log::ERR, "Unexpected exception while looking for drive");
    
    std::stringstream errMsg;
    errMsg << "Unexpected exception while looking for drive" << lc;
    mount->abort();
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
    errMsg << "Error opening tape drive" << lc;
    mount->abort();
    cta::log::LogContext::ScopedParam sp11(lc, cta::log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    cta::log::LogContext::ScopedParam sp14(lc, cta::log::Param("errorMessage", errMsg.str()));
    lc.log(cta::log::ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    cta::log::LogContext::ScopedParam sp09(lc, cta::log::Param("devFilename", driveConfig.devFilename));
    lc.log(cta::log::ERR, "Unexpected exception while opening drive");
    
    std::stringstream errMsg;
    errMsg << "Unexpected exception while opening drive" << lc;
    mount->abort();
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
  case cta::common::dataStructures::MountType::Archive : return "Archive";
  case cta::common::dataStructures::MountType::Label: return "Label";
  default                      : return "UNKNOWN";
  }
}
