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
#include "castor/exception/Exception.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/System.hpp"
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
#include "log.h"
#include "serrno.h"
#include "stager_client_commandline.h"
#include "scheduler/RetrieveMount.hpp"

#include <google/protobuf/stubs/common.h>
#include <memory>

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(
    const std::string & hostname,
    castor::log::Logger & log,
    System::virtualWrapper & sysWrapper,
    const DriveConfig & driveConfig,
    castor::mediachanger::MediaChangerFacade & mc,
    castor::messages::TapeserverProxy & initialProcess,
    castor::server::ProcessCap & capUtils,
    const DataTransferConfig & castorConf,
    cta::Scheduler & scheduler): 
    m_log(log),
    m_sysWrapper(sysWrapper),
    m_driveConfig(driveConfig),
    m_castorConf(castorConf), 
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
  log::LogContext lc(m_log);
  // Create a sticky thread name, which will be overridden by the other threads
  lc.pushOrReplace(log::Param("thread", "MainThread"));
  // 2a) Get initial information from the client
  std::unique_ptr<cta::TapeMount> tapeMount;
  try {
    tapeMount.reset(m_scheduler.getNextMount(m_driveConfig.getLogicalLibrary(), m_driveConfig.getUnitName()).release());
  } catch (cta::exception::Exception & e) {
    log::ScopedParamContainer localParams(lc);
    localParams.add("errorMessage", e.getMessageValue());
    lc.log(LOG_ERR, "Error while scheduling new mount. Putting the drive down.");
    return MARK_DRIVE_AS_DOWN;
  }
  // No mount to be done found, that was fast...
  if (!tapeMount.get()) {
    lc.log(LOG_INFO, "No new mount found!");
    return MARK_DRIVE_AS_UP;
  }
  m_volInfo.vid=tapeMount->getVid();
  m_volInfo.mountType=tapeMount->getMountType();
  m_volInfo.nbFiles=tapeMount->getNbFiles();
  // 2b) ... and log.
  // Make the DGN and TPVID parameter permanent.
  log::ScopedParamContainer params(lc);
  params.add("TPVID", m_volInfo.vid);
  {
    log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", tapeMount->getMountTransactionId())
               .add("mountType", mountTypeToString(m_volInfo.mountType));
    lc.log(LOG_INFO, "Got volume from client");
  }
  
  // Depending on the type of session, branch into the right execution
  switch(m_volInfo.mountType) {
  case cta::MountType::RETRIEVE:
    return executeRead(lc, dynamic_cast<cta::RetrieveMount *>(tapeMount.get()));
  case cta::MountType::ARCHIVE:
    return executeWrite(lc, dynamic_cast<cta::ArchiveMount *>(tapeMount.get()));
  default:
    return MARK_DRIVE_AS_UP;
  }
}
//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
 castor::tape::tapeserver::daemon::DataTransferSession::executeRead(log::LogContext & lc, cta::RetrieveMount *retrieveMount) {
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
    RecallWatchDog rwd(15,60*10,m_intialProcess,m_driveConfig.getUnitName(),lc);
    
    RecallMemoryManager mm(m_castorConf.nbBufs, m_castorConf.bufsz,lc);
    TapeServerReporter tsr(m_intialProcess, m_driveConfig, 
            m_hostname, m_volInfo, lc);
    //we retrieved the detail from the client in execute, so at this point 
    //we can already report !
    tsr.gotReadMountDetailsFromClient();
    
    TapeReadSingleThread trst(*drive, m_mc, tsr, m_volInfo, 
        m_castorConf.bulkRequestRecallMaxFiles,m_capUtils,rwd,lc);

    DiskWriteThreadPool dwtp(m_castorConf.nbDiskThreads,
        rrp,
        rwd,
        lc,
        m_castorConf.remoteFileProtocol,
        m_castorConf.xrootPrivateKey,
        m_castorConf.moverHandlerPort);
    RecallTaskInjector rti(mm, trst, dwtp, *retrieveMount,
            m_castorConf.bulkRequestRecallMaxFiles,
            m_castorConf.bulkRequestRecallMaxBytes,lc);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type
    // of the client
    trst.setTaskInjector(&rti);
    rrp.setWatchdog(rwd);
    
    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    castor::utils::Timer timer;
    if (rti.synchronousInjection()) {  //adapt the recall task injector (starting from synchronousInjection)
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
      rrp.waitThread();
      dwtp.waitThreads();
      trst.waitThreads();
      tsr.waitThreads();
      rwd.stopAndWaitThread();
      return trst.getHardwareStatus();
    } else {
      // Just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      lc.log(LOG_ERR, "Aborting recall mount startup: empty mount");
      log::LogContext::ScopedParam sp1(lc, log::Param("errorMessage", "Aborted: empty recall mount"));
      log::LogContext::ScopedParam sp2(lc, log::Param("errorCode", SEINTERNAL));
      try {
        retrieveMount->abort();
        log::LogContext::ScopedParam sp08(lc, log::Param("MountTransactionId", retrieveMount->getMountTransactionId()));
        log::LogContext::ScopedParam sp11(lc, log::Param("errorMessage", "Aborted: empty recall mount"));
        log::LogContext::ScopedParam sp12(lc, log::Param("errorCode", SEINTERNAL));
        lc.log(LOG_ERR, "Notified client of end session with error");
      } catch(castor::exception::Exception & ex) {
        log::LogContext::ScopedParam sp1(lc, log::Param("notificationError", ex.getMessageValue()));
        lc.log(LOG_ERR, "Failed to notified client of end session with error");
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
  log::LogContext & lc, cta::ArchiveMount *archiveMount) {
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
    MigrationWatchDog mwd(15,60*10,m_intialProcess,m_driveConfig.getUnitName(),lc);
    TapeWriteSingleThread twst(*drive,
        m_mc,
        tsr,
        mwd,
        m_volInfo,
        lc,
        mrp,
        m_capUtils,    
        m_castorConf.maxFilesBeforeFlush,
        m_castorConf.maxBytesBeforeFlush);
    DiskReadThreadPool drtp(m_castorConf.nbDiskThreads,
        m_castorConf.bulkRequestMigrationMaxFiles,
        m_castorConf.bulkRequestMigrationMaxBytes,
        mwd,
        lc,
        m_castorConf.remoteFileProtocol,
        m_castorConf.xrootPrivateKey,
        m_castorConf.moverHandlerPort);
    MigrationTaskInjector mti(mm, drtp, twst, *archiveMount, 
            m_castorConf.bulkRequestMigrationMaxFiles,
            m_castorConf.bulkRequestMigrationMaxBytes,lc);
    drtp.setTaskInjector(&mti);
    twst.setTaskInjector(&mti);
    mrp.setWatchdog(mwd);
    castor::utils::Timer timer;

    if (mti.synchronousInjection()) {
      const uint64_t firstFseqFromClient = mti.firstFseqToWrite();
      
      //the last fseq written on the tape is the first file's fseq minus one
      twst.setlastFseq(firstFseqFromClient-1);
      
      //we retrieved the detail from the client in execute, so at this point 
      //we can report. We get in exchange the number of files on the tape.
      // This function can throw an exception (usually for permission reasons)
      // This is not a reason to put the drive down. The error is already logged
      // upstream.
      // Letting the exception slip through would leave the drive down.
      // Initialise with something obviously wrong.
      uint64_t nbOfFileOnTape = std::numeric_limits<uint64_t>::max();
      try {
        nbOfFileOnTape = tsr.gotWriteMountDetailsFromClient();
      } catch (castor::exception::Exception & e) {
        log::LogContext::ScopedParam sp1(lc, log::Param("errorMessage", e.getMessage().str()));
        lc.log(LOG_INFO, "Aborting the session after problem with mount details. Notifying the client.");
        mrp.synchronousReportEndWithErrors(e.getMessageValue(), SEINTERNAL);
        return MARK_DRIVE_AS_UP;
      } catch (...) {
        lc.log(LOG_INFO, "Aborting the session after problem with mount details (unknown exception). Notifying the client.");
        mrp.synchronousReportEndWithErrors("Unknown exception while checking session parameters with VMGR", SEINTERNAL);
        return MARK_DRIVE_AS_UP;
      }

      //theses 2 numbers should match. Otherwise, it means the stager went mad 
      if(firstFseqFromClient != nbOfFileOnTape + 1) {
        std::stringstream ss;
        ss << "First file to write's fseq(" << firstFseqFromClient << ")  and number of files on the tape (" << nbOfFileOnTape << " + 1) dont match";
        lc.log(LOG_ERR, ss.str());
       //no mount at all, drive to be kept up = return 0
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
      lc.log(LOG_ERR, "Aborting migration mount startup: empty mount");
      log::LogContext::ScopedParam sp1(lc, log::Param("errorMessage", "Aborted: empty migration mount"));
      log::LogContext::ScopedParam sp2(lc, log::Param("errorCode", SEINTERNAL));
      try {
        archiveMount->complete();
        log::LogContext::ScopedParam sp1(lc, log::Param("MountTransactionId", archiveMount->getMountTransactionId()));
        lc.log(LOG_ERR, "Notified client of end session with error");
      } catch(castor::exception::Exception & ex) {
        log::LogContext::ScopedParam sp1(lc, log::Param("notificationError", ex.getMessageValue()));
        lc.log(LOG_ERR, "Failed to notified client of end session with error");
      }
      // Empty mount, hardware safe
      return MARK_DRIVE_AS_UP;
    }
  }
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
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(const DriveConfig
  &driveConfig, log::LogContext& lc, cta::TapeMount *mount) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(driveConfig.getDevFilename());
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Drive not found on this path");
    
    std::stringstream errMsg;
    errMsg << "Drive not found on this path" << lc;
    mount->abort();
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    log::LogContext::ScopedParam sp13(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    log::LogContext::ScopedParam sp10(lc, log::Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error looking to path to tape drive");
    
    std::stringstream errMsg;
    errMsg << "Error looking to path to tape drive: " << lc;
    mount->abort();
    log::LogContext::ScopedParam sp11(lc, log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp15(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Unexpected exception while looking for drive");
    
    std::stringstream errMsg;
    errMsg << "Unexpected exception while looking for drive" << lc;
    mount->abort();
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    log::LogContext::ScopedParam sp13(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  }
  try {
    std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive;
    drive.reset(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));
    if (drive.get()) drive->config = driveConfig;
    return drive.release();
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    log::LogContext::ScopedParam sp10(lc, log::Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error opening tape drive");
    
    std::stringstream errMsg;
    errMsg << "Error opening tape drive" << lc;
    mount->abort();
    log::LogContext::ScopedParam sp11(lc, log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp15(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Unexpected exception while opening drive");
    
    std::stringstream errMsg;
    errMsg << "Unexpected exception while opening drive" << lc;
    mount->abort();
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", mount->getMountTransactionId()));
    log::LogContext::ScopedParam sp13(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
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
    m_log(LOG_ERR, "google::protobuf::ShutdownProtobufLibrary() threw an"
      " unexpected exception");
  }
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DataTransferSession::
  mountTypeToString(const cta::MountType::Enum mountType) const throw() {
  switch(mountType) {
  case cta::MountType::RETRIEVE: return "RETRIEVE";
  case cta::MountType::ARCHIVE : return "ARCHIVE";
  default                      : return "UNKNOWN";
  }
}
