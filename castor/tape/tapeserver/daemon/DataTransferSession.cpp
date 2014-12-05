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
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "h/log.h"
#include "h/serrno.h"
#include "h/stager_client_commandline.h"

#include <google/protobuf/stubs/common.h>
#include <memory>

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(
    const std::string & hostname,
    const legacymsg::RtcpJobRqstMsgBody & clientRequest, 
    castor::log::Logger & log,
    System::virtualWrapper & sysWrapper,
    const DriveConfig & driveConfig,
    castor::mediachanger::MediaChangerFacade & mc,
    castor::messages::TapeserverProxy & initialProcess,
    castor::server::ProcessCap & capUtils,
    const DataTransferConfig & castorConf): 
    m_request(clientRequest),
    m_log(log),
    m_clientProxy(clientRequest),
    m_sysWrapper(sysWrapper),
    m_driveConfig(driveConfig),
    m_castorConf(castorConf), 
    m_mc(mc),
    m_intialProcess(initialProcess),
    m_capUtils(capUtils) {
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
  log::LogContext::ScopedParam sp01(lc, log::Param("clientHost", m_request.clientHost));
  log::LogContext::ScopedParam sp02(lc, log::Param("clientPort", m_request.clientPort));
  log::LogContext::ScopedParam sp03(lc, log::Param("mountTransactionId", m_request.volReqId));
  log::LogContext::ScopedParam sp04(lc, log::Param("volReqId", m_request.volReqId));
  log::LogContext::ScopedParam sp05(lc, log::Param("driveUnit", m_request.driveUnit));
  log::LogContext::ScopedParam sp06(lc, log::Param("dgn", m_request.dgn));
  // 2a) Get initial information from the client
  client::ClientProxy::RequestReport reqReport;
  try {
    m_clientProxy.fetchVolumeId(m_volInfo, reqReport);
  } catch(client::ClientProxy::EndOfSessionWithError & eoswe) {
    std::stringstream fullError;
    fullError << "Received end of session with error from client when requesting Volume "
      << eoswe.getMessageValue();
    lc.log(LOG_ERR, fullError.str());
    m_clientProxy.reportEndOfSession(reqReport);
    log::ScopedParamContainer params(lc);
    params.add("tapebridgeTransId", reqReport.transactionId)
          .add("connectDuration", reqReport.connectDuration)
          .add("sendRecvDuration", reqReport.sendRecvDuration);
    lc.log(LOG_INFO, "Acknowledged client of end session with error");
    return MARK_DRIVE_AS_UP;    
  } catch(client::ClientProxy::EndOfSession & eos) {
    lc.log(LOG_INFO, "Received end of session from client when requesting Volume");
    m_clientProxy.reportEndOfSession(reqReport);
    log::ScopedParamContainer params(lc);
    params.add("tapebridgeTransId", reqReport.transactionId)
          .add("connectDuration", reqReport.connectDuration)
          .add("sendRecvDuration", reqReport.sendRecvDuration);
    lc.log(LOG_INFO, "Acknowledged client of end session");
    return MARK_DRIVE_AS_UP;  
  } catch (client::ClientProxy::UnexpectedResponse & unexp) {
    std::stringstream fullError;
    fullError << "Received unexpected response from client when requesting Volume"
      << unexp.getMessageValue();
    lc.log(LOG_ERR, fullError.str());
    m_clientProxy.reportEndOfSession(reqReport);
    log::LogContext::ScopedParam sp07(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp08(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp09(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp10(lc, log::Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return MARK_DRIVE_AS_UP;
  } catch (castor::exception::Exception & ex) {
    std::stringstream fullError;
    fullError << "When requesting Volume, "
      << ex.getMessageValue();
    lc.log(LOG_ERR, fullError.str());
    try {
      // Attempt to notify the client. This is so hopeless and likely to fail
      // that is does not deserve a log.
      m_clientProxy.reportEndOfSession(reqReport);
    } catch (...) {}
    log::LogContext::ScopedParam sp07(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp10(lc, log::Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Could not contact client for Volume (client notification was attempted).");
    return MARK_DRIVE_AS_UP;
  }
  // 2b) ... and log.
  // Make the DGN and TPVID parameter permanent.
  log::ScopedParamContainer params(lc);
  params.add("dgn", m_request.dgn)
        .add("TPVID", m_volInfo.vid);
  {
    log::ScopedParamContainer localParams(lc);
    localParams.add("tapebridgeTransId", reqReport.transactionId)
               .add("connectDuration", reqReport.connectDuration)
               .add("sendRecvDuration", reqReport.sendRecvDuration)
               .add("density", m_volInfo.density)
               .add("label", m_volInfo.labelObsolete)
               .add("clientType", volumeClientTypeToString(m_volInfo.clientType))
               .add("mode", volumeModeToString(m_volInfo.volumeMode));
    lc.log(LOG_INFO, "Got volume from client");
  }
  
  // Depending on the type of session, branch into the right execution
  switch(m_volInfo.volumeMode) {
  case tapegateway::READ:
    return executeRead(lc);
  case tapegateway::WRITE:
    return executeWrite(lc);
  case tapegateway::DUMP:
    executeDump(lc);
    return MARK_DRIVE_AS_UP;
  default:
      return MARK_DRIVE_AS_UP;
  }
}
//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
 castor::tape::tapeserver::daemon::DataTransferSession::executeRead(log::LogContext & lc) {
  // We are ready to start the session. We need to create the whole machinery 
  // in order to get the task injector ready to check if we actually have a 
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A NULL pointer is returned on failure
  std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig,lc));
  
  if(!drive.get()) return MARK_DRIVE_AS_DOWN;    
  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker rrp(m_clientProxy,
        m_castorConf.bulkRequestMigrationMaxFiles,
        lc);

    // If we talk to a command line client, we do not batch report
    if (tapegateway::READ_TP == m_volInfo.clientType) {
      rrp.disableBulk();
    }
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
    RecallTaskInjector rti(mm, trst, dwtp, m_clientProxy,
            m_castorConf.bulkRequestRecallMaxFiles,
            m_castorConf.bulkRequestRecallMaxBytes,lc);
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // In order to implement the fix, the task injector needs to know the type
    // of the client
    rti.setClientType(m_volInfo.clientType);
    trst.setTaskInjector(&rti);
    rrp.setWatchdog(rwd);
    
    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    castor::utils::Timer timer;
    if (rti.synchronousInjection()) {
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
        client::ClientProxy::RequestReport reqReport;
        m_clientProxy.reportEndOfSessionWithError("Aborted: empty recall mount", SEINTERNAL, reqReport);
        log::LogContext::ScopedParam sp08(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
        log::LogContext::ScopedParam sp09(lc, log::Param("connectDuration", reqReport.connectDuration));
        log::LogContext::ScopedParam sp10(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
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
  log::LogContext & lc) {
  // We are ready to start the session. We need to create the whole machinery 
  // in order to get the task injector ready to check if we actually have a 
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(findDrive(m_driveConfig,lc));
  if (!drive.get()) return MARK_DRIVE_AS_UP;
  // Once we got hold of the drive, we can run the session
  {
    
    //dereferencing configLine is safe, because if configLine were not valid, 
    //then findDrive would have return NULL and we would have not end up there
    TapeServerReporter tsr(m_intialProcess, m_driveConfig, m_hostname,m_volInfo,lc);
    
    MigrationMemoryManager mm(m_castorConf.nbBufs,
        m_castorConf.bufsz,lc);
    MigrationReportPacker mrp(m_clientProxy, lc);
    MigrationWatchDog mwd(15,60*10,m_intialProcess,m_driveConfig.getUnitName(),lc);
    TapeWriteSingleThread twst(*drive.get(),
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
    MigrationTaskInjector mti(mm, drtp, twst, m_clientProxy, 
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
        lc.log(LOG_ERR, "First file to write's fseq  and number of files on "
        "the tape according to the VMGR dont match");
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
        client::ClientProxy::RequestReport reqReport;
        m_clientProxy.reportEndOfSessionWithError("Aborted: empty migration mount", SEINTERNAL, reqReport);
        log::LogContext::ScopedParam sp1(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
        log::LogContext::ScopedParam sp2(lc, log::Param("connectDuration", reqReport.connectDuration));
        log::LogContext::ScopedParam sp3(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
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
//DataTransferSession::executeDump
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DataTransferSession::executeDump(log::LogContext & lc) {
  // We are ready to start the session. In case of read there is no interest in
  // creating the machinery before getting the tape mounted, so do it now.
  // 1) Get hold of the drive and check it.
  
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
 * Try to find the drive that is described by m_request.driveUnit and m_volInfo.density
 * @param lc For logging purpose
 * @return the drive if found, NULL otherwise
 */
castor::tape::tapeserver::drive::DriveInterface *
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(const DriveConfig
  &driveConfig, log::LogContext& lc) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(driveConfig.getDevFilename());
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Drive not found on this path");
    
    client::ClientProxy::RequestReport reqReport;
    std::stringstream errMsg;
    errMsg << "Drive not found on this path" << lc;
    m_clientProxy.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp11(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp12(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp13(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    log::LogContext::ScopedParam sp10(lc, log::Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error looking to path to tape drive");
    
    client::ClientProxy::RequestReport reqReport;
    std::stringstream errMsg;
    errMsg << "Error looking to path to tape drive: " << lc;
    m_clientProxy.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    log::LogContext::ScopedParam sp11(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp12(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp13(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp15(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Unexpected exception while looking for drive");
    
    client::ClientProxy::RequestReport reqReport;
    std::stringstream errMsg;
    errMsg << "Unexpected exception while looking for drive" << lc;
    m_clientProxy.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp11(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp12(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp13(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  }
  try {
    std::auto_ptr<castor::tape::tapeserver::drive::DriveInterface> drive;
    drive.reset(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));
    if (drive.get()) drive->config = driveConfig;
    return drive.release();
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    log::LogContext::ScopedParam sp10(lc, log::Param("errorMessage", e.getMessageValue()));
    lc.log(LOG_ERR, "Error opening tape drive");
    
    client::ClientProxy::RequestReport reqReport;
    std::stringstream errMsg;
    errMsg << "Error opening tape drive" << lc;
    m_clientProxy.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    log::LogContext::ScopedParam sp11(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp12(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp13(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp14(lc, log::Param("errorMessage", errMsg.str()));
    log::LogContext::ScopedParam sp15(lc, log::Param("errorCode", SEINTERNAL));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return NULL;
  } catch (...) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.getDevFilename()));
    lc.log(LOG_ERR, "Unexpected exception while opening drive");
    
    client::ClientProxy::RequestReport reqReport;
    std::stringstream errMsg;
    errMsg << "Unexpected exception while opening drive" << lc;
    m_clientProxy.reportEndOfSessionWithError("Drive unit not found", SEINTERNAL, reqReport);
    log::LogContext::ScopedParam sp10(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp11(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp12(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
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
// volumeClientTypeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DataTransferSession::
  volumeClientTypeToString(const tapegateway::ClientType mode) const throw() {
  switch(mode) {
  case tapegateway::TAPE_GATEWAY: return "TAPE_GATEWAY";
  case tapegateway::READ_TP     : return "READ_TP";
  case tapegateway::WRITE_TP    : return "WRITE_TP";
  case tapegateway::DUMP_TP     : return "DUMP_TP";
  default                       : return "UKNOWN";
  }
}   

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::DataTransferSession::
  volumeModeToString(const tapegateway::VolumeMode mode) const throw() {
  switch(mode) {
  case tapegateway::READ : return "READ";
  case tapegateway::WRITE: return "WRITE";
  case tapegateway::DUMP : return "DUMP";
  default                : return "UKNOWN";
  }
}
