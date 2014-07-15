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

#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "log.h"
#include "stager_client_commandline.h"
#include "castor/tape/utils/utils.hpp"
#include "castor/System.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "h/serrno.h"
#include <google/protobuf/stubs/common.h>
#include <memory>
#include <zmq.h>

//------------------------------------------------------------------------------
// m_zmqContext
//------------------------------------------------------------------------------
void *castor::tape::tapeserver::daemon::DataTransferSession::m_zmqContext =
  NULL;

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::DataTransferSession(
    const std::string & hostname,
    const legacymsg::RtcpJobRqstMsgBody & clientRequest, 
    castor::log::Logger& logger, System::virtualWrapper & sysWrapper,
    const utils::DriveConfig & driveConfig,
    castor::legacymsg::RmcProxy & rmc,
    castor::messages::TapeserverProxy & initialProcess,
    castor::server::ProcessCap &capUtils,
    const CastorConf & castorConf): 
    m_request(clientRequest),
    m_logger(logger),
    m_clientProxy(clientRequest),
    m_sysWrapper(sysWrapper),
    m_driveConfig(driveConfig),
    m_castorConf(castorConf), 
    m_rmc(rmc),
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
int castor::tape::tapeserver::daemon::DataTransferSession::execute()
 {
  // 1) Prepare the logging environment
  log::LogContext lc(m_logger);
  // Create a sticky thread name, which will be overridden by the other threads
  lc.pushOrReplace(log::Param("thread", "mainThread"));
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
  } catch(client::ClientProxy::EndOfSession & eof) {
    std::stringstream fullError;
    fullError << "Received end of session rom client when requesting Volume "
      << eof.getMessageValue();
    lc.log(LOG_ERR, fullError.str());
    m_clientProxy.reportEndOfSession(reqReport);
    log::LogContext::ScopedParam sp07(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp08(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp09(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp10(lc, log::Param("ErrorMsg", fullError.str()));
    lc.log(LOG_ERR, "Notified client of end session with error");
    return 0;
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
    return 0;
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
    return 0;
  }
  // 2b) ... and log.
  // Make the TPVID parameter permanent.
  log::LogContext::ScopedParam sp07(lc, log::Param("TPVID", m_request.dgn));
  {
    log::LogContext::ScopedParam sp08(lc, log::Param("tapebridgeTransId", reqReport.transactionId));
    log::LogContext::ScopedParam sp09(lc, log::Param("connectDuration", reqReport.connectDuration));
    log::LogContext::ScopedParam sp00(lc, log::Param("sendRecvDuration", reqReport.sendRecvDuration));
    log::LogContext::ScopedParam sp11(lc, log::Param("TPVID", m_volInfo.vid));
    log::LogContext::ScopedParam sp12(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp13(lc, log::Param("label", m_volInfo.labelObsolete));
    log::LogContext::ScopedParam sp14(lc, log::Param("clientType", utils::volumeClientTypeToString(m_volInfo.clientType)));
    log::LogContext::ScopedParam sp15(lc, log::Param("mode", utils::volumeModeToString(m_volInfo.volumeMode)));
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
    return 0;
  default:
      return 0;
  }
}
//------------------------------------------------------------------------------
//DataTransferSession::executeRead
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DataTransferSession::executeRead(log::LogContext & lc) {
  // We are ready to start the session. We need to create the whole machinery 
  // in order to get the task injector ready to check if we actually have a 
  // file to recall.
  // findDrive does not throw exceptions (it catches them to log errors)
  // A NULL pointer is returned on failure
  std::auto_ptr<castor::tape::drives::DriveInterface> drive(findDrive(m_driveConfig,lc));
  
  if(!drive.get()) return 0;    
  // We can now start instantiating all the components of the data path
  {
    // Allocate all the elements of the memory management (in proper order
    // to refer them to each other)
    RecallReportPacker rrp(m_clientProxy,
        m_castorConf.tapebridgeBulkRequestMigrationMaxFiles,
        lc);
    // If we talk to a command line client, we do not batch report
    if (tapegateway::READ_TP == m_volInfo.clientType) {
      rrp.disableBulk();
    }
    TaskWatchDog<detail::Recall> watchdog(m_intialProcess,rrp,m_logger);
    
    RecallMemoryManager mm(m_castorConf.rtcopydNbBufs, m_castorConf.rtcopydBufsz,lc);
    TapeServerReporter tsr(m_intialProcess, m_driveConfig, 
            m_hostname, m_volInfo, lc);
    //we retrieved the detail from the client in execute, so at this point 
    //we can already report !
    tsr.gotReadMountDetailsFromClient();
    
    TapeReadSingleThread trst(*drive, m_rmc, tsr, m_volInfo, 
        m_castorConf.tapebridgeBulkRequestRecallMaxFiles,m_capUtils,watchdog,lc);

    DiskWriteThreadPool dwtp(m_castorConf.tapeserverdDiskThreads,
        rrp,
        lc);
    RecallTaskInjector rti(mm, trst, dwtp, m_clientProxy,
            m_castorConf.tapebridgeBulkRequestRecallMaxFiles,
            m_castorConf.tapebridgeBulkRequestRecallMaxBytes,lc);
    trst.setTaskInjector(&rti);
    
    // We are now ready to put everything in motion. First step is to check
    // we get any concrete job to be done from the client (via the task injector)
    utils::Timer timer;
    if (rti.synchronousInjection()) {
      // We got something to recall. Time to start the machinery
      trst.setWaitForInstructionsTime(timer.secs());
      watchdog.startThread();
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
      watchdog.stopAndWaitThread();
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
      //empty mount, hardware is OK, returns 0
      return 0;
    }
  }
}
//------------------------------------------------------------------------------
//DataTransferSession::executeWrite
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DataTransferSession::executeWrite(log::LogContext & lc) {
  // We are ready to start the session. We need to create the whole machinery 
  // in order to get the task injector ready to check if we actually have a 
  // file to migrate.
  // 1) Get hold of the drive error logs are done inside the findDrive function
  std::auto_ptr<castor::tape::drives::DriveInterface> drive(findDrive(m_driveConfig,lc));
  if (!drive.get()) return 0;
  // Once we got hold of the drive, we can run the session
  {
    
    //deferencing configLine is safe, because if configLine were not valid, 
    //then findDrive would have return NULL and we would have not end up there
    TapeServerReporter tsr(m_intialProcess, m_driveConfig, m_hostname,m_volInfo,lc);
    
    MigrationMemoryManager mm(m_castorConf.rtcopydNbBufs,
        m_castorConf.rtcopydBufsz,lc);
    MigrationReportPacker mrp(m_clientProxy,
        lc);
    TapeWriteSingleThread twst(*drive.get(),
        m_rmc,
        tsr,
        m_volInfo,
        lc,
        mrp,
        m_capUtils,    
        m_castorConf.tapebridgeMaxFilesBeforeFlush,
        m_castorConf.tapebridgeMaxBytesBeforeFlush/m_castorConf.rtcopydBufsz);
    DiskReadThreadPool drtp(m_castorConf.tapeserverdDiskThreads,
        m_castorConf.tapebridgeBulkRequestMigrationMaxFiles,
        m_castorConf.tapebridgeBulkRequestMigrationMaxBytes,
        lc);
    MigrationTaskInjector mti(mm, drtp, twst, m_clientProxy, 
            m_castorConf.tapebridgeBulkRequestMigrationMaxBytes,
            m_castorConf.tapebridgeBulkRequestMigrationMaxFiles,lc);
    drtp.setTaskInjector(&mti);
    utils::Timer timer;
    if (mti.synchronousInjection()) {
      const uint64_t firstFseqFromClient = mti.firstFseqToWrite();
      
      //the last fseq written on the tape is the first file's fseq minus one
      twst.setlastFseq(firstFseqFromClient-1);
      
      //we retrieved the detail from the client in execute, so at this point 
      //we can report. We get in exchange the number of files on the tape
      const uint64_t nbOfFileOnTape = tsr.gotWriteMountDetailsFromClient();

      //theses 2 numbers should match. Otherwise, it means the stager went mad 
      if(firstFseqFromClient != nbOfFileOnTape + 1) {
        lc.log(LOG_ERR, "First file to write's fseq  and number of files on "
        "the tape according to the VMGR dont match");
       //no mount at all, drive to be kept up = return 0
        return 0;
      }
      // We have something to do: start the session by starting all the 
      // threads.
      mm.startThreads();
      drtp.startThreads();
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
      
      return twst.getHardwareStatus();
    } else {
      // Just log this was an empty mount and that's it. The memory management
      // will be deallocated automatically.
      lc.log(LOG_ERR, "Aborting migration mount startup: empty mount");
      log::LogContext::ScopedParam sp1(lc, log::Param("errorMessage", "Aborted: empty recall mount"));
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
      //empty mount, hardware safe, return 0
      return 0;
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
castor::tape::drives::DriveInterface *
castor::tape::tapeserver::daemon::DataTransferSession::findDrive(const utils::DriveConfig
  &driveConfig, log::LogContext& lc) {
  // Find the drive in the system's SCSI devices
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo;
  try {
    driveInfo = dv.findBySymlink(driveConfig.devFilename);
  } catch (castor::tape::SCSI::DeviceVector::NotFound & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.devFilename));
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
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.devFilename));
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
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.devFilename));
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
    std::auto_ptr<castor::tape::drives::DriveInterface> drive;
    drive.reset(castor::tape::drives::DriveFactory(driveInfo, m_sysWrapper));
    if (drive.get()) drive->librarySlot = driveConfig.librarySlot;
    return drive.release();
  } catch (castor::exception::Exception & e) {
    // We could not find this drive in the system's SCSI devices
    log::LogContext::ScopedParam sp08(lc, log::Param("density", m_volInfo.density));
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.devFilename));
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
    log::LogContext::ScopedParam sp09(lc, log::Param("devFilename", driveConfig.devFilename));
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
// getZmqContext
//------------------------------------------------------------------------------
void *castor::tape::tapeserver::daemon::DataTransferSession::getZmqContext() {
  // Create the ZMQ context if it does not already exist
  if(NULL == m_zmqContext) {
    const int sizeOfIOThreadPoolForZMQ = 1;
    m_zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
    if(NULL == m_zmqContext) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to instantiate ZMQ context: " << message;
      throw ex;
    }
  }

  return m_zmqContext;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::~DataTransferSession(){
  if(NULL != m_zmqContext) {
    zmq_term(m_zmqContext);
    m_zmqContext = NULL;
  }
  google::protobuf::ShutdownProtobufLibrary();
}
