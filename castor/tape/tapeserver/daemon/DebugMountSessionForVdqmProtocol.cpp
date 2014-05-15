/******************************************************************************
 *         castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.hpp"

#include <memory>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::DebugMountSessionForVdqmProtocol(
  const int argc,
  char **const argv,
  const std::string &hostName,
  const legacymsg::RtcpJobRqstMsgBody &job,
  castor::log::Logger &logger,
  const utils::TpconfigLines &tpConfig,
  legacymsg::VdqmProxy &vdqm,
  legacymsg::VmgrProxy &vmgr,
  legacymsg::RmcProxy &rmc,
  legacymsg::TapeserverProxy &tps) throw():
  m_netTimeout(5), // Timeout in seconds
  m_sessionPid(getpid()),
  m_argc(argc),
  m_argv(argv),
  m_hostName(hostName),
  m_job(job),
  m_log(logger),
  m_tpConfig(tpConfig),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_rmc(rmc),
  m_tps(tps) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::execute() throw (castor::exception::Exception) {
  changeProcessName();

  assignSessionPidToDrive();

  uint64_t clientMsgSeqNb = 0;

  std::auto_ptr<castor::tape::tapegateway::Volume> volume(getVolume(clientMsgSeqNb++));
  if(NULL != volume.get()) {
    log::Param params[] = {
      log::Param("TPVID", volume->vid()),
      log::Param("density", volume->density()),
      log::Param("label", volume->label()),
      log::Param("mode", volume->mode())};
    m_log(LOG_INFO, "Got VID from client", params);
    switch(volume->mode()) {
    case tapegateway::READ:
      m_tps.gotReadMountDetailsFromClient(m_job.driveUnit, volume->vid());
      break;
    case tapegateway::WRITE:
      m_tps.gotWriteMountDetailsFromClient(m_job.driveUnit, volume->vid());
      break;
    case tapegateway::DUMP:
      m_tps.gotDumpMountDetailsFromClient(m_job.driveUnit, volume->vid());
      break;
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Got an unknown volume-mode from the client: vid="
          << volume->vid() << " mode=" << volume->mode();
        throw ex;
      }
    }
    mountTape(volume->vid());
    transferFiles(*(volume.get()), clientMsgSeqNb);
  } else {
     m_log(LOG_WARNING, "Could not get VID from client");
  }

  const bool forceUnmount = true;
  releaseDrive(forceUnmount);

  if(NULL != volume.get()) {
    unmountTape(volume->vid());
  }
}

//------------------------------------------------------------------------------
// changeProcessName
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::changeProcessName() throw() {
  const size_t argv0Len = strlen(m_argv[0]);
  strncpy(m_argv[0], "mtsession", argv0Len);
}

//------------------------------------------------------------------------------
// assignSessionPidToDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::assignSessionPidToDrive() throw (castor::exception::Exception) {
  m_vdqm.assignDrive(m_hostName,  m_job.driveUnit, m_job.dgn, m_job.volReqId, m_sessionPid);
}

//------------------------------------------------------------------------------
// getVolume
//------------------------------------------------------------------------------
castor::tape::tapegateway::Volume *castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::getVolume(const uint64_t clientMsgSeqNb) const throw(castor::exception::Exception) {
  // Prepare the request
  tapegateway::VolumeRequest request;
  request.setMountTransactionId(m_job.volReqId);
  request.setAggregatorTransactionId(clientMsgSeqNb);
  request.setUnit(m_job.driveUnit);

  // Send the request and receive the reply
  std::auto_ptr<castor::IObject> obj(connectSendRequestAndReceiveReply("VolumeRequest", request));

  switch(obj->type()) {
  case OBJ_Volume:
    {
      // Down cast the reply to its specific class
      tapegateway::Volume *reply =
        dynamic_cast<tapegateway::Volume*>(obj.get());

      if(reply == NULL) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to get volume"
          ": Failed to down cast reply object to tapegateway::Volume";
        throw ex;
      }

      try {
        checkTransactionIds("Volume", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to get volume"
          ": " << ne.getMessage().str();
        throw ex;
      }

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }

  case OBJ_NoMoreFiles:
    {
      // Down cast the reply to its specific class
      tapegateway::NoMoreFiles *reply =
        dynamic_cast<tapegateway::NoMoreFiles*>(obj.get());

      if(reply == NULL) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to get volume"
          ": Failed to down cast reply object to tapegateway::NoMoreFiles";
        throw ex;
      }

      try {
        checkTransactionIds("NoMoreFiles", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to get volume: " << ne.getMessage().str();
        throw ex;
      }
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(obj.get());
    break;

  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get volume: Unknown reply type " << obj->type();
      throw ex;
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}

//-----------------------------------------------------------------------------
// connectSendRequestAndReceiveReply
//-----------------------------------------------------------------------------
castor::IObject *castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::connectSendRequestAndReceiveReply(const char *const requestTypeName, IObject &request) const throw(castor::exception::Exception) {
  const int clientSock = io::connectWithTimeout(m_job.clientHost, m_job.clientPort, m_netTimeout);
  castor::io::ClientSocket sock(clientSock);
  sock.setTimeout(m_netTimeout);
  sock.setConnTimeout(m_netTimeout);

  // Send the request
  try {
    sock.sendObject(request);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send request and receive reply"
      ": Failed to send "  << requestTypeName << " to client"
      ": " << ne.getMessage().str();
    throw ex;
  }

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope
  std::auto_ptr<castor::IObject> obj;
  try {
    obj.reset(sock.readObject());

    if(obj.get() == NULL) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to send request and receive reply"
        ": ClientSocket::readObject() returned null";
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<  "Failed to send request and receive reply"
      ": Failed to read " <<  requestTypeName << " reply from client"
      ": " << ne.getMessage().str();

    if(ne.code() == SETIMEDOUT) {
      ex.getMessage() << ": Timed out after " << sock.timeout() << " seconds";
    }

    throw ex;
  }

  // Close the connection to the client
  sock.close();

  return(obj.release());
}

//-----------------------------------------------------------------------------
// checkTransactionIds
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::checkTransactionIds(
  const char *const messageTypeName,
  const uint32_t    actualMountTransactionId,
  const uint64_t    expectedTapebridgeTransId,
  const uint64_t    actualTapebridgeTransId) const
  throw(castor::exception::Exception) {

  int nbErrors = 0;
  castor::exception::Exception ex;
  ex.getMessage() << "Mismatch detected";

  if(m_job.volReqId != actualMountTransactionId) {
    nbErrors++;
    ex.getMessage() <<
      ": Mount transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedMountTransactionId=" << m_job.volReqId <<
      " actualMountTransactionId=" << actualMountTransactionId;
  }

  if(expectedTapebridgeTransId != actualTapebridgeTransId) {
    nbErrors++;
    ex.getMessage() <<
      ": Tape-bridge transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedTapebridgeTransId=" << expectedTapebridgeTransId <<
      " actualTapebridgeTransId=" << actualTapebridgeTransId;
  }
  if(nbErrors > 0) {
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// throwEndNotificationErrorReport
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::throwEndNotificationErrorReport(IObject *const obj) const throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  tapegateway::EndNotificationErrorReport *reply =
    dynamic_cast<tapegateway::EndNotificationErrorReport*>(obj);

  if(reply == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to down cast reply object to tapegateway::EndNotificationErrorReport";
    throw ex;
  }

  // Translate the reception of the error report into a C++ exception
  castor::exception::Exception ex;
  ex.getMessage() << "Client error report: " << reply->errorMessage();
  throw ex;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::mountTape(const std::string &vid) throw (castor::exception::Exception) {
  const std::string drive = getLibrarySlot(m_job.driveUnit);
  m_rmc.mountTape(vid, drive);

  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};
  m_log(LOG_INFO, "Tape mounted", params);
  m_vdqm.tapeMounted(m_hostName, m_job.driveUnit, m_job.dgn, vid, m_sessionPid);
}

//------------------------------------------------------------------------------
// getLibrarySlot
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::getLibrarySlot(const std::string &unitName)
  throw (castor::exception::Exception) {
  for(utils::TpconfigLines::const_iterator itor = m_tpConfig.begin(); itor != m_tpConfig.end(); itor++) {
    if(unitName == itor->unitName) {
      return itor->librarySlot;
    }
  }

  castor::exception::Exception ex;
  ex.getMessage() << "Failed to find library position of drive " << unitName;
  throw ex;
}

//------------------------------------------------------------------------------
// transferFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::transferFiles(tapegateway::Volume &volume, uint64_t &clientMsgSeqNb) throw (castor::exception::Exception) {
  switch(volume.mode()) {
  case tapegateway::READ:
    recallFiles(volume.vid(), clientMsgSeqNb);
    break;
  case tapegateway::WRITE:
    migrateFiles(volume.vid(), clientMsgSeqNb);
    break;
  case tapegateway::DUMP:
    dumpFiles(volume.vid());
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to transfer files: Unknown mode: " << volume.mode();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// recallFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::recallFiles(const std::string &vid, uint64_t &clientMsgSeqNb) throw (castor::exception::Exception) {
  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};

  m_log(LOG_INFO, "Starting to recall files from tape to disk", params);

  tapegateway::FilesToRecallList *fileList = NULL;
  const uint64_t maxFiles = 1;
  const uint64_t maxBytes = 1;
  while(NULL != (fileList = getFilesToRecall(++clientMsgSeqNb, maxFiles, maxBytes))) {
    std::vector<tapegateway::FileToRecallStruct*> files = fileList->filesToRecall();
    for(std::vector<tapegateway::FileToRecallStruct*>::const_iterator itor = files.begin(); itor != files.end(); itor++) {
      const tapegateway::FileToRecallStruct *const file = *itor;
      if(NULL == file) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to recall files: FileToRecallStruct pointer is NULL";
        throw ex;
      }
      log::Param params[] = {
        log::Param("path", file->path()),
        log::Param("fseq", file->fseq()),
        log::Param("blockId0", (unsigned int)file->blockId0()),
        log::Param("blockId1", (unsigned int)file->blockId1()),
        log::Param("blockId2", (unsigned int)file->blockId2()),
        log::Param("blockId3", (unsigned int)file->blockId3())};
      m_log(LOG_INFO, "Pretending to recall file", params);

      // Emulate the actual transfer by sleeping
      ::sleep(1);
    }
  }

  m_log(LOG_INFO, "Finished recalling files from tape to disk", params);
  notifyEndOfSession(clientMsgSeqNb);
}

//------------------------------------------------------------------------------
// migrateFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::migrateFiles(const std::string &vid, uint64_t &clientMsgSeqNb) throw (castor::exception::Exception) {
  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};
  
  m_log(LOG_INFO, "Starting to migrate files from disk to tape", params);
  
  // Emulate migrating files from disk to tape by sleeping
  ::sleep(1);

  m_log(LOG_INFO, "Finished migrating files from disk to tape", params);
}

//------------------------------------------------------------------------------
// dumpFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::dumpFiles(const std::string &vid) throw (castor::exception::Exception) {
  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};

  m_log(LOG_INFO, "Starting to dump tape files", params);

  // Emulate dumping tape files
  ::sleep(1);

  m_log(LOG_INFO, "Finished dumping tape files", params);
}

//------------------------------------------------------------------------------
// releaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::releaseDrive(const bool forceUnmount) throw (castor::exception::Exception) {
  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("forceUnmount", forceUnmount)};
  m_log(LOG_INFO, "Releasing tape drive", params);

  m_vdqm.releaseDrive(m_hostName, m_job.driveUnit, m_job.dgn, forceUnmount, m_sessionPid);
}

//------------------------------------------------------------------------------
// unmountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::unmountTape(const std::string &vid) throw (castor::exception::Exception) {
  const std::string librarySlot = getLibrarySlot(m_job.driveUnit);
  m_rmc.unmountTape(vid, librarySlot);

  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid),
     log::Param("librarySlot", librarySlot)};
  m_log(LOG_INFO, "Tape unmounted", params);
}

//-----------------------------------------------------------------------------
// getFilesToRecall
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FilesToRecallList *castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::getFilesToRecall(const uint64_t clientMsgSeqNb, const uint64_t maxFiles, const uint64_t maxBytes) const throw(castor::exception::Exception) {
  const int clientSock = sendFilesToRecallListRequest(clientMsgSeqNb, maxFiles, maxBytes);
  tapegateway::FilesToRecallList *const fileList = receiveFilesToRecallListRequestReplyAndClose(clientMsgSeqNb, clientSock);
  return fileList;
}

//-----------------------------------------------------------------------------
// sendFilesToRecallListRequest
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::sendFilesToRecallListRequest(const uint64_t clientMsgSeqNb, const uint64_t maxFiles, const uint64_t maxBytes) const throw(castor::exception::Exception) {

  // Check method arguments
  if(maxFiles == 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send FilesToRecallListRequest"
      ": Invalid argument: maxFiles is 0";
    throw ex;
  }
  if(maxBytes == 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send FilesToRecallListRequest"
      ": Invalid argument: maxBytes is 0";
    throw ex;
  }

  // Prepare the request
  tapegateway::FilesToRecallListRequest request;
  request.setMountTransactionId(m_job.volReqId);
  request.setAggregatorTransactionId(clientMsgSeqNb);
  request.setMaxFiles(maxFiles);
  request.setMaxBytes(maxBytes);

  // Send the request and return the socket-descriptor of the connection
  try {
    return connectAndSendMessage(request);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send FilesToRecallListRequest"
      ": maxFiles=" << maxFiles << " maxBytes=" << maxBytes <<
      ": " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectAndSendMessage
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::connectAndSendMessage(IObject &message) const throw(castor::exception::Exception) {
  const int clientSock = io::connectWithTimeout(m_job.clientHost, m_job.clientPort, m_netTimeout);
  castor::io::ClientSocket sock(clientSock);
  sock.setTimeout(m_netTimeout);
  sock.setConnTimeout(m_netTimeout);

  // Send the message
  try {
    sock.sendObject(message);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send message"
      ": Failed to send request to client"
      ": clientHost=" << m_job.clientHost << " clientPort=" << m_job.clientPort <<
      " netTimeout=" << m_netTimeout <<
      ": " << ex.getMessage().str();
    throw ex;
  }

  // Release the socket-descriptor from the castor::io::ClientSocket object so
  // that it is not closed by its destructor
  const int socketDescriptor = sock.socket();
  sock.resetSocket();

  // Return the socket-descriptor
  return(socketDescriptor);
}

//-----------------------------------------------------------------------------
// receiveFilesToRecallListRequestReplyAndClose
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FilesToRecallList *castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::receiveFilesToRecallListRequestReplyAndClose(const uint64_t clientMsgSeqNb, const int clientSock) const throw(castor::exception::Exception) {
  const char *const task = "receive reply to FilesToRecallListRequest and close connection";

  std::auto_ptr<castor::IObject> obj;
  try {
    obj.reset(receiveReplyAndClose(clientSock));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": " << ne.getMessage().str();
    throw ex;
  }

  switch(obj->type()) {
  case OBJ_FilesToRecallList:
    {
      // Down cast the reply to its specific class
      tapegateway::FilesToRecallList *reply =
        dynamic_cast<tapegateway::FilesToRecallList*>(obj.get());

      if(reply == NULL) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to " << task << ": Failed to down cast reply object to tapegateway::FilesToRecallList";
        throw ex;
      }

      try {
        checkTransactionIds("FilesToRecallList", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to " << task << ": " << ne.getMessage().str();
        throw ex;
      }

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }
    break;

  case OBJ_NoMoreFiles:
    {
      // Down cast the reply to its specific class
      tapegateway::NoMoreFiles *reply =
        dynamic_cast<tapegateway::NoMoreFiles*>(obj.get());

      if(reply == NULL) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to " << task << ": Failed to down cast reply object to tapegateway::NoMoreFiles";
        throw ex;
      }

      try {
        checkTransactionIds("NoMoreFiles", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to " << task << ": " << ne.getMessage().str();
        throw ex;
      }
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(obj.get());
    break;

  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task << ": Unknown reply type"
        ": Reply type = " << obj->type();
      throw ex;
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}

//-----------------------------------------------------------------------------
// receiveReplyAndClose
//-----------------------------------------------------------------------------
castor::IObject *castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::receiveReplyAndClose(const int clientSock) const throw(castor::exception::Exception) {
  const char *const task = "receive reply and close connection";
  // Receive the reply object
  castor::io::AbstractTCPSocket sock(clientSock);
  sock.setTimeout(m_netTimeout);
  std::auto_ptr<castor::IObject> obj;
  try {
    obj.reset(sock.readObject());

    if(NULL == obj.get()) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task << ": ClientSocket::readObject() returned null";
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task << ": Failed to read reply from client"
      ": " << ne.getMessage().str();

    if(ne.code() == SETIMEDOUT) {
      ex.getMessage() << ": Timed out after " << sock.timeout() << " seconds";
    }
    throw ex;
  }

  // Close the connection to the client
  sock.close();

  return(obj.release());
}

//-----------------------------------------------------------------------------
// notifyEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::notifyEndOfSession(const uint64_t clientMsgSeqNb) const throw(castor::exception::Exception) {

  // Prepare the request
  tapegateway::EndNotification request;
  request.setMountTransactionId(m_job.volReqId);
  request.setAggregatorTransactionId(clientMsgSeqNb);

  const char *requestTypeName = "EndNotification";
  notifyClient(clientMsgSeqNb, requestTypeName, request);
}

//-----------------------------------------------------------------------------
// notifyClient
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::notifyClient(
  const uint64_t    clientMsgSeqNb,
  const char *const requestTypeName,
  IObject&          request) const
  throw(castor::exception::Exception) {

  // Send the request and receive the reply
  std::auto_ptr<castor::IObject> obj(connectSendRequestAndReceiveReply(requestTypeName, request));

  switch(obj->type()) {
  case OBJ_NotificationAcknowledge:
    {
      // Down cast the reply to its specific class
      tapegateway::NotificationAcknowledge *reply =
        dynamic_cast<tapegateway::NotificationAcknowledge*>(obj.get());

      if(reply == NULL) {
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to notify client: Failed to down cast reply object to tapegateway::NotificationAcknowledge";
        throw ex;
      }

      checkTransactionIds("NotificationAcknowledge",
        reply->mountTransactionId(), clientMsgSeqNb,
        reply->aggregatorTransactionId());
    }
    break;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(obj.get());
    break;

  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to notify client: Unknown reply type : Reply type = " << obj->type();
      throw ex;
    }
  } // switch(obj->type())
}
