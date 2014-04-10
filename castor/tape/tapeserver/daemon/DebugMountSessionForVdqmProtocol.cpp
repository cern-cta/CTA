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
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.hpp"

#include <memory>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::DebugMountSessionForVdqmProtocol(
  const std::string &hostName,
  const legacymsg::RtcpJobRqstMsgBody &job,
  castor::log::Logger &logger,
  const utils::TpconfigLines &tpConfig,
  Vdqm &vdqm,
  Vmgr &vmgr) throw():
  m_netTimeout(5), // Timeout in seconds
  m_sessionPid(getpid()),
  m_hostName(hostName),
  m_job(job),
  m_log(logger),
  m_tpConfig(tpConfig),
  m_vdqm(vdqm),
  m_vmgr(vmgr) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::execute() throw (castor::exception::Exception) {
  assignSessionPidToDrive();

  uint64_t clientMsgSeqNb = 0;

  std::auto_ptr<castor::tape::tapegateway::Volume> volume(getVolume(clientMsgSeqNb++));
  if(NULL != volume.get()) {
    mountTape(volume->vid());
    transferFiles(*(volume.get()));
  }

  const bool forceUnmount = true;
  releaseDrive(forceUnmount);

  if(NULL != volume.get()) {
    unmountTape(volume->vid());
  }
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
        castor::exception::Internal ex;
        ex.getMessage() << "Failed to get volume"
          ": Failed to down cast reply object to tapegateway::Volume";
        throw ex;
      }

      try {
        checkTransactionIds("Volume", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Internal ex;
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
        castor::exception::Internal ex;
        ex.getMessage() << "Failed to get volume"
          ": Failed to down cast reply object to tapegateway::NoMoreFiles";
        throw ex;
      }

      try {
        checkTransactionIds("NoMoreFiles", reply->mountTransactionId(),
          clientMsgSeqNb, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ne) {
        castor::exception::Internal ex;
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
      castor::exception::Internal ex;
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
  const int clientFd = io::connectWithTimeout(m_job.clientHost, m_job.clientPort, m_netTimeout);
  castor::io::ClientSocket sock(clientFd);
  sock.setTimeout(m_netTimeout);
  sock.setConnTimeout(m_netTimeout);

  // Send the request
  try {
    sock.sendObject(request);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
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
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to send request and receive reply"
        ": ClientSocket::readObject() returned null";
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() <<  "Failed to send request and receive reply"
      ": Failed to read " <<  requestTypeName << " reply from client"
      ": " << ne.getMessage().str();

    if(ex.code() == SETIMEDOUT) {
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
  std::stringstream errorMessage;

  if(m_job.volReqId != actualMountTransactionId) {
    nbErrors++;
    errorMessage <<
      ": Mount transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedMountTransactionId=" << m_job.volReqId <<
      " actualMountTransactionId=" << actualMountTransactionId;
  }

  if(expectedTapebridgeTransId != actualTapebridgeTransId) {
    nbErrors++;
    errorMessage <<
      ": Tape-bridge transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedTapebridgeTransId=" << expectedTapebridgeTransId <<
      " actualTapebridgeTransId=" << actualTapebridgeTransId;
  }
  if(nbErrors > 0) {
    TAPE_THROW_CODE(EBADMSG,
      errorMessage.str());
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
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to down cast reply object to tapegateway::EndNotificationErrorReport";
    throw ex;
  }

  // Translate the reception of the error report into a C++ exception
  castor::exception::Internal ex;
  ex.getMessage() << "Client error report: " << reply->errorMessage();
  throw ex;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::mountTape(const std::string &vid) throw (castor::exception::Exception) {
  // Emulate tape mount by sleeping
  ::sleep(1);

  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};
  m_log(LOG_INFO, "Tape mounted", params);
  m_vdqm.tapeMounted(m_hostName, m_job.driveUnit, m_job.dgn, vid, m_sessionPid);
}

//------------------------------------------------------------------------------
// transferFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::transferFiles(tapegateway::Volume &volume) throw (castor::exception::Exception) {
  switch(volume.mode()) {
  case tapegateway::READ:
    recallFiles(volume.vid());
    break;
  case tapegateway::WRITE:
    migrateFiles(volume.vid());
    break;
  case tapegateway::DUMP:
    dumpFiles(volume.vid());
    break;
  default:
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to transfer files: Unknown mode: " << volume.mode();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// recallFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::recallFiles(const std::string &vid) throw (castor::exception::Exception) {
  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};

  m_log(LOG_INFO, "Starting to recall files from tape to disk", params);

  // Emulate recalling files from tape to disk by sleeping
  ::sleep(1);

  m_log(LOG_INFO, "Finished recalling files from tape to disk", params);
}

//------------------------------------------------------------------------------
// migrateFiles
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::migrateFiles(const std::string &vid) throw (castor::exception::Exception) {
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
  // Emulate tape unmount by sleeping
  ::sleep(1);

  log::Param params[] = {
    log::Param("unitName", m_job.driveUnit),
    log::Param("TPVID", vid)};
  m_log(LOG_INFO, "Tape unmounted", params);
  m_vdqm.tapeUnmounted(m_hostName, m_job.driveUnit, m_job.dgn, vid);
}
