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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


#include "castor/messages/Constants.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/messages.hpp"
#include "castor/messages/MigrationJobFromTapeGateway.pb.h"
#include "castor/messages/MigrationJobFromWriteTp.pb.h"
#include "castor/messages/NbFilesOnTape.pb.h"
#include "castor/messages/RecallJobFromReadTp.pb.h"
#include "castor/messages/RecallJobFromTapeGateway.pb.h"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/TapeMountedForMigration.pb.h"
#include "castor/messages/TapeMountedForRecall.pb.h"
#include "castor/messages/TapeUnmounted.pb.h"
#include "castor/messages/TapeUnmountStarted.pb.h"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/serrno.h"
#include "h/vmgr_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeMessageHandler::TapeMessageHandler(
  reactor::ZMQReactor &reactor,
  log::Logger &log,DriveCatalogue &driveCatalogue,
  const std::string &hostName,
  castor::legacymsg::VdqmProxy & vdqm,
  castor::legacymsg::VmgrProxy & vmgr,
  void *const zmqContext):
  m_reactor(reactor),
  m_log(log),
  m_socket(zmqContext, ZMQ_REP),
  m_driveCatalogue(driveCatalogue),
  m_hostName(hostName),
  m_vdqm(vdqm),
  m_vmgr(vmgr) { 

  std::ostringstream endpoint;
  endpoint << "tcp://127.0.0.1:" << TAPE_SERVER_INTERNAL_LISTENING_PORT;
  
  try {
    m_socket.bind(endpoint.str().c_str());
    log::Param params[] = {log::Param("endpoint", endpoint.str())};
    m_log(LOG_INFO, "Bound the ZMQ_REP socket of the TapeMessageHandler",
      params);
  } catch(castor::exception::Exception &ne){
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to bind the ZMQ_REP socket of the TapeMessageHandler"
      ": endpoint=" << endpoint.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeMessageHandler::~TapeMessageHandler()
  throw() {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::TapeMessageHandler::getName() 
  const throw() {
  return "TapeMessageHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::fillPollFd(
  zmq_pollitem_t &fd) throw() {
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = m_socket.getZmqSocket();
  fd.fd = -1;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeMessageHandler::handleEvent(
  const zmq_pollitem_t &fd) throw() {
  m_log(LOG_DEBUG, "handling event in TapeMessageHandler");

  // Try to receive a request, simply giving up if an exception is raised
  messages::Frame rqst;
  try {
    checkSocket(fd);
    rqst = messages::recvFrame(m_socket);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "TapeMessageHandler failed to handle event", params);
    return false; // Give up and stay registered with the reactor
  }

  // From this point on any exception thrown should be converted into a
  // ReturnValue message and sent back to the client
  messages::Frame reply;
  try {
    reply = dispatchMsgHandler(rqst);
  } catch(castor::exception::Exception &ex) {
    reply = createReturnValueFrame(ex.code(), ex.getMessage().str());
  } catch(std::exception &se) {
    reply = createReturnValueFrame(SEINTERNAL, se.what());
  } catch(...) {
    reply = createReturnValueFrame(SEINTERNAL, "Caught an unknown exception");
  }

  // Send the reply to the client
  try {
    messages::sendFrame(m_socket, reply);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "TapeMessageHandler failed to send reply to client", params);
  }

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::checkSocket(
  const zmq_pollitem_t &fd) {
  void* underlyingSocket = m_socket.getZmqSocket();
  if(fd.socket != underlyingSocket){
    castor::exception::Exception ex;
    ex.getMessage() << "TapeMessageHandler passed wrong poll item";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  dispatchMsgHandler(const messages::Frame &rqst) {
  m_log(LOG_DEBUG, "TapeMessageHandler dispatching message handler");
  
  switch(rqst.header.msgtype()) {
  case messages::MSG_TYPE_HEARTBEAT:
    return handleHeartbeat(rqst);

  case messages::MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY:
    return handleMigrationJobFromTapeGateway(rqst);

  case messages::MSG_TYPE_MIGRATIONJOBFROMWRITETP:
    return handleMigrationJobFromWriteTp(rqst);

  case messages::MSG_TYPE_RECALLJOBFROMREADTP:
    return handleRecallJobFromReadTp(rqst);

  case messages::MSG_TYPE_RECALLJOBFROMTAPEGATEWAY:
    return handleRecallJobFromTapeGateway(rqst);

  case messages::MSG_TYPE_TAPEMOUNTEDFORMIGRATION:
    return handleTapeMountedForMigration(rqst);

  case messages::MSG_TYPE_TAPEMOUNTEDFORRECALL:
    return handleTapeMountedForRecall(rqst);

  case messages::MSG_TYPE_TAPEUNMOUNTSTARTED:
    return handleTapeUnmountStarted(rqst);

  case messages::MSG_TYPE_TAPEUNMOUNTED:
    return handleTapeUnmounted(rqst);

  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unknown request type: msgtype=" << rqst.header.msgtype();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleHeartbeatMsg
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleHeartbeat(const messages::Frame &rqst) {
  m_log(LOG_INFO, "Handling Heartbeat message");

  try {
    castor::messages::Heartbeat rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle Heartbeat message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleMigrationJobFromTapeGateway
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleMigrationJobFromTapeGateway(const messages::Frame &rqst) {
  m_log(LOG_INFO, "Handling MigrationJobFromTapeGateway message");

  try {
    castor::messages::MigrationJobFromWriteTp rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);
    
    // Query the vmgrd daemon for information about the tape
    const legacymsg::VmgrTapeInfoMsgBody tapeInfo =
      m_vmgr.queryTape(rqstBody.vid());

    // If migrating files to tape and the client is the tapegatewayd daemon then
    // the tape must be BUSY
    if(!(tapeInfo.status & TAPE_BUSY)) {
      castor::exception::Exception ex;
      ex.getMessage() << "Invalid tape mount: Tape is not BUSY"
        ": The tapegatewayd daemon cannot mount a tape for write access if the"
        " tape is not BUSY";
      throw ex;
    }

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    drive->receivedMigrationJob(rqstBody.vid());

    messages::Frame reply = createNbFilesOnTapeFrame(tapeInfo.nbFiles);
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to handle MigrationJobFromTapeGateway message: " <<
      ne.getMessage().str();
    throw ex;
  }   
}

//------------------------------------------------------------------------------
// createNbFilesOnTapeFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  createNbFilesOnTapeFrame(const uint32_t nbFiles) {
  messages::Frame frame;

  frame.header = messages::protoTapePreFillHeader();
  frame.header.set_msgtype(messages::MSG_TYPE_NBFILESONTAPE);
  frame.header.set_bodysignature("PIPO");

  messages::NbFilesOnTape body;
  body.set_nbfiles(nbFiles);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}

//------------------------------------------------------------------------------
// handleMigrationJobFromWriteTp
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleMigrationJobFromWriteTp(const messages::Frame &rqst) {
  m_log(LOG_INFO, "Handling MigrationJobFromWriteTp message");

  try {
    messages::MigrationJobFromWriteTp rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    // Query the vmgrd daemon for information about the tape
    const legacymsg::VmgrTapeInfoMsgBody tapeInfo =
      m_vmgr.queryTape(rqstBody.vid());

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    drive->receivedMigrationJob(rqstBody.vid());

    messages::Frame reply = createNbFilesOnTapeFrame(tapeInfo.nbFiles);
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to handle MigrationJobFromWriteTp message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleRecallJobFromReadTp
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleRecallJobFromReadTp(const messages::Frame &rqst) {

  try {
    messages::RecallJobFromReadTp rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    drive->receivedRecallJob(rqstBody.vid());

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to handle RecallJobFromTapeGateway message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleRecallJobFromTapeGateway
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleRecallJobFromTapeGateway(const messages::Frame &rqst) {
  m_log(LOG_INFO, "Handling RecallJobFromTapeGateway message");

  try {
    messages::RecallJobFromTapeGateway rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    drive->receivedRecallJob(rqstBody.vid());

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to handle RecallJobFromTapeGateway message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTapeMountedForMigration
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeMountedForMigration(const messages::Frame &rqst) {
  m_log(LOG_INFO, "Handling TapeMountedForMigration message");

  try {
    messages::TapeMountedForMigration rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    const utils::DriveConfig &driveConfig = drive->getConfig();
    
    const std::string &vid = rqstBody.vid();
      drive->tapeMountedForMigration(vid);
      m_vmgr.tapeMountedForWrite(vid, drive->getSessionPid());
      m_vdqm.tapeMounted(m_hostName, rqstBody.unitname(), driveConfig.dgn,
        rqstBody.vid(), drive->getSessionPid());

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle TapeMountedForMigration message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTapeMountedForRecall
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeMountedForRecall(const messages::Frame& rqst) {
  m_log(LOG_INFO, "Handling TapeMountedForRecall message");

  try {
    messages::TapeMountedForRecall rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqstBody.unitname());
    const utils::DriveConfig &driveConfig = drive->getConfig();
    
    const std::string vid = rqstBody.vid();
    drive->tapeMountedForRecall(vid);
    m_vmgr.tapeMountedForRead(vid, drive->getSessionPid());
    m_vdqm.tapeMounted(m_hostName, rqstBody.unitname(), driveConfig.dgn,
      rqstBody.vid(), drive->getSessionPid());

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle TapeMountedForRecall message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTapeUnmountStarted
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeUnmountStarted(const messages::Frame& rqst) {
  m_log(LOG_INFO, "Handling TapeUnmountStarted message");

  try {
    messages::TapeUnmountStarted rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle TapeUnmountStarted message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTapeUnmounted
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeUnmounted(const messages::Frame& rqst) {
  m_log(LOG_INFO, "Handling TapeUnmounted message");

  try {
    messages::TapeUnmounted rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const messages::Frame reply = createReturnValueFrame(0, "");
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle TapeUnmounted message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// sendSuccessReplyToClient
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  sendSuccessReplyToClient() {
  sendReplyToClient(0,"");
}

//------------------------------------------------------------------------------
// sendErrorReplyToClient
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::
sendErrorReplyToClient(const castor::exception::Exception& ex){
  //any positive value will trigger an exception in the client side
  sendReplyToClient(ex.code(),ex.getMessageValue());
}

//------------------------------------------------------------------------------
// sendReplyToClient
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::sendReplyToClient(
  const int returnValue, const std::string& msg) {
  const messages::Frame reply = createReturnValueFrame(returnValue, msg);

  messages::sendFrame(m_socket, reply);
}

//------------------------------------------------------------------------------
// createReturnValueFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::tape::tapeserver::daemon::TapeMessageHandler::
  createReturnValueFrame(const int returnValue, const std::string& msg) {
  messages::Frame frame;

  frame.header = castor::messages::protoTapePreFillHeader();
  frame.header.set_msgtype(messages::MSG_TYPE_RETURNVALUE);
  frame.header.set_bodyhashvalue(messages::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  messages::ReturnValue body;
  body.set_returnvalue(returnValue);
  body.set_message(msg);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}
