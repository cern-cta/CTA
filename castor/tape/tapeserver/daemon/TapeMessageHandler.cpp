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
#include "castor/messages/TapeMountedForMigration.pb.h"
#include "castor/messages/TapeMountedForRecall.pb.h"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
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
  fd.fd= -1;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeMessageHandler::handleEvent(
  const zmq_pollitem_t &fd) throw() {
  try {
    checkSocket(fd);
    m_log(LOG_DEBUG,"handling event in TapeMessageHandler");
  
    messages::Header header; 
    try {
      tape::utils::ZmqMsg headerBlob;
      m_socket.recv(&headerBlob.getZmqMsg());
    
      if(!zmq_msg_more(&headerBlob.getZmqMsg())){
        castor::exception::Exception ex;
        ex.getMessage() << "No message body after reading the header";
        throw ex;
      }
    
      header = buildHeader(headerBlob);
    } catch(castor::exception::Exception &ne){
      castor::exception::Exception ex;
      ex.getMessage() << "Error handling message header: " <<
        ne.getMessage().str();
      throw ex;
    }

    tape::utils::ZmqMsg bodyBlob;
    try {
      m_socket.recv(&bodyBlob.getZmqMsg());
    } catch(castor::exception::Exception &ne){
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive message body: " << 
        ne.getMessage().str();
      throw ex;
    }
  
    dispatchMsgHandler(header, bodyBlob);

  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "TapeMessageHandler failed to handle event", params);
  } catch(std::exception &se) {
    log::Param params[] = {log::Param("message", se.what())};
    m_log(LOG_ERR, "TapeMessageHandler failed to handle event", params);
  } catch(...) {
    log::Param params[] = {
      log::Param("message", "Caught an unknown exception")};
    m_log(LOG_ERR, "TapeMessageHandler failed to handle event", params);
  }

  return false; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// checkSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::checkSocket(
  const zmq_pollitem_t &fd) {
  void* underlyingSocket = m_socket.getZmqSocket();
  if(fd.socket != underlyingSocket){
    castor::exception::Exception ex;
    ex.getMessage() <<"TapeMessageHandler passed wrong poll item";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// buildHeader
//------------------------------------------------------------------------------
castor::messages::Header castor::tape::tapeserver::daemon::TapeMessageHandler::buildHeader(
  tape::utils::ZmqMsg &headerBlob){  
  messages::Header header; 
  const bool headerCorrectlyParsed = header.ParseFromArray(
    zmq_msg_data(&headerBlob.getZmqMsg()), zmq_msg_size(&headerBlob.getZmqMsg()));
  
  if(!headerCorrectlyParsed){
    castor::exception::Exception ex;
    ex.getMessage() <<"TapeMessageHandler received a bad header while handling event";
    throw ex;
  }
    if(header.magic() != TPMAGIC){
    castor::exception::Exception ex;
    ex.getMessage() <<"Wrong magic number in the header";
    throw ex;
  }
  if(header.protocoltype() != messages::protocolType::Tape){
    castor::exception::Exception ex;
    ex.getMessage() <<"Wrong protocol specified in the header";
    throw ex;
  }
  return header;
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::dispatchMsgHandler(
  messages::Header& header, const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_DEBUG, "TapeMessageHandler dispatching messag handler");
  
  switch(header.reqtype()){
  case messages::reqType::Heartbeat:
    return handleHeartbeatMsg(header, bodyBlob);
  case messages::reqType::MigrationJobFromTapeGateway:
    return handleMigrationJobFromTapeGateway(header, bodyBlob);
  case messages::reqType::MigrationJobFromWriteTp:
    return handleMigrationJobFromWriteTp(header, bodyBlob);
  case messages::reqType::RecallJobFromReadTp:
    return handleRecallJobFromReadTp(header, bodyBlob);
  case messages::reqType::RecallJobFromTapeGateway:
    return handleRecallJobFromTapeGateway(header, bodyBlob);
  case messages::reqType::TapeMountedForMigration:
    return handleTapeMountedForMigration(header, bodyBlob);
  case messages::reqType::TapeMountedForRecall:
    return handleTapeMountedForRecall(header, bodyBlob);
  case messages::reqType::TapeUnmountStarted:
    return handleTapeUnmountStarted(header, bodyBlob);
  case messages::reqType::TapeUnmounted:
    return handleTapeUnmounted(header, bodyBlob);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unknown request type: reqtype=" << header.reqtype();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleHeartbeatMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::handleHeartbeatMsg(
  const messages::Header& header, const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_DEBUG, "Handling Heartbeat message");

  try {
    castor::messages::Heartbeat body;
    parseMsgBlob(body, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);
  
    std::vector<log::Param> param;
    param.push_back(log::Param("bytesMoved",body.bytesmoved()));
  
    sendSuccessReplyToClient();
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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleMigrationJobFromTapeGateway(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling MigrationJobFromTapeGateway message");

  try {
    castor::messages::MigrationJobFromWriteTp rqst;
    parseMsgBlob(rqst, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);
    
    // Query the vmgrd daemon for information about the tape
    const legacymsg::VmgrTapeInfoMsgBody tapeInfo =
      m_vmgr.queryTape(rqst.vid());

    // If migrating files to tape and the client is the tapegatewayd daemon then
    // the tape must be BUSY
    if(!(tapeInfo.status & TAPE_BUSY)) {
      castor::exception::Exception ex;
      ex.getMessage() << "Invalid tape mount: Tape is not BUSY: The tapegatewayd"
        " daemon cannot mount a tape for write access if the tape is not BUSY";
      throw ex;
    }

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqst.unitname());
    drive->receivedMigrationJob(rqst.vid());
    
    // TO BE DONE - Exception should cause sendErrorReplyToClient(ex) to be
    // called.
    messages::NbFilesOnTape replyBody;
    replyBody.set_nbfiles(tapeInfo.nbFiles);
    
    messages::Header replyHeader = castor::messages::protoTapePreFillHeader();
    replyHeader.set_reqtype(messages::reqType::NbFilesOnTape);
    replyHeader.set_bodyhashvalue(messages::computeSHA1Base64(replyBody));
    replyHeader.set_bodysignature("PIPO");
      
    //send the number of files on the tape to the client
    messages::sendMessage(m_socket, replyHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_socket, replyBody);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to handle MigrationJobFromTapeGateway message: " <<
      ne.getMessage().str();
    throw ex;
  }   
}

//------------------------------------------------------------------------------
// handleMigrationJobFromWriteTp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleMigrationJobFromWriteTp(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling MigrationJobFromWriteTp message");

  try {
    castor::messages::MigrationJobFromWriteTp rqst;
    parseMsgBlob(rqst, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);

    // Query the vmgrd daemon for information about the tape
    const legacymsg::VmgrTapeInfoMsgBody tapeInfo =
      m_vmgr.queryTape(rqst.vid());

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(rqst.unitname());
    drive->receivedMigrationJob(rqst.vid());

    // TO BE DONE - Exception should cause sendErrorReplyToClient(ex) to be
    // called.
    messages::NbFilesOnTape replyBody;
    replyBody.set_nbfiles(tapeInfo.nbFiles);

    messages::Header replyHeader = castor::messages::protoTapePreFillHeader();
    replyHeader.set_reqtype(messages::reqType::NbFilesOnTape);
    replyHeader.set_bodyhashvalue(messages::computeSHA1Base64(replyBody));
    replyHeader.set_bodysignature("PIPO");

    //send the number of files on the tape to the client
    messages::sendMessage(m_socket, replyHeader,ZMQ_SNDMORE);
    messages::sendMessage(m_socket, replyBody);
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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleRecallJobFromReadTp(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {

  try {
    castor::messages::RecallJobFromReadTp body;
    parseMsgBlob(body, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(body.unitname());
    drive->receivedRecallJob(body.vid());

    // TO BE DONE - Exception should cause sendErrorReplyToClient(ex) to be
    // called.
    sendSuccessReplyToClient();

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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleRecallJobFromTapeGateway(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling RecallJobFromTapeGateway message");

  try {
    castor::messages::RecallJobFromTapeGateway body;
    parseMsgBlob(body, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(body.unitname());
    drive->receivedRecallJob(body.vid());

    // TO BE DONE - Exception should cause sendErrorReplyToClient(ex) to be
    // called.
    sendSuccessReplyToClient();

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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeMountedForMigration(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling TapeMountedForMigration message");
  try {
    castor::messages::TapeMountedForMigration body;
    parseMsgBlob(body, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(body.unitname());
    const utils::DriveConfig &driveConfig = drive->getConfig();
    
    const std::string &vid = body.vid();
    try {
      drive->tapeMountedForMigration(vid);
      m_vmgr.tapeMountedForWrite(vid, drive->getSessionPid());
      m_vdqm.tapeMounted(m_hostName, body.unitname(), driveConfig.dgn,
        body.vid(), drive->getSessionPid());
    } catch(const castor::exception::Exception& ex) {
      sendErrorReplyToClient(ex);
      throw;
    }
    sendSuccessReplyToClient();
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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeMountedForRecall(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling TapeMountedForRecall message");

  try {
    castor::messages::TapeMountedForRecall body;
    parseMsgBlob(body, bodyBlob);
    castor::messages::checkSHA1(header, bodyBlob);

    DriveCatalogueEntry *const drive =
      m_driveCatalogue.findDrive(body.unitname());
    const utils::DriveConfig &driveConfig = drive->getConfig();
    
    const std::string vid = body.vid();
    try {
      drive->tapeMountedForRecall(vid);
      m_vmgr.tapeMountedForRead(vid, drive->getSessionPid());
      m_vdqm.tapeMounted(m_hostName, body.unitname(), driveConfig.dgn,
        body.vid(), drive->getSessionPid());
    } catch(const castor::exception::Exception& ex) {
      sendErrorReplyToClient(ex);
      throw;
    }
    sendSuccessReplyToClient();
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
void castor::tape::tapeserver::daemon::TapeMessageHandler::
  handleTapeUnmountStarted(const messages::Header& header,
  const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling TapeUnmountStarted message");

  try {
    sendSuccessReplyToClient();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle TapeUnmountStarted message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleNotifyDriveTapeUnmountedMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::handleTapeUnmounted(
  const messages::Header& header, const tape::utils::ZmqMsg &bodyBlob) {
  m_log(LOG_INFO, "Handling TapeUnmounted message");

  try {
    sendSuccessReplyToClient();
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
  castor::messages::ReturnValue body;
  body.set_returnvalue(returnValue);
  body.set_message(msg);
  
  messages::Header header = castor::messages::protoTapePreFillHeader();
  header.set_reqtype(messages::reqType::ReturnValue);
  header.set_bodyhashvalue(messages::computeSHA1Base64(body));
  header.set_bodysignature("PIPO");

  messages::sendMessage(m_socket, header,ZMQ_SNDMORE);
  messages::sendMessage(m_socket, body);
}
