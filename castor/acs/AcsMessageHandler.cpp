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

#include "castor/messages/messages.hpp"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/AcsMountTapeReadOnly.pb.h"
#include "castor/messages/AcsMountTapeReadWrite.pb.h"
#include "castor/messages/AcsDismountTape.pb.h"
#include "castor/acs/Constants.hpp"
#include "castor/acs/AcsMessageHandler.hpp"
#include "castor/acs/AcsDismountTape.hpp"
#include "castor/acs/AcsMountTapeReadOnly.hpp"
#include "castor/acs/AcsMountTapeReadWrite.hpp"
#include "castor/acs/CastorConf.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/acs/Acs.hpp"
#include "castor/acs/AcsImpl.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsMessageHandler::AcsMessageHandler(
  castor::tape::reactor::ZMQReactor &reactor,
  log::Logger &log,
  const std::string &hostName,
  void *const zmqContext,
  const CastorConf &castorConf,
  AcsPendingRequests &acsPendingRequests):
  m_reactor(reactor),
  m_log(log),
  m_socket(zmqContext, ZMQ_ROUTER),
  m_hostName(hostName),
  m_castorConf(castorConf),
  m_acsPendingRequests(acsPendingRequests) { 

  std::ostringstream endpoint;
  endpoint << "tcp://127.0.0.1:" << m_castorConf.ascServerInternalListeningPort;
  
  try {
    m_socket.bind(endpoint.str().c_str());
    log::Param params[] = {log::Param("endpoint", endpoint.str())};
    m_log(LOG_INFO, "Bound the ZMQ socket of the AcsMessageHandler",
      params);
  } catch(castor::exception::Exception &ne){
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to bind the ZMQ socket of the AcsMessageHandler"
      ": endpoint=" << endpoint.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsMessageHandler::~AcsMessageHandler()
  throw() {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::acs::AcsMessageHandler::getName()
  const throw() {
  return "AcsMessageHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::acs::AcsMessageHandler::fillPollFd(
  zmq_pollitem_t &fd) throw() {
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = m_socket.getZmqSocket();
  fd.fd = -1;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::acs::AcsMessageHandler::handleEvent(
  const zmq_pollitem_t &fd) throw() {
  // Try to receive a request, simply giving up if an exception is raised
  messages::Frame rqst;

  //for handling zeroMQ's router socket type specific elements 
  //ie first frame = identity of the sender
  //   second one  =  empty
  //   third and following = actual data frames
 
  //The ZmqMsg address data can be dump as string and used as key for storing 
  //the identity (for clients who need a late answer)
  castor::messages::ZmqMsg address;
  castor::messages::ZmqMsg empty;
  try {
    checkSocket(fd);
    m_socket.recv(address);
    m_socket.recv(empty);
    rqst = messages::recvFrame(m_socket);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "AcsMessageHandler failed to handle event", params);
    return false; // Give up and stay registered with the reactor
  }
  log::Param params[] = {
      log::Param("sender identity", 
              tape::utils::toHexString(address.getData(),address.size()))
     };
  m_log(LOG_DEBUG, "handling event in AcsMessageHandler", params);
 
  // From this point on any exception thrown should be converted into an
  // Exception message and sent back to the client 
  messages::Frame reply;
  bool exceptionOccurred = false;
  
  try {
    //m_acsPendingRequests.checkAndAddRequest(address, empty, rqst, m_socket);
    // if there are any problems we need to send the replay to the client.
    
    reply = dispatchMsgHandler(rqst);
  } catch(castor::exception::Exception &ex) {
    reply = createExceptionFrame(ex.code(), ex.getMessage().str());
    exceptionOccurred=true;
    m_log(LOG_ERR, ex.getMessage().str());
  } catch(std::exception &se) {
    reply = createExceptionFrame(SEINTERNAL, se.what());
    exceptionOccurred=true;
    m_log(LOG_ERR, se.what());
  } catch(...) {
    reply = createExceptionFrame(SEINTERNAL, "Caught an unknown exception");  
    exceptionOccurred=true;
    m_log(LOG_ERR, "Caught an unknown exception");
  }

/*
  if (exceptionOccurred) {
    // Send the reply to the client if we were not able to add request to the
    // list
    try {
      //we need to prepend our frames the same way we received them
      // ie identity + empty frames 
      m_socket.send(address,ZMQ_SNDMORE);
      m_socket.send(empty,ZMQ_SNDMORE);
      messages::sendFrame(m_socket, reply);    
    } catch(castor::exception::Exception &ex) {
      log::Param params[] = {log::Param("message", ex.getMessage().str())};
      m_log(LOG_ERR, "AcsMessageHandler failed to send reply to client", params);
    }
  }
*/

  // Send the reply to the client
  try {
    //we need to prepend our frames the same way we received them
    // ie identity + empty frames 
    m_socket.send(address,ZMQ_SNDMORE);
    m_socket.send(empty,ZMQ_SNDMORE);

    messages::sendFrame(m_socket, reply);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "AcsMessageHandler failed to send reply to client", params);
  }

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkSocket
//------------------------------------------------------------------------------
void castor::acs::AcsMessageHandler::checkSocket(
  const zmq_pollitem_t &fd) const{
  void* underlyingSocket = m_socket.getZmqSocket();
  if(fd.socket != underlyingSocket){
    castor::exception::Exception ex;
    ex.getMessage() << "AcsMessageHandler passed wrong poll item";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  dispatchMsgHandler(const messages::Frame &rqst) {
  m_log(LOG_DEBUG, "AcsMessageHandler dispatching message handler");
  
  switch(rqst.header.msgtype()) {
    case messages::MSG_TYPE_ACSMOUNTTAPEREADONLY:
      return handleAcsMountTapeReadOnly(rqst);
      
    case messages::MSG_TYPE_ACSMOUNTTAPEREADWRITE:
      return handleAcsMountTapeReadWrite(rqst);  

    case messages::MSG_TYPE_ACSDISMOUNTTAPE:
      return handleAcsDismountTape(rqst);

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
// handleAcsMountTapeReadOnly
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  handleAcsMountTapeReadOnly(const messages::Frame &rqst) {
  m_log(LOG_DEBUG, "Handling AcsMountTapeReadOnly message");

  try {
    messages::AcsMountTapeReadOnly rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);
    
    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    log::Param params[] = {log::Param("vid", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "AcsMountTapeReadOnly message", params);

    castor::acs::AcsImpl acsWrapper;
    castor::acs::AcsMountTapeReadOnly acsMountTapeReadOnly(vid, acs, lsm, 
      panel, drive, acsWrapper, m_log, m_castorConf);
    try {
      acsMountTapeReadOnly.execute();
      m_log(LOG_INFO,"Tape successfully mounted for read only access", params);
    } catch (castor::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape mount for read only access failed: "
        + ne.getMessage().str(), params);  
      throw;  
    }     
    const messages::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsMountTapeReadOnly message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleAcsMountTapeReadWrite
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  handleAcsMountTapeReadWrite(const messages::Frame &rqst) {
  m_log(LOG_DEBUG, "Handling AcsMountTapeReadWrite message");

  try {
    messages::AcsMountTapeReadWrite rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);
     
    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    log::Param params[] = {log::Param("vid", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "AcsMountTapeReadWrite message", params);

    castor::acs::AcsImpl acsWrapper;
    castor::acs::AcsMountTapeReadWrite acsMountTapeReadWrite(vid, acs,
      lsm, panel, drive, acsWrapper, m_log, m_castorConf);
    try {
      acsMountTapeReadWrite.execute();   
      m_log(LOG_INFO,"Tape successfully mounted for read/write access", params);
    } catch (castor::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape mount for read/write access failed: "
        + ne.getMessage().str(), params);  
      throw;  
    }     
    const messages::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsMountTapeReadWrite message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleAcsDismountTape
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  handleAcsDismountTape(const messages::Frame& rqst) {
  m_log(LOG_DEBUG, "Handling AcsDismountTape message");

  try {
    messages::AcsDismountTape rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    log::Param params[] = {log::Param("vid", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "AcsDismountTape message", params);

    castor::acs::AcsImpl acsWrapper;
    castor::acs::AcsDismountTape acsDismountTape(vid, acs, lsm, panel, drive,
      acsWrapper, m_log, m_castorConf);
    try {
      acsDismountTape.execute();
      m_log(LOG_INFO,"Tape successfully dismounted", params);
    } catch (castor::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape dismount failed: "+ne.getMessage().str(), params);  
      throw;  
    }    
    const messages::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsDismountTape message: " <<
      ne.getMessage().str();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsDismountTape message: " 
                    << "Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createReturnValueFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  createReturnValueFrame(const int value) {
  messages::Frame frame;

  frame.header = castor::messages::protoTapePreFillHeader();
  frame.header.set_msgtype(messages::MSG_TYPE_RETURNVALUE);
  frame.header.set_bodyhashvalue(messages::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  messages::ReturnValue body;
  body.set_value(value);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}

//------------------------------------------------------------------------------
// createExceptionFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsMessageHandler::
  createExceptionFrame(const int code, const std::string& msg) {
  messages::Frame frame;

  frame.header = castor::messages::protoTapePreFillHeader();
  frame.header.set_msgtype(messages::MSG_TYPE_EXCEPTION);
  frame.header.set_bodyhashvalue(messages::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  messages::Exception body;
  body.set_code(code);
  body.set_message(msg);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}
