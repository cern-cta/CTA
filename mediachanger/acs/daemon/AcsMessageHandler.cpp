/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/Constants.hpp"
#include "mediachanger/acs/daemon/Constants.hpp"
#include "AcsMessageHandler.hpp"
#include "AcsDismountTape.hpp"
#include "AcsForceDismountTape.hpp"
#include "AcsMountTapeReadOnly.hpp"
#include "AcsMountTapeReadWrite.hpp"
#include "mediachanger/acs/Acs.hpp"
#include "mediachanger/acs/AcsImpl.hpp"
#include "common/log/log.hpp"
#include "common/log/SyslogLogger.hpp"
#include "mediachanger/messages.hpp"
#include "mediachanger/ReturnValue.pb.h"
#include "mediachanger/AcsMountTapeReadOnly.pb.h"
#include "mediachanger/AcsMountTapeReadWrite.pb.h"
#include "mediachanger/AcsDismountTape.pb.h"
#include "mediachanger/AcsForceDismountTape.pb.h"
#include "mediachanger/Exception.pb.h"
#include "mediachanger/reactor/ZMQPollEventHandler.hpp"
#include "mediachanger/reactor/ZMQReactor.hpp"
#include "mediachanger/ZmqSocket.hpp"
#include "errno.h"

#include <iostream>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsMessageHandler::AcsMessageHandler(
  cta::log::Logger &log,
  cta::mediachanger::reactor::ZMQReactor &reactor,
  void *const zmqContext,
  const std::string &hostName,
  const AcsdConfiguration &ctaConf,
  AcsPendingRequests &acsPendingRequests):
  m_log(log),
  m_reactor(reactor),
  m_socket(zmqContext, ZMQ_ROUTER),
  m_hostName(hostName),
  m_ctaConf(ctaConf),
  m_acsPendingRequests(acsPendingRequests) { 

  std::ostringstream endpoint;
  endpoint << "tcp://127.0.0.1:" << m_ctaConf.port.value();
  
  try {
    m_socket.bind(endpoint.str().c_str());
    std::list<log::Param> params = {log::Param("endpoint", endpoint.str())};
    m_log(LOG_INFO, "Bound the ZMQ socket of the AcsMessageHandler",
      params);
  } catch(cta::exception::Exception &ne){
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to bind the ZMQ socket of the AcsMessageHandler"
      ": endpoint=" << endpoint.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsMessageHandler::~AcsMessageHandler()
  throw() {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::daemon::AcsMessageHandler::getName()
  const throw() {
  return "AcsMessageHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMessageHandler::fillPollFd(
  zmq_pollitem_t &fd) throw() {
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = m_socket.getZmqSocket();
  fd.fd = -1;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsMessageHandler::handleEvent(
  const zmq_pollitem_t &fd) throw() {
  // Try to receive a request, simply giving up if an exception is raised
  cta::mediachanger::Frame rqst;

  //for handling zeroMQ's router socket type specific elements 
  //ie first frame = identity of the sender
  //   second one  =  empty
  //   third and following = actual data frames
 
  //The ZmqMsg address data can be dump as string and used as key for storing 
  //the identity (for clients who need a late answer)
  cta::mediachanger::ZmqMsg address;
  cta::mediachanger::ZmqMsg empty;
  try {
    checkSocket(fd);
    m_socket.recv(address);
    m_socket.recv(empty);
    rqst = recvFrame(m_socket);
  } catch(cta::exception::Exception &ex) {
    std::list<log::Param> params = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "AcsMessageHandler failed to handle event", params);
    return false; // Give up and stay registered with the reactor
  }
  std::list<log::Param> params = {
      log::Param("sender identity", 
              utils::hexDump(address.getData(),address.size()))
     };
  m_log(LOG_DEBUG, "handling event in AcsMessageHandler", params);
 
  // From this point on any exception thrown should be converted into an
  // Exception message and sent back to the client 
  cta::mediachanger::Frame reply;
  
  try {
    // if there are any problems we need to send the replay to the client.
    reply = dispatchMsgHandler(rqst);
  } catch(cta::exception::Exception &ex) {
    reply = createExceptionFrame(ECANCELED, ex.getMessage().str()); 
    m_log(LOG_ERR, ex.getMessage().str());
  } catch(std::exception &se) {
    reply = createExceptionFrame(ECANCELED, se.what());
    m_log(LOG_ERR, se.what());
  } catch(...) {
    reply = createExceptionFrame(ECANCELED, "Caught an unknown exception"); 
    m_log(LOG_ERR, "Caught an unknown exception");
  }

  // Send the reply to the client
  try {
    //we need to prepend our frames the same way we received them
    // ie identity + empty frames 
    m_socket.send(address,ZMQ_SNDMORE);
    m_socket.send(empty,ZMQ_SNDMORE);

    cta::mediachanger::sendFrame(m_socket, reply);
  } catch(cta::exception::Exception &ex) {
    std::list<log::Param> params = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "AcsMessageHandler failed to send reply to client", params);
  }

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkSocket
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMessageHandler::checkSocket(
  const zmq_pollitem_t &fd) const{
  void* underlyingSocket = m_socket.getZmqSocket();
  if(fd.socket != underlyingSocket){
    cta::exception::Exception ex;
    ex.getMessage() << "AcsMessageHandler passed wrong poll item";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  dispatchMsgHandler(const cta::mediachanger::Frame &rqst) {
  m_log(LOG_DEBUG, "AcsMessageHandler dispatching message handler");
  
  const cta::mediachanger::acs::daemon::MsgType msgType = (cta::mediachanger::acs::daemon::MsgType)rqst.header.msgtype();
  switch(msgType) {
  case cta::mediachanger::MSG_TYPE_ACSMOUNTTAPEREADONLY:
    return handleAcsMountTapeReadOnly(rqst);
      
  case cta::mediachanger::MSG_TYPE_ACSMOUNTTAPEREADWRITE:
    return handleAcsMountTapeReadWrite(rqst);  

  case cta::mediachanger::MSG_TYPE_ACSDISMOUNTTAPE:
    return handleAcsDismountTape(rqst);

  case cta::mediachanger::MSG_TYPE_ACSFORCEDISMOUNTTAPE:
    return handleAcsForceDismountTape(rqst);

  default:
    {
      const std::string msgTypeStr = cta::mediachanger::acs::daemon::msgTypeToString(msgType);
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unexpected request type: msgType=" << msgType << " msgTypeStr=" <<
        msgTypeStr;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleAcsMountTapeReadOnly
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  handleAcsMountTapeReadOnly(const cta::mediachanger::Frame &rqst) {
  m_log(LOG_DEBUG, "Handling AcsMountTapeReadOnly message");

  try {
    cta::mediachanger::AcsMountTapeReadOnly rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);
    
    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    std::list<log::Param> params = {log::Param("TPVID", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "Mount tape for read-only access", params);

    cta::mediachanger::acs::AcsImpl acsWrapper;
    cta::mediachanger::acs::daemon::AcsMountTapeReadOnly acsMountTapeReadOnly(vid, acs, lsm, 
      panel, drive, acsWrapper, m_log, m_ctaConf);
    try {
      acsMountTapeReadOnly.execute();
      m_log(LOG_INFO,"Tape successfully mounted for read-only access", params);
    } catch (cta::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape mount for read-only access failed: "
        + ne.getMessage().str(), params);  
      throw;  
    }     
    const cta::mediachanger::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsMountTapeReadOnly message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleAcsMountTapeReadWrite
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  handleAcsMountTapeReadWrite(const cta::mediachanger::Frame &rqst) {
  m_log(LOG_DEBUG, "Handling AcsMountTapeReadWrite message");

  try {
    cta::mediachanger::AcsMountTapeReadWrite rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);
     
    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    std::list<log::Param> params = {log::Param("TPVID", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "Mount tape for read/write access",params);

    cta::mediachanger::acs::AcsImpl acsWrapper;
    cta::mediachanger::acs::daemon::AcsMountTapeReadWrite acsMountTapeReadWrite(vid, acs,
      lsm, panel, drive, acsWrapper, m_log, m_ctaConf);
    try {
      acsMountTapeReadWrite.execute();   
      m_log(LOG_INFO,"Tape successfully mounted for read/write access", params);
    } catch (cta::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape mount for read/write access failed: "
        + ne.getMessage().str(), params);  
      throw;  
    }     
    const cta::mediachanger::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsMountTapeReadWrite message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleAcsDismountTape
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  handleAcsDismountTape(const cta::mediachanger::Frame& rqst) {
  m_log(LOG_DEBUG, "Handling AcsDismountTape message");

  try {
    cta::mediachanger::AcsDismountTape rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();
    
    std::list<log::Param> params = {log::Param("TPVID", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "Dismount tape",params);

    cta::mediachanger::acs::AcsImpl acsWrapper;
    cta::mediachanger::acs::daemon::AcsDismountTape acsDismountTape(vid, acs, lsm, panel, drive,
      acsWrapper, m_log, m_ctaConf);
    try {
      acsDismountTape.execute();
      m_log(LOG_INFO,"Tape successfully dismounted", params);
    } catch (cta::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape dismount failed: "+ne.getMessage().str(), params);  
      throw;  
    }    
    const cta::mediachanger::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsDismountTape message: " <<
      ne.getMessage().str();
    throw ex;
  } catch(...) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsDismountTape message: " 
                    << "Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleAcsForceDismountTape
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  handleAcsForceDismountTape(const cta::mediachanger::Frame& rqst) {
  m_log(LOG_DEBUG, "Handling AcsDismountTape message");

  try {
    cta::mediachanger::AcsForceDismountTape rqstBody;
    rqst.parseBodyIntoProtocolBuffer(rqstBody);

    const std::string vid = rqstBody.vid();
    const uint32_t acs    = rqstBody.acs();
    const uint32_t lsm    = rqstBody.lsm();
    const uint32_t panel  = rqstBody.panel();
    const uint32_t drive  = rqstBody.drive();

    std::list<log::Param> params = {log::Param("TPVID", vid),
      log::Param("acs", acs),
      log::Param("lsm", lsm),
      log::Param("panel", panel),
      log::Param("drive", drive)};
    m_log(LOG_INFO, "Force dismount tape", params);

    cta::mediachanger::acs::AcsImpl acsWrapper;
    cta::mediachanger::acs::daemon::AcsForceDismountTape acsForceDismountTape(vid, acs, lsm,
      panel, drive, acsWrapper, m_log, m_ctaConf);
    try {
      acsForceDismountTape.execute();
      m_log(LOG_INFO,"Tape successfully force dismounted", params);
    } catch (cta::exception::Exception &ne) {
      m_log(LOG_ERR,"Tape force dismount failed: "+ne.getMessage().str(),
        params);
      throw;
    }
    const cta::mediachanger::Frame reply = createReturnValueFrame(0);
    return reply;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsForceDismountTape message: " <<
      ne.getMessage().str();
    throw ex;
  } catch(...) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to handle AcsForceDismountTape message: "
                    << "Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createReturnValueFrame
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  createReturnValueFrame(const int value) {
  cta::mediachanger::Frame frame;

  frame.header = cta::mediachanger::protoTapePreFillHeader();
  frame.header.set_msgtype(cta::mediachanger::MSG_TYPE_RETURNVALUE);
  frame.header.set_bodyhashvalue(cta::mediachanger::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  cta::mediachanger::ReturnValue body;
  body.set_value(value);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}

//------------------------------------------------------------------------------
// createExceptionFrame
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsMessageHandler::
  createExceptionFrame(const int code, const std::string& msg) {
  cta::mediachanger::Frame frame;

  frame.header = cta::mediachanger::protoTapePreFillHeader();
  frame.header.set_msgtype(cta::mediachanger::MSG_TYPE_EXCEPTION);
  frame.header.set_bodyhashvalue(cta::mediachanger::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  cta::mediachanger::Exception body;
  body.set_code(code);
  body.set_message(msg);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}
