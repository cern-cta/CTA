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

#include "castor/messages/AddLogParams.pb.h"
#include "castor/messages/ArchiveJobFromCTA.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/DeleteLogParams.pb.h"
#include "castor/messages/MutexLocker.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/LabelError.pb.h"
#include "castor/messages/messages.hpp"
#include "castor/messages/MigrationJobFromTapeGateway.pb.h"
#include "castor/messages/MigrationJobFromWriteTp.pb.h"
#include "castor/messages/NbFilesOnTape.pb.h"
#include "castor/messages/RecallJobFromReadTp.pb.h"
#include "castor/messages/RecallJobFromTapeGateway.pb.h"
#include "castor/messages/RetrieveJobFromCTA.pb.h"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/TapeMountedForRecall.pb.h"
#include "castor/messages/TapeMountedForMigration.pb.h"
#include "castor/messages/TapeserverProxyZmq.hpp"
#include "castor/messages/TapeUnmounted.pb.h"
#include "castor/messages/TapeUnmountStarted.pb.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::TapeserverProxyZmq::TapeserverProxyZmq(log::Logger &log, 
  const unsigned short serverPort, void *const zmqContext,
  const std::string &driveName) throw():
  m_log(log),
  m_driveName(driveName),
  m_serverPort(serverPort),
  m_serverSocket(zmqContext, ZMQ_REQ) {
  connectZmqSocketToLocalhost(m_serverSocket, serverPort);
}

//------------------------------------------------------------------------------
// reportState
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::reportState(const cta::tape::session::SessionState state,
  const cta::tape::session::SessionType type, const std::string& vid) {
  if ((type == cta::tape::session::SessionType::Archive) 
      && (state == cta::tape::session::SessionState::Mounting)) {
    gotArchiveJobFromCTA(vid, m_driveName, 0);
  }
}

//------------------------------------------------------------------------------
// reportHeartbeat
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {}

//------------------------------------------------------------------------------
// gotArchiveJobFromCTA
//------------------------------------------------------------------------------
uint32_t castor::messages::TapeserverProxyZmq::gotArchiveJobFromCTA(
  const std::string &vid, const std::string &unitName, const uint32_t nbFiles) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createArchiveJobFromCTAFrame(vid, unitName, nbFiles);
    sendFrame(m_serverSocket, rqst);

    NbFilesOnTape reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    return reply.nbfiles();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of archive job from CTA: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotRetrieveJobFromCTA
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotRetrieveJobFromCTA(
  const std::string &vid, const std::string &unitName) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createRetrieveJobFromCTAFrame(vid, unitName);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of retrieve job from CTA: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createRetrieveJobFromCTAFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createRetrieveJobFromCTAFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_RETRIEVEJOBFROMCTA);
    frame.header.set_bodysignature("PIPO");

    RetrieveJobFromCTA body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create RetrieveJobFromCTA frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createArchiveJobFromCTAFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createArchiveJobFromCTAFrame(const std::string &vid,
  const std::string &unitName, const uint32_t nbFiles) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ARCHIVEJOBFROMCTA);
    frame.header.set_bodysignature("PIPO");

    ArchiveJobFromCTA body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    body.set_nbfiles(nbFiles);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create ArchiveJobFromCTA frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForRecall
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForRecall(
  const std::string &vid, const std::string &unitName) {  
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createTapeMountedForRecallFrame(vid, unitName);
    sendFrame(m_serverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for recall: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createTapeMountedForRecallFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createTapeMountedForRecallFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_TAPEMOUNTEDFORRECALL);
    frame.header.set_bodysignature("PIPO");

    TapeMountedForRecall body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create TapeMountedForRecall frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForMigration
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForMigration(
  const std::string &vid, const std::string &unitName) {  
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createTapeMountedForMigrationFrame(vid, unitName);
    sendFrame(m_serverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for migration: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createTapeMountedForMigrationFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createTapeMountedForMigrationFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_TAPEMOUNTEDFORMIGRATION);
    frame.header.set_bodysignature("PIPO");

    TapeMountedForMigration body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create TapeMountedForMigration frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// notifyHeartbeat
//-----------------------------------------------------------------------------
void  castor::messages::TapeserverProxyZmq::notifyHeartbeat(
  const std::string &unitName, const uint64_t nbBlocksMoved) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createHeartbeatFrame(unitName, nbBlocksMoved);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of heartbeat: " <<
      "unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createHeartbeatFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createHeartbeatFrame(const std::string &unitName,
  const uint64_t nbBlocksMoved) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_HEARTBEAT);
    frame.header.set_bodysignature("PIPO");
    
    Heartbeat body;
    body.set_unitname(unitName);
    body.set_nbblocksmoved(nbBlocksMoved);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create Heartbeat frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// addLogParams
//-----------------------------------------------------------------------------
void  castor::messages::TapeserverProxyZmq::addLogParams(
  const std::string &unitName,
  const std::list<castor::log::Param> & params) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createAddLogParamsFrame(unitName, params);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to send tapeserver addLogParams: " <<
      "unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAddLogParamsFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createAddLogParamsFrame(const std::string &unitName,
  const std::list<castor::log::Param> & params) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ADDLOGPARAMS);
    frame.header.set_bodysignature("PIPO");
    
    AddLogParams body;
    body.set_unitname(unitName);
    for(std::list<castor::log::Param>::const_iterator i=params.begin();
        i!=params.end(); i++) {
      LogParam * lp = body.add_params();
      lp->set_name(i->getName());
      lp->set_value(i->getValue());
    }
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AddLogParams frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// deleteLogParams
//-----------------------------------------------------------------------------
void  castor::messages::TapeserverProxyZmq::deleteLogParams(
  const std::string &unitName,
  const std::list<std::string> & paramNames) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createDeleteLogParamsFrame(unitName, paramNames);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to send tapeserver deleteLogParams: " <<
      "unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAddLogParamsFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createDeleteLogParamsFrame(const std::string &unitName,
  const std::list<std::string> & paramNames) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_DELETELOGPARAMS);
    frame.header.set_bodysignature("PIPO");
    
    DeleteLogParams body;
    body.set_unitname(unitName);
    for(std::list<std::string>::const_iterator i=paramNames.begin();
        i!=paramNames.end(); i++) {
      body.add_param_names(*i);
    }
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create DeleteLogParams frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// labelError
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::labelError(
  const std::string &unitName, const std::string &message) {
  MutexLocker lock(&m_mutex);

  try {
    const Frame rqst = createLabelErrorFrame(unitName, message);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of label-session error: " <<
      "unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createLabelErrorFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createLabelErrorFrame(const std::string &unitName,
  const std::string &message) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_LABELERROR);
    frame.header.set_bodysignature("PIPO");

    LabelError body;
    body.set_unitname(unitName);
    body.set_message(message);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create LabelError frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}
