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

#include "castor/io/io.hpp"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/LabelError.pb.h"
#include "castor/messages/messages.hpp"
#include "castor/messages/MigrationJobFromTapeGateway.pb.h"
#include "castor/messages/MigrationJobFromWriteTp.pb.h"
#include "castor/messages/NbFilesOnTape.pb.h"
#include "castor/messages/RecallJobFromReadTp.pb.h"
#include "castor/messages/RecallJobFromTapeGateway.pb.h"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/TapeMountedForRecall.pb.h"
#include "castor/messages/TapeMountedForMigration.pb.h"
#include "castor/messages/TapeserverProxyZmq.hpp"
#include "castor/messages/TapeUnmounted.pb.h"
#include "castor/messages/TapeUnmountStarted.pb.h"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::TapeserverProxyZmq::TapeserverProxyZmq(log::Logger &log, 
  const unsigned short tapeserverPort, const int netTimeout,
  void *const zmqContext) throw():
  m_log(log),
  m_tapeserverHostName("localhost"),
  m_tapeserverPort(tapeserverPort),
  m_netTimeout(netTimeout),
  m_tapeserverSocket(zmqContext, ZMQ_REQ) {
  connectZmqSocketToLocalhost(m_tapeserverSocket, tapeserverPort);
}

//------------------------------------------------------------------------------
// gotRecallJobFromTapeGateway
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotRecallJobFromTapeGateway(
  const std::string &vid, const std::string &unitName) {
  try {
    const Frame rqst = createRecallJobFromTapeGatewayFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
      "Failed to notify tapeserver of recall job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createRecallJobFromTapeGatewayFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createRecallJobFromTapeGatewayFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_RECALLJOBFROMTAPEGATEWAY);
    frame.header.set_bodysignature("PIPO");

    RecallJobFromTapeGateway body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create RecallJobFromTapeGateway frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}


//------------------------------------------------------------------------------
// gotRecallJobFromReadTp
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotRecallJobFromReadTp(
  const std::string &vid, const std::string &unitName) {
  try {
    const Frame rqst = createRecallJobFromReadTpFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
      "Failed to notify tapeserver of recall job from readtp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createRecallJobFromReadTpFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createRecallJobFromReadTpFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_RECALLJOBFROMREADTP);
    frame.header.set_bodysignature("PIPO");

    RecallJobFromReadTp body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create RecallJobFromReadTp frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotMigrationJobFromTapeGateway
//------------------------------------------------------------------------------
uint32_t castor::messages::TapeserverProxyZmq::gotMigrationJobFromTapeGateway(
  const std::string &vid, const std::string &unitName) {
  try {
    const Frame rqst = createMigrationJobFromTapeGatewayFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);

    NbFilesOnTape reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
    return reply.nbfiles();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createMigrationJobFromTapeGatewayFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createMigrationJobFromTapeGatewayFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY);
    frame.header.set_bodysignature("PIPO");

    MigrationJobFromTapeGateway body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create MigrationJobFromTapeGateway frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotMigrationJobFromWriteTp
//------------------------------------------------------------------------------
uint32_t castor::messages::TapeserverProxyZmq::gotMigrationJobFromWriteTp(
  const std::string &vid, const std::string &unitName) {
  try {
    const Frame rqst = createMigrationJobFromWriteTpFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);

    NbFilesOnTape reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
    return reply.nbfiles();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from writetp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createMigrationJobFromWriteTpFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createMigrationJobFromWriteTpFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_MIGRATIONJOBFROMWRITETP);
    frame.header.set_bodysignature("PIPO");

    MigrationJobFromWriteTp body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create MigrationJobFromWriteTp frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForRecall
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForRecall(
  const std::string &vid, const std::string &unitName) {  
  try {
    const Frame rqst = createTapeMountedForRecallFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
  try {
    const Frame rqst = createTapeMountedForMigrationFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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

//------------------------------------------------------------------------------
// tapeUnmountStarted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmountStarted(
  const std::string &vid, const std::string &unitName) {   
  try {
    const Frame rqst = createTapeUnmountStartedFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
      "Failed to notify tapeserver of start of tape unmount: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createTapeUnmountStartedFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createTapeUnmountStartedFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_TAPEUNMOUNTSTARTED);
    frame.header.set_bodysignature("PIPO");

    TapeUnmountStarted body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create TapeUnmountStarted frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmounted(
  const std::string &vid, const std::string &unitName) {
  try {
    const Frame rqst = createTapeUnmountedFrame(vid, unitName);
    sendFrame(m_tapeserverSocket, rqst);
  
    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
      "Failed to notify tapeserver that tape is unmounted: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createTapeUnmountedFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::TapeserverProxyZmq::
  createTapeUnmountedFrame(const std::string &vid,
  const std::string &unitName) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_TAPEUNMOUNTED);
    frame.header.set_bodysignature("PIPO");

    TapeUnmounted body;
    body.set_vid(vid);
    body.set_unitname(unitName);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create TapeUnmounted frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// notifyHeartbeat
//-----------------------------------------------------------------------------
void  castor::messages::TapeserverProxyZmq::notifyHeartbeat(
  const std::string &unitName, const uint64_t nbBlocksMoved) {

  try {
    const Frame rqst = createHeartbeatFrame(unitName, nbBlocksMoved);
    sendFrame(m_tapeserverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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

//------------------------------------------------------------------------------
// labelError
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::labelError(
  const std::string &unitName, const castor::exception::Exception &labelEx) {
  try {
    const Frame rqst = createLabelErrorFrame(unitName, labelEx);
    sendFrame(m_tapeserverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_tapeserverSocket, reply);
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
  const castor::exception::Exception &labelEx) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_LABELERROR);
    frame.header.set_bodysignature("PIPO");

    LabelError body;
    body.set_unitname(unitName);
    body.set_code(labelEx.code());
    body.set_message(labelEx.getMessage().str());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create LabelError frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}
