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
#include "castor/messages/messages.hpp"
#include "castor/messages/MigrationJobFromTapeGateway.pb.h"
#include "castor/messages/MigrationJobFromWriteTp.pb.h"
#include "castor/messages/NbFilesOnTape.pb.h"
#include "castor/messages/ProtoTapeReplyContainer.hpp"
#include "castor/messages/RecallJobFromReadTp.pb.h"
#include "castor/messages/RecallJobFromTapeGateway.pb.h"
#include "castor/messages/TapeMountedForRecall.pb.h"
#include "castor/messages/TapeMountedForMigration.pb.h"
#include "castor/messages/TapeserverProxyZmq.hpp"
#include "castor/messages/TapeUnmounted.pb.h"
#include "castor/messages/TapeUnmountStarted.pb.h"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"

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
    messages::RecallJobFromTapeGateway rqstBody;
    rqstBody.set_vid(vid);
    rqstBody.set_unitname(unitName);

    messages::Header rqstHeader = castor::messages::protoTapePreFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_msgtype(MSG_TYPE_RECALLJOBFROMTAPEGATEWAY);

    messages::sendMessage(m_tapeserverSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_tapeserverSocket, rqstBody);

    messages::ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of recall job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() << 
      "Failed to notify tapeserver of recall job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of recall job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotRecallJobFromReadTp
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotRecallJobFromReadTp(
  const std::string &vid, const std::string &unitName) {
  try {
    messages::RecallJobFromReadTp rqstBody;
    rqstBody.set_vid(vid);
    rqstBody.set_unitname(unitName);

    messages::Header rqstHeader = castor::messages::protoTapePreFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_msgtype(MSG_TYPE_RECALLJOBFROMREADTP);

    messages::sendMessage(m_tapeserverSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_tapeserverSocket, rqstBody);

    messages::ProtoTapeReplyContainer reply(m_tapeserverSocket);

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of recall job from readtp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of recall job from readtp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of recall job from readtp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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

    messages::ProtoTapeReplyContainer rawReply(m_tapeserverSocket);
    if(rawReply.header.msgtype() != MSG_TYPE_NBFILESONTAPE) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive reply from tapeserverd"
        ": Unexpected message type: expected=" << MSG_TYPE_NBFILESONTAPE
        << " actual=" << rawReply.header.msgtype();
      throw ex;
    }
    messages::NbFilesOnTape reply;
    if(!reply.ParseFromArray(rawReply.blobBody.getData(),
      rawReply.blobBody.size())) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to parse reply from tapeserverd"
        ": msgType=" << rawReply.header.msgtype();
      throw ex;
    }
    return reply.nbfiles(); 
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from tapegateway: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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

    messages::ProtoTapeReplyContainer rawReply(m_tapeserverSocket);
    if(rawReply.header.msgtype() != MSG_TYPE_NBFILESONTAPE) {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to receive NbFilesOnTape reply from tapeserverd: "
        "Unexpected message type: expected=" << MSG_TYPE_NBFILESONTAPE <<
        " actual=" << rawReply.header.msgtype();
      throw ex;
    }
    messages::NbFilesOnTape reply;
    if(!reply.ParseFromArray(rawReply.blobBody.getData(),
      rawReply.blobBody.size())) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to parse NbFilesOnTape reply from tapeserverd";
      throw ex;
    }
    return reply.nbfiles();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from writetp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from writetp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of migration job from writetp: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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
  
    ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for recall: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for recall: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for recall: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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
  
    ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for migration: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for migration: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of tape mounted for migration: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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
  
    ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of start of tape unmount: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of start of tape unmount: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of start of tape unmount: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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
  
    ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver that tape is unmounted: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) { 
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver that tape is unmounted: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver that tape is unmounted: " <<
      "vid=" << vid << " unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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

    ProtoTapeReplyContainer reply(m_tapeserverSocket);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of heartbeat: " <<
      "unitName=" << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &se) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of heartbeat: " <<
      "unitName=" << unitName << ": " <<
      se.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to notify tapeserver of heartbeat: " <<
      "unitName=" << unitName << ": " <<
      "Caught an unknown exception";
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
