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
#include "castor/messages/RecallJobFromReadTp.pb.h"
#include "castor/messages/RecallJobFromTapeGateway.pb.h"
#include "castor/messages/ReplyContainer.hpp"
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
  m_messageSocket(zmqContext, ZMQ_REQ),
  m_heartbeatSocket(zmqContext, ZMQ_REQ) {
  castor::messages::connectToLocalhost(m_messageSocket,tapeserverPort);
  castor::messages::connectToLocalhost(m_heartbeatSocket,tapeserverPort);
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

    messages::Header rqstHeader = castor::messages::preFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_reqtype(messages::reqType::RecallJobFromTapeGateway);

    messages::sendMessage(m_messageSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_messageSocket, rqstBody);

    messages::ProtoTapeReplyContainer reply(m_messageSocket);
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

    messages::Header rqstHeader = castor::messages::preFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_reqtype(messages::reqType::RecallJobFromReadTp);

    messages::sendMessage(m_messageSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_messageSocket, rqstBody);

    messages::ProtoTapeReplyContainer reply(m_messageSocket);

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
    messages::MigrationJobFromTapeGateway rqstBody;
    rqstBody.set_vid(vid);
    rqstBody.set_unitname(unitName);

    messages::Header rqstHeader = castor::messages::preFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_reqtype(messages::reqType::MigrationJobFromTapeGateway);

    messages::sendMessage(m_messageSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_messageSocket, rqstBody);

    messages::ProtoTapeReplyContainer rawReply(m_messageSocket);
    if(rawReply.header.reqtype() != messages::reqType::NbFilesOnTape) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive reply from tapeserverd"
        ": Unexpected message type: expected=" << messages::reqType::NbFilesOnTape
        << " actual=" << rawReply.header.reqtype();
      throw ex;
    }
    messages::NbFilesOnTape reply;
    if(!reply.ParseFromArray(rawReply.blobBody.data(),
      rawReply.blobBody.size())) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to parse reply from tapeserverd"
        ": msgType=" << rawReply.header.reqtype();
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
// gotMigrationJobFromWriteTp
//------------------------------------------------------------------------------
uint32_t castor::messages::TapeserverProxyZmq::gotMigrationJobFromWriteTp(
  const std::string &vid, const std::string &unitName) {
  try {
    messages::MigrationJobFromWriteTp rqstBody;
    rqstBody.set_vid(vid);
    rqstBody.set_unitname(unitName);

    messages::Header rqstHeader = castor::messages::preFillHeader();
    rqstHeader.set_bodyhashvalue(computeSHA1Base64(rqstBody));
    rqstHeader.set_bodysignature("PIPO");
    rqstHeader.set_reqtype(messages::reqType::MigrationJobFromWriteTp);

    messages::sendMessage(m_messageSocket, rqstHeader, ZMQ_SNDMORE);
    messages::sendMessage(m_messageSocket, rqstBody);

    messages::ProtoTapeReplyContainer rawReply(m_messageSocket);
    if(rawReply.header.reqtype() != messages::reqType::NbFilesOnTape) {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to receive NbFilesOnTape reply from tapeserverd: "
        "Unexpected message type: expected=" << messages::reqType::NbFilesOnTape
        << " actual=" << rawReply.header.reqtype();
      throw ex;
    }
    messages::NbFilesOnTape reply;
    if(!reply.ParseFromArray(rawReply.blobBody.data(),
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
// tapeMountedForRecall
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForRecall(
  const std::string &vid, const std::string &unitName) {  
  try {
    castor::messages::TapeMountedForRecall body;
    body.set_unitname(unitName);
    body.set_vid(vid);
  
    castor::messages::Header header = castor::messages::preFillHeader();
    header.set_bodyhashvalue(computeSHA1Base64(body));
    header.set_bodysignature("PIPO");
    header.set_reqtype(castor::messages::reqType::TapeMountedForRecall);
  
    castor::messages::sendMessage(m_messageSocket,header,ZMQ_SNDMORE);
    castor::messages::sendMessage(m_messageSocket,body);
  
    castor::messages::ProtoTapeReplyContainer reply(m_messageSocket);
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
// tapeMountedForMigration
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForMigration(
  const std::string &vid, const std::string &unitName) {  
  try {
    castor::messages::TapeMountedForMigration body;
    body.set_unitname(unitName);
    body.set_vid(vid);
  
    castor::messages::Header header = castor::messages::preFillHeader();
    header.set_bodyhashvalue(computeSHA1Base64(body));
    header.set_bodysignature("PIPO");
    header.set_reqtype(castor::messages::reqType::TapeMountedForMigration);
  
    castor::messages::sendMessage(m_messageSocket,header,ZMQ_SNDMORE);
    castor::messages::sendMessage(m_messageSocket,body);
  
    castor::messages::ProtoTapeReplyContainer reply(m_messageSocket);
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
// tapeUnmountStarted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmountStarted(
  const std::string &vid, const std::string &unitName) {   
  try {
    castor::messages::TapeUnmountStarted body;
    body.set_unitname(unitName);
    body.set_vid(vid);

    castor::messages::Header header = castor::messages::preFillHeader();
    header.set_bodyhashvalue(computeSHA1Base64(body));
    header.set_bodysignature("PIPO");
    header.set_reqtype(castor::messages::reqType::TapeUnmountStarted);

    castor::messages::sendMessage(m_messageSocket,header,ZMQ_SNDMORE);
    castor::messages::sendMessage(m_messageSocket,body);
  
    castor::messages::ProtoTapeReplyContainer reply(m_messageSocket);
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
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmounted(
  const std::string &vid, const std::string &unitName) {
  try {
    castor::messages::TapeUnmounted body;
    body.set_unitname(unitName);
    body.set_vid(vid);
    
    castor::messages::Header header=castor::messages::preFillHeader();
    header.set_bodyhashvalue(computeSHA1Base64(body));
    header.set_bodysignature("PIPO");
    header.set_reqtype(castor::messages::reqType::TapeUnmounted);
  
    castor::messages::sendMessage(m_messageSocket,header,ZMQ_SNDMORE);
    castor::messages::sendMessage(m_messageSocket,body);
  
    castor::messages::ProtoTapeReplyContainer reply(m_messageSocket);
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

//-----------------------------------------------------------------------------
// notifyHeartbeat
//-----------------------------------------------------------------------------
 void  castor::messages::TapeserverProxyZmq::
 notifyHeartbeat(uint64_t nbOfMemblocksMoved){
   messages::Heartbeat body;
   body.set_bytesmoved(nbOfMemblocksMoved);
   
   messages::Header header = messages::preFillHeader();
   header.set_reqtype(messages::reqType::Heartbeat);
   header.set_bodyhashvalue(computeSHA1Base64(body));
   header.set_bodysignature("PIPO");

   messages::sendMessage(m_heartbeatSocket,header,ZMQ_SNDMORE);
   messages::sendMessage(m_heartbeatSocket,body);
   
   ProtoTapeReplyContainer reply(m_heartbeatSocket);
 }
