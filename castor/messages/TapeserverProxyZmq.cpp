/******************************************************************************
 *                castor/messages/TapeserverProxyZmq.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/TapeserverProxyZmq.hpp"
#include "castor/messages/messages.hpp"
#include "castor/messages/NotifyDrive.pb.h"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"

namespace {
  castor::messages::NotifyDriveBeforeMountStarted_TapeClientType 
  convertClientType(castor::tape::tapegateway::ClientType val){
    switch(val){
      case castor::tape::tapegateway::TAPE_GATEWAY:
        return castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_GATEWAY;
      case castor::tape::tapegateway::READ_TP:
        return castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_READTP;
      case castor::tape::tapegateway::WRITE_TP:
        return castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_WRITETP;
      case castor::tape::tapegateway::DUMP_TP:
        return castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_DUMPTP;
      default:
        return castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_DUMPTP;
    }
  }
    castor::messages::TapeMode 
  convertVolumeMode(castor::tape::tapegateway::VolumeMode val){
    switch(val){
      case castor::tape::tapegateway::READ:
        return castor::messages::TAPE_MODE_READ;
      case castor::tape::tapegateway::WRITE:
        return castor::messages::TAPE_MODE_READWRITE;
      case castor::tape::tapegateway::DUMP:
        return castor::messages::TAPE_MODE_DUMP;
      default:
        return castor::messages::TAPE_MODE_NONE;
    }
  }
}
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::TapeserverProxyZmq::TapeserverProxyZmq(log::Logger &log, 
        const unsigned short tapeserverPort, const int netTimeout,zmq::context_t& ctx) throw():
  m_log(log),
  m_tapeserverHostName("localhost"),
  m_tapeserverPort(tapeserverPort),
  m_netTimeout(netTimeout),m_socket(ctx,ZMQ_REQ) {
  castor::messages::connectToLocalhost(m_socket);
}

//------------------------------------------------------------------------------
// gotReadMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotReadMountDetailsFromClient(
castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
        const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveBeforeMountStarted);
  
  castor::messages::NotifyDriveBeforeMountStarted body;
  body.set_clienttype(convertClientType(volInfo.clientType));
  body.set_mode(convertVolumeMode(volInfo.volumeMode));
  body.set_unitname(unitName);
  body.set_vid(volInfo.vid);

  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg(); 
}

//------------------------------------------------------------------------------
// gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------
uint64_t
  castor::messages::TapeserverProxyZmq::gotWriteMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveBeforeMountStarted);
  
  castor::messages::NotifyDriveBeforeMountStarted body;
  body.set_clienttype(convertClientType(volInfo.clientType));
  body.set_mode(convertVolumeMode(volInfo.volumeMode));
  body.set_unitname(unitName);
  body.set_vid(volInfo.vid);

  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  readReplyMsg();
  return 0; // TO BE DONE
}

//------------------------------------------------------------------------------
// gotDumpMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::gotDumpMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveBeforeMountStarted);
  
  castor::messages::NotifyDriveBeforeMountStarted body;
  body.set_clienttype(convertClientType(volInfo.clientType));
  body.set_mode(convertVolumeMode(volInfo.volumeMode));
  body.set_unitname(unitName);
  body.set_vid(volInfo.vid);

  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg();
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForRead(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveTapeMounted);
  
  castor::messages::NotifyDriveTapeMounted body;
  body.set_mode(convertVolumeMode(volInfo.volumeMode));
  body.set_unitname(unitName);
  body.set_vid(volInfo.vid);

  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg();
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeMountedForWrite(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveTapeMounted);
  
  castor::messages::NotifyDriveTapeMounted body;
  body.set_mode(convertVolumeMode(volInfo.volumeMode));
  body.set_unitname(unitName);
  body.set_vid(volInfo.vid);

  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg();
}

//------------------------------------------------------------------------------
// tapeUnmounting
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmounting(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveUnmountStarted);
  
  castor::messages::NotifyDriveUnmountStarted body;
  
  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg();
}

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::tapeUnmounted(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  castor::messages::Header header=castor::messages::preFilleHeader();
  header.set_bodyhashvalue("PIPO");
  header.set_bodysignature("PIPO");
  header.set_reqtype(castor::messages::reqType::NotifyDriveTapeUnmounted);
  
  castor::messages::NotifyDriveTapeUnmounted body;
  
  castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
  castor::messages::sendMessage(m_socket,body);
  
  readReplyMsg();
}

//-----------------------------------------------------------------------------
// readReplyMsg
//-----------------------------------------------------------------------------
void castor::messages::TapeserverProxyZmq::readReplyMsg()  {
  
}
