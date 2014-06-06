/******************************************************************************
 *                castor/legacymsg/TapeserverProxyTcpIp.cpp
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
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/GenericReplyMsgBody.hpp"
#include "castor/legacymsg/GenericMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/legacymsg/TapeserverProxyTcpIp.hpp"
#include "castor/legacymsg/legacymsg.hpp"
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
castor::legacymsg::TapeserverProxyTcpIp::TapeserverProxyTcpIp(log::Logger &log, const unsigned short tapeserverPort, const int netTimeout) throw():
  m_log(log),
  m_tapeserverHostName("localhost"),
  m_tapeserverPort(tapeserverPort),
  m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeserverProxyTcpIp::~TapeserverProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// updateDrive
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::updateDriveInfo(
  const castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event,
  const castor::tape::tapegateway::VolumeMode mode,
  const castor::tape::tapegateway::ClientType clientType,
  const std::string &unitName,
  const std::string &vid)
{    
  legacymsg::TapeUpdateDriveRqstMsgBody body;
  try {    
    switch(clientType) {
      case castor::tape::tapegateway::TAPE_GATEWAY:
        body.clientType=castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_GATEWAY;
        break;
      case castor::tape::tapegateway::READ_TP:
        body.clientType=castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_READTP;
        break;
      case castor::tape::tapegateway::WRITE_TP:
        body.clientType=castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_WRITETP;
        break;
      case castor::tape::tapegateway::DUMP_TP:
        body.clientType=castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_DUMPTP;
        break;
      default:
        body.clientType=castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_NONE;
        break;
    }
    
    switch(mode) {
      case castor::tape::tapegateway::READ:
        body.mode=castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_READ;
        break;
      case castor::tape::tapegateway::WRITE:
        body.mode=castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_READWRITE;
        break;
      case castor::tape::tapegateway::DUMP:
        body.mode=castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_DUMP;
        break;
      default:
        body.mode=castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_NONE;
        break;
    }
    
    body.event=event;
    castor::utils::copyString(body.vid, vid.c_str());
    castor::utils::copyString(body.drive, unitName.c_str()); 
    castor::utils::SmartFd fd(connectToTapeserver());
    writeTapeUpdateDriveRqstMsg(fd.get(), body);
    readReplyMsg(fd.get());  
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify tapeserverd of drive info update: "
        << " event = "<< body.event 
        << " mode = " << body.mode 
        << " clientType = " << body.clientType
        << " unitName = " << body.drive
        << " vid = " << body.vid
        << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotReadMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::gotReadMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_BEFORE_MOUNT_STARTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//------------------------------------------------------------------------------
// gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------
uint64_t
  castor::legacymsg::TapeserverProxyTcpIp::gotWriteMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_BEFORE_MOUNT_STARTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);

  return 0; // TO BE DONE
}

//------------------------------------------------------------------------------
// gotDumpMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::gotDumpMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_BEFORE_MOUNT_STARTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeMountedForRead(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_MOUNTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeMountedForWrite(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_MOUNTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//------------------------------------------------------------------------------
// tapeUnmounting
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeUnmounting(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_UNMOUNT_STARTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeUnmounted(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {  
  updateDriveInfo(
    legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_UNMOUNTED, 
    volInfo.volumeMode, 
    volInfo.clientType,
    unitName,
    volInfo.vid);
}

//-----------------------------------------------------------------------------
// connectToTapeserver
//-----------------------------------------------------------------------------
int castor::legacymsg::TapeserverProxyTcpIp::connectToTapeserver() const  {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_tapeserverHostName,
      m_tapeserverPort, m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to tapeserver on host " <<
      m_tapeserverHostName << " port " << m_tapeserverPort << ": " <<
      ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeTapeUpdateDriveRqstMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::writeTapeUpdateDriveRqstMsg(
  const int fd, const legacymsg::TapeUpdateDriveRqstMsgBody &body) {
  char buf[3 * sizeof(uint32_t) + sizeof(body)]; // header + body
  const size_t len = legacymsg::marshal(buf, sizeof(buf), body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write drive status message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readReplyMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::readReplyMsg(const int fd)  {
  
  size_t headerBufLen = 12; // 12 bytes of header
  char headerBuf[12];
  memset(headerBuf, '\0', headerBufLen);
  
  try {
    castor::io::readBytes(fd, m_netTimeout, headerBufLen, headerBuf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read Tape Update Drive reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
  
  const char *headerBufPtr = headerBuf;
  castor::legacymsg::MessageHeader replyHeader;
  memset(&replyHeader, '\0', sizeof(replyHeader));
  castor::legacymsg::unmarshal(headerBufPtr, headerBufLen, replyHeader);
  
  if(MSG_DATA != replyHeader.reqType || TPMAGIC != replyHeader.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Wrong reply type or magic # in Tape Update Drive reply message. replymsg.reqType: " << replyHeader.reqType << " (expected: " << MSG_DATA << ") replymsg.magic: " << replyHeader.magic << " (expected: " << TPMAGIC << ")";
    throw ex;
  }
  
  size_t bodyBufLen = 4+CA_MAXLINELEN+1; // 4 bytes of return code + max length of error message
  char bodyBuf[4+CA_MAXLINELEN+1];
  memset(bodyBuf, '\0', bodyBufLen);
  int actualBodyLen = replyHeader.lenOrStatus;
  
  try {
    castor::io::readBytes(fd, m_netTimeout, actualBodyLen, bodyBuf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read Tape Update Drive reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
  
  const char *bodyBufPtr = bodyBuf;
  castor::legacymsg::GenericReplyMsgBody replymsg;
  memset(&replymsg, '\0', sizeof(replymsg));
  castor::legacymsg::unmarshal(bodyBufPtr, bodyBufLen, replymsg);
  
  if(0 != replymsg.status) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to update drive state: " << replymsg.errorMessage;
    throw ex;
  }
}
