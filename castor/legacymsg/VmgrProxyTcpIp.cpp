/******************************************************************************
 *                castor/legacymsg/VmgrProxyTcpIp.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/legacymsg/VmgrProxyTcpIp.hpp"
#include "castor/legacymsg/VmgrTapeMountedMsgBody.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/vmgr.h"

#include <unistd.h>
#include <sys/types.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrProxyTcpIp::VmgrProxyTcpIp(
  log::Logger &log,
  const std::string &vmgrHostName,
  const unsigned short vmgrPort,
  const int netTimeout) throw():
    m_log(log),
    m_vmgrHostName(vmgrHostName),
    m_vmgrPort(vmgrPort),
    m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrProxyTcpIp::~VmgrProxyTcpIp() throw() {
}

//-----------------------------------------------------------------------------
// readVmgrReply
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::readVmgrRcReply(const int fd)  {
  legacymsg::MessageHeader reply;

  try {
    reply = readRcReplyMsg(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read VMGR reply: " <<
      ne.getMessage().str();
    throw ex;
  }

  if(VMGR_MAGIC != reply.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read VMGR reply: Invalid magic"
      ": expected=0x" << std::hex << VMGR_MAGIC << " actual=" << reply.magic;
    throw ex;
  }
  
  if(VMGR_RC != reply.reqType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Wrong VMGR reply type"
      ": expected=0x" << std::hex << VMGR_RC << " actual=" << reply.reqType;
    throw ex;
  } 
  
  if(0 != reply.lenOrStatus) {
    // VMGR is reporting an error
    char errBuf[80];
    sstrerror_r(reply.reqType, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() << "VMGR reported an error: " << errBuf;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readRcReplyMsg
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::VmgrProxyTcpIp::readRcReplyMsg(const int fd)  {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader reply;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read reply header: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, reply);

  return reply;
}

//-----------------------------------------------------------------------------
// writeDriveStatusMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::writeTapeMountNotificationMsg(const int fd, const legacymsg::VmgrTapeMountedMsgBody &body)  {
  char buf[REQBUFSZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write mount notification message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::sendNotificationAndReceiveReply(const legacymsg::VmgrTapeMountedMsgBody &body)  {
  castor::utils::SmartFd fd(connectToVmgr());
  writeTapeMountNotificationMsg(fd.get(), body);
  readVmgrRcReply(fd.get());
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForRead(const std::string &vid) 
{
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = getuid();
    msg.gid = getgid();
    msg.jid = getpid();
    msg.mode = WRITE_DISABLE;
    castor::utils::copyString(msg.vid, vid.c_str());

    sendNotificationAndReceiveReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForWrite(const std::string &vid)
{
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = getuid();
    msg.gid = getgid();
    msg.jid = getpid();
    msg.mode = WRITE_ENABLE;
    castor::utils::copyString(msg.vid, vid.c_str());

    sendNotificationAndReceiveReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted: " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToVmgr
//-----------------------------------------------------------------------------
int castor::legacymsg::VmgrProxyTcpIp::connectToVmgr() const  {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_vmgrHostName, m_vmgrPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to vmgr on host " << m_vmgrHostName
      << " port " << m_vmgrPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//------------------------------------------------------------------------------
// marshalQueryTapeRequest
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::marshalQueryTapeRequest(const std::string &vid, legacymsg::VmgrTapeInfoRqstMsgBody &request, char *buf, size_t bufLen, size_t &totalLen) {  
  try {
    totalLen = legacymsg::marshal(buf, bufLen, request);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal request for tape information: vid=" << vid
      << ". Reason: " << ne.getMessage().str();
  }
}

//------------------------------------------------------------------------------
// sendQueryTapeRequest
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::sendQueryTapeRequest(const std::string &vid, const int fd, char *buf, size_t totalLen) {  
  try {
    io::writeBytes(fd, m_netTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send request for tape information to the VMGR: vid=" << vid
      << ". Reason: " << ne.getMessage().str();
  }
}

//------------------------------------------------------------------------------
// receiveQueryTapeReplyHeader
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::receiveQueryTapeReplyHeader(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader) {   
  const size_t bufLen = 12; // Magic + type + len
  size_t len = bufLen;
  char buf[bufLen];

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read reply header: "
      << ne.getMessage().str();
    throw ex;
  }
  
  const char *bufPtr = buf;
  legacymsg::unmarshal(bufPtr, len, replyHeader);
}

//------------------------------------------------------------------------------
// handleErrorReply
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::handleErrorReply(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader) 
{
  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[VMGR_REPLY_BUFSIZE - 3 * sizeof(uint32_t)];

  // If the error string is larger than the receive buffer
  if(replyHeader.lenOrStatus > sizeof(bodyBuf)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Error string from VMGR is larger than the receive buffer: reqType=MSG_ERR: vid=" << vid 
      << ": receive buffer size=" << sizeof(bodyBuf) << " bytes: error string size including null terminator=" << replyHeader.lenOrStatus << " bytes";
    throw ex;
  }

  // Receive the error string
  try {
    io::readBytes(fd, m_netTimeout, replyHeader.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive error string from VMGR: reqType=MSG_ERR: vid=" << vid 
      << ". Reason: " << ne.getMessage().str();
    throw ne;
  }

  // Ensure the error string is null terminated
  bodyBuf[sizeof(bodyBuf)-1] = '\0';

  // Convert the error string into an exception
  castor::exception::Exception ex;
  ex.getMessage() << "Received an error string from the VMGR: reqType=MSG_ERR: vid=" << vid 
    << ": VMGR error string=" << bodyBuf;
  throw ex;
}

//------------------------------------------------------------------------------
// handleDataReply
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::handleDataReply(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader, legacymsg::VmgrTapeInfoMsgBody &reply) 
{
  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[VMGR_REPLY_BUFSIZE - 3 * sizeof(uint32_t)];

  // If the message body is larger than the receive buffer
  if(replyHeader.lenOrStatus > sizeof(bodyBuf)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Message body from VMGR is larger than the receive buffer: reqType=MSG_DATA: vid=" << vid 
      << ": receive buffer size=" << sizeof(bodyBuf) << " bytes: error string size including null terminator=" << replyHeader.lenOrStatus << " bytes";
    throw ex;
  }

  // Receive the message body
  try {
    io::readBytes(fd, m_netTimeout, replyHeader.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive message body from VMGR: reqType=MSG_DATA: vid=" << vid 
      << ". Reason: " << ne.getMessage().str();
    throw ex;
  }

  // Unmarshal the message body
  try {
    const char *bufPtr = bodyBuf;
    size_t len = replyHeader.lenOrStatus;
    legacymsg::unmarshal(bufPtr, len, reply);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to unmarshal message body from VMGR: reqType=MSG_DATA: vid=" << vid
      << ". Reason: "<< ne.getMessage().str();
    throw ex;
  }

  log::Param params[] = {log::Param("vid", vid)};
  m_log(LOG_INFO, "Received tape information from the VMGR", params);
}

//------------------------------------------------------------------------------
// queryTape
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::queryTape(const std::string &vid, legacymsg::VmgrTapeInfoMsgBody &reply) {
  try {
    
    castor::utils::SmartFd fd(connectToVmgr());
    
    legacymsg::VmgrTapeInfoRqstMsgBody request;    
    request.uid = getuid();
    request.gid = getgid();
    castor::utils::copyString(request.vid, vid.c_str());
    request.side = 0; // HARDCODED side

    char buf[VMGR_REQUEST_BUFSIZE];
    size_t totalLen = 0;
    marshalQueryTapeRequest(vid, request, buf, VMGR_REQUEST_BUFSIZE, totalLen);
    sendQueryTapeRequest(vid, fd.get(), buf, totalLen);
    legacymsg::MessageHeader replyHeader;
    receiveQueryTapeReplyHeader(vid, fd.get(), replyHeader);

    switch(replyHeader.reqType) {

    // The VMGR wishes to keep the connection open - this is unexpected
    case VMGR_IRC:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "VMGR unexpectedly wishes to keep the connection open: reqType=VMGR_IRC: vid=" << vid;
        throw ex;
      }
      break;
      
    // The VMGR has returned an error code
    case VMGR_RC:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an error code from the VMGR: reqType=VMGR_RC: vid=" << vid << ": VMGR error code=" << replyHeader.lenOrStatus;
        throw ex;
      }
      break;

    // The VMGR has returned an error string
    case MSG_ERR:
      handleErrorReply(vid, fd.get(), replyHeader);
      break;    

    // The VMGR returned the tape information
    case MSG_DATA:
      handleDataReply(vid, fd.get(), replyHeader, reply);
      break;

    // The VMGR returned an unknown message type
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an unkown message type from the VMGR: reqType=" << replyHeader.reqType << ": vid=" << vid;
        throw ex;
      }
      break;
    }    
    
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to query the VMGR for tape " << vid <<
      " Reason: " << ne.getMessage().str();
    throw ex;
  }
}
