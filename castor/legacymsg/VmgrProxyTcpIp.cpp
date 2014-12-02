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
  const std::string &vmgrHostName,
  const unsigned short vmgrPort,
  const int netTimeout) throw():
    m_vmgrHostName(vmgrHostName),
    m_vmgrPort(vmgrPort),
    m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrProxyTcpIp::~VmgrProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForRead(
  const std::string &vid, const uint32_t jid) {
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = geteuid();
    msg.gid = getegid();
    msg.jid = jid;
    msg.mode = WRITE_DISABLE;
    castor::utils::copyString(msg.vid, vid);

    connectWriteNotificationAndReadReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted for READ by jid " << jid << " : " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForWrite(
  const std::string &vid, const uint32_t jid) {
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = geteuid();
    msg.gid = getegid();
    msg.jid = jid;
    msg.mode = WRITE_ENABLE;
    castor::utils::copyString(msg.vid, vid);

    connectWriteNotificationAndReadReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted for WRITE by jid " << jid << " : " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// connectWriteNotificationAndReadReply
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::connectWriteNotificationAndReadReply(
  const VmgrTapeMountedMsgBody &body) const {
  castor::utils::SmartFd fd(connectToVmgr());
  writeTapeMountNotificationMsg(fd.get(), body);
  readVmgrRcReply(fd.get());
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

//-----------------------------------------------------------------------------
// writeTapeMountNotificationMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::writeTapeMountNotificationMsg(
  const int fd, const VmgrTapeMountedMsgBody &body) const {
  char buf[REQBUFSZ];
  const size_t len = marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write mount notification message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readVmgrRcReply
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::readVmgrRcReply(const int fd) const {
  MessageHeader reply;

  try {
    reply = readMsgHeader(fd);
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
    const std::string errStr = utils::serrnoToString(reply.reqType);
    castor::exception::Exception ex;
    ex.getMessage() << "VMGR reported an error: " << errStr;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader
  castor::legacymsg::VmgrProxyTcpIp::readMsgHeader(const int fd) const {
  char buf[3 * sizeof(uint32_t)]; // Magic + type + len

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      << ne.getMessage().str();
    throw ex;
  }

  MessageHeader header;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  unmarshal(bufPtr, bufLen, header);
  return header;
}

//------------------------------------------------------------------------------
// queryTape
//------------------------------------------------------------------------------
castor::legacymsg::VmgrTapeInfoMsgBody
  castor::legacymsg::VmgrProxyTcpIp::queryTape(const std::string &vid) {
  try {
    castor::utils::SmartFd fd(connectToVmgr());

    VmgrTapeInfoRqstMsgBody request;
    request.uid = geteuid();
    request.gid = getegid();
    castor::utils::copyString(request.vid, vid);
    request.side = 0; // HARDCODED side

    char buf[VMGR_REQUEST_BUFSIZE];
    const size_t totalLen = marshal(buf, sizeof(buf), request);
    io::writeBytes(fd.get(), m_netTimeout, totalLen, buf);

    MessageHeader replyHeader = readMsgHeader(fd.get());
    if(VMGR_MAGIC != replyHeader.magic) {
      castor::exception::Exception ex;
      ex.getMessage() << "Reply header contains invalid magic"
        ": expected=0x" << std::hex << VMGR_MAGIC << " actual=" <<
        replyHeader.magic;
      throw ex;
    }

    switch(replyHeader.reqType) {

    // The VMGR wishes to keep the connection open - this is unexpected
    case VMGR_IRC:
      {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "VMGR unexpectedly wishes to keep the connection open"
          ": reqType=VMGR_IRC";
        throw ex;
      }

    // The VMGR has returned an error code
    case VMGR_RC:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an error code from the VMGR"
          ": reqType=VMGR_RC errorCode=" << replyHeader.lenOrStatus;
        throw ex;
      }

    // The VMGR has returned an error string
    case MSG_ERR:
      throw readErrReply(fd.get(), replyHeader);

    // The VMGR returned the tape information
    case MSG_DATA:
      {
        VmgrTapeInfoMsgBody reply =
          readDataReply<VmgrTapeInfoMsgBody>(fd.get(), replyHeader);
        return reply;
      }

    // The VMGR returned an unknown message type
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an unkown message type from the VMGR"
          ": reqType=" << replyHeader.reqType;
        throw ex;
      }
    }

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to query the VMGR for tape " << vid <<
      ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// queryPool
//------------------------------------------------------------------------------
castor::legacymsg::VmgrPoolInfoMsgBody
  castor::legacymsg::VmgrProxyTcpIp::queryPool(const std::string &poolName) {
  try {
    castor::utils::SmartFd fd(connectToVmgr());

    VmgrQryPoolMsgBody request;
    request.uid = geteuid();
    request.gid = getegid();
    castor::utils::copyString(request.poolName, poolName);

    char buf[VMGR_REQUEST_BUFSIZE];
    const size_t totalLen = marshal(buf, sizeof(buf), request);
    io::writeBytes(fd.get(), m_netTimeout, totalLen, buf);

    MessageHeader replyHeader = readMsgHeader(fd.get());
    if(VMGR_MAGIC != replyHeader.magic) {
      castor::exception::Exception ex;
      ex.getMessage() << "Reply header contains invalid magic"
        ": expected=0x" << std::hex << VMGR_MAGIC << " actual=" <<
        replyHeader.magic;
      throw ex;
    }

    switch(replyHeader.reqType) {

    // The VMGR wishes to keep the connection open - this is unexpected
    case VMGR_IRC:
      {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "VMGR unexpectedly wishes to keep the connection open"
          ": reqType=VMGR_IRC";
        throw ex;
      }

    // The VMGR has returned an error code
    case VMGR_RC:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an error code from the VMGR"
          ": reqType=VMGR_RC errorCode=" << replyHeader.lenOrStatus;
        throw ex;
      }

    // The VMGR has returned an error string
    case MSG_ERR:
      throw readErrReply(fd.get(), replyHeader);

    // The VMGR returned the tape information
    case MSG_DATA:
      {
        const VmgrPoolInfoMsgBody reply =
          readDataReply<VmgrPoolInfoMsgBody>(fd.get(), replyHeader);
        return reply;
      }

    // The VMGR returned an unknown message type
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Received an unkown message type from the VMGR"
          ": reqType=" << replyHeader.reqType;
        throw ex;
      }
    }

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to query the VMGR for pool " << poolName <<
      ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readErrReply
//------------------------------------------------------------------------------
castor::exception::Exception castor::legacymsg::VmgrProxyTcpIp::readErrReply(
  const int fd, const MessageHeader &replyHeader) const {

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[VMGR_REPLY_BUFSIZE - 3 * sizeof(uint32_t)];

  // If the error string is larger than the receive buffer
  if(replyHeader.lenOrStatus > sizeof(bodyBuf)) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Error string from VMGR is larger than the receive buffer"
      ": reqType=MSG_ERR receiveBufferSize=" << sizeof(bodyBuf) << " bytes"
      " errorStringSizeIncludingNullTerminator=" << replyHeader.lenOrStatus <<
      " bytes";
    throw ex;
  }

  // Receive the error string
  try {
    io::readBytes(fd, m_netTimeout, replyHeader.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive error string from VMGR"
     ": reqType=MSG_ERR :" << ne.getMessage().str();
    throw ex;
  }

  // Ensure the error string is null terminated
  bodyBuf[sizeof(bodyBuf)-1] = '\0';

  // Convert the error string into an exception
  castor::exception::Exception ex;
  ex.getMessage() << "Received an error string from the VMGR"
    ": reqType=MSG_ERR: " << bodyBuf;
  return ex;
}
