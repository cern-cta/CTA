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
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/RmcMarshal.hpp"
#include "castor/legacymsg/RmcProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/rmc_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::RmcProxyTcpIp::RmcProxyTcpIp(
  const unsigned short rmcPort,
  const int netTimeout,
  const unsigned int maxRqstAttempts) throw():
  m_rmcPort(rmcPort),
  m_netTimeout(netTimeout),
  m_maxRqstAttempts(maxRqstAttempts) {
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::RmcProxyTcpIp::~RmcProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeReadOnly(
  const std::string &vid, const mediachanger::ScsiLibrarySlot &librarySlot) {
  // SCSI libraries do not support read-only mounts
  mountTapeReadWrite(vid, librarySlot);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeReadWrite(
  const std::string &vid, const mediachanger::ScsiLibrarySlot &librarySlot) {
  try {
    RmcMountMsgBody rqstBody;
    rqstBody.uid = geteuid();
    rqstBody.gid = getegid();
    castor::utils::copyString(rqstBody.vid, vid);
    rqstBody.drvOrd = librarySlot.getDrvOrd();

    rmcSendRecvNbAttempts(m_maxRqstAttempts, librarySlot.getRmcHostName(),
      rqstBody);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to mount tape in SCSI tape-library for read/write access"
      ": vid=" << vid << " librarySlot=" << librarySlot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::dismountTape(const std::string &vid,
  const mediachanger::ScsiLibrarySlot &librarySlot) {
  try {
    RmcUnmountMsgBody rqstBody;
    rqstBody.uid = geteuid();
    rqstBody.gid = getegid();
    castor::utils::copyString(rqstBody.vid, vid);
    rqstBody.drvOrd = librarySlot.getDrvOrd();
    rqstBody.force = 0;

    rmcSendRecvNbAttempts(m_maxRqstAttempts, librarySlot.getRmcHostName(),
      rqstBody);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to dismount tape in SCSI tape-library"
      ": vid=" << vid << " librarySlot=" << librarySlot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::forceDismountTape(const std::string &vid,
  const mediachanger::ScsiLibrarySlot &librarySlot) {
  // SCSI libraries do not support forced dismounts
  dismountTape(vid, librarySlot);
}

//-----------------------------------------------------------------------------
// connectToRmc
//-----------------------------------------------------------------------------
int castor::legacymsg::RmcProxyTcpIp::connectToRmc(const std::string &rmcHost)
  const {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(rmcHost, m_rmcPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to rmcd: rmcHost=" << rmcHost
      << " rmcPort=" << RMC_PORT << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeRmcMountMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::writeRmcMountMsg(const int fd,
  const RmcMountMsgBody &body) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write RMC_SCSI_MOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readRmcMsgHeader
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader
  castor::legacymsg::RmcProxyTcpIp::readRmcMsgHeader(const int fd) {
  char buf[12]; // Magic + type + len
  MessageHeader header;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  unmarshal(bufPtr, bufLen, header);

  if(RMC_MAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      " Header contains an invalid magic number: expected=0x" << std::hex <<
      RMC_MAGIC << " actual=0x" << header.magic;
    throw ex;
  }

  return header;
}

//-----------------------------------------------------------------------------
// writeRmcUnmountMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::writeRmcUnmountMsg(const int fd,
  const RmcUnmountMsgBody &body) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write RMC_SCSI_UNMOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// rmcReplyTypeToStr
//-----------------------------------------------------------------------------
std::string castor::legacymsg::RmcProxyTcpIp::rmcReplyTypeToStr(
  const int replyType) {
  std::ostringstream oss;
  switch(replyType) {
  case RMC_RC:
    oss << "RMC_RC";
    break;
  case MSG_ERR:
    oss << "MSG_ERR";
    break;
  default:
    oss << "UNKNOWN(0x" << std::hex << replyType << ")";
  }
  return oss.str();
}

//-----------------------------------------------------------------------------
// handleMSG_ERR
//-----------------------------------------------------------------------------
std::string castor::legacymsg::RmcProxyTcpIp::handleMSG_ERR(
  const MessageHeader &header,
  const int fd) {
  char errorBuf[1024];
  const int nbBytesToRead = header.lenOrStatus > sizeof(errorBuf) ?
    sizeof(errorBuf) : header.lenOrStatus;
  io::readBytes(fd, m_netTimeout, nbBytesToRead, errorBuf);
  errorBuf[sizeof(errorBuf) - 1] = '\0';
  return errorBuf;
}
