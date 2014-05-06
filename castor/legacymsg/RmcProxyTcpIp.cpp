/******************************************************************************
 *                castor/tape/tapeserver/daemon/RmcProxyTcpIp.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/RmcMarshal.hpp"
#include "castor/legacymsg/RmcProxyTcpIp.hpp"
#include "castor/legacymsg/ScsiLibrarySlot.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/rmc_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::RmcProxyTcpIp::RmcProxyTcpIp(log::Logger &log, const int netTimeout) throw():
    m_uid(getuid()),
    m_gid(getgid()),
    m_log(log),
    m_netTimeout(netTimeout) {
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::RmcProxyTcpIp::~RmcProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTape(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
  // Verify parameters
  if(vid.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to mount tape: VID is an empty string";
    throw ex;
  }
  if(CA_MAXVIDLEN < vid.length()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to mount tape: VID is too long"
      ": vid=" << vid << " maxLen=" << CA_MAXVIDLEN << " actualLen=" << vid.length();
    throw ex;
  }
  if(librarySlot.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to mount tape: librarySlot is an empty string"
      ": vid=" << vid;
    throw ex;
  }

  // Dispatch the appropriate helper method depending on library slot type
  switch(getLibrarySlotType(librarySlot)) {
  case RMC_LIBRARY_SLOT_TYPE_ACS:
    mountTapeAcs(vid, librarySlot);
    break;
  case RMC_LIBRARY_SLOT_TYPE_MANUAL:
    mountTapeManual(vid);
    break;
  case RMC_LIBRARY_SLOT_TYPE_SCSI:
    mountTapeScsi(vid, librarySlot);
    break;
  default:
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to mount tape: Unknown library slot type"
        ": vid=" << vid << " librarySlot=" << librarySlot;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getLibrarySlotType
//------------------------------------------------------------------------------
castor::legacymsg::RmcProxyTcpIp::RmcLibrarySlotType castor::legacymsg::RmcProxyTcpIp::getLibrarySlotType(const std::string &librarySlot) throw() {
  if(0 == librarySlot.find("acs@")) {
    return RMC_LIBRARY_SLOT_TYPE_ACS;
  } else if(0 == librarySlot.find("manual")) {
    return RMC_LIBRARY_SLOT_TYPE_MANUAL;
  } else if(0 == librarySlot.find("smc@")) {
    return RMC_LIBRARY_SLOT_TYPE_SCSI;
  } else {
    return RMC_LIBRARY_SLOT_TYPE_UNKNOWN;
  }
}

//------------------------------------------------------------------------------
// mountTapeAcs
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeAcs(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// mountTapeManual
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeManual(const std::string &vid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// mountTapeScsi
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeScsi(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
  std::ostringstream task;
  task << "mount tape " << vid << " in " << librarySlot;

  try {
    const ScsiLibrarySlot parsedSlot(librarySlot);

    castor::utils::SmartFd fd(connectToRmc(parsedSlot.rmcHostName));

    RmcMountMsgBody body;
    body.uid = m_uid;
    body.gid = m_gid;
    castor::utils::copyString(body.vid, vid.c_str());
    body.drvOrd = parsedSlot.drvOrd;
    writeRmcMountMsg(fd.get(), body);

    const MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Internal ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          parsedSlot.rmcHostName << ": code=" << header.lenOrStatus;
        throw ex;
      }
      break;
    case MSG_ERR:
      {
        char errorBuf[1024];
        const int nbBytesToRead = header.lenOrStatus > sizeof(errorBuf) ?
          sizeof(errorBuf) : header.lenOrStatus;
        castor::io::readBytes(fd.get(), m_netTimeout, nbBytesToRead, errorBuf);
        errorBuf[sizeof(errorBuf) - 1] = '\0';
        castor::exception::Internal ex;
        ex.getMessage() << "Received error message from rmc running on " <<
          parsedSlot.rmcHostName << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << parsedSlot.rmcHostName <<
          " has an unexpected request type"
          ": reqType=0x" << header.reqType;
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to " << task.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// unmountTape
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTape(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
  // Verify parameters
  if(vid.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to unmount tape: VID is an empty string";
    throw ex;
  }
  if(CA_MAXVIDLEN < vid.length()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to nmount tape: VID is too long"
      ": vid=" << vid << " maxLen=" << CA_MAXVIDLEN << " actualLen=" << vid.length();
    throw ex;
  }
  if(librarySlot.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to unmount tape: librarySlot is an empty string"
      ": vid=" << vid;
    throw ex;
  }

  // Dispatch the appropriate helper method depending on library slot type
  switch(getLibrarySlotType(librarySlot)) {
  case RMC_LIBRARY_SLOT_TYPE_ACS:
    unmountTapeAcs(vid, librarySlot);
    break;
  case RMC_LIBRARY_SLOT_TYPE_MANUAL:
    unmountTapeManual(vid);
    break;
  case RMC_LIBRARY_SLOT_TYPE_SCSI:
    unmountTapeScsi(vid, librarySlot);
    break;
  default:
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to unmount tape: Unknown library slot type"
        ": vid=" << vid << " librarySlot=" << librarySlot;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// unmountTapeAcs
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeAcs(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// unmountTapeManual
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeManual(const std::string &vid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// unmountTapeScsi
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeScsi(const std::string &vid, const std::string &librarySlot) throw(castor::exception::Exception) {
  std::ostringstream task;
  task << "unmount tape " << vid << " from " << librarySlot;

  try {
    const ScsiLibrarySlot parsedSlot(librarySlot);

    castor::utils::SmartFd fd(connectToRmc(parsedSlot.rmcHostName));

    RmcUnmountMsgBody body;
    body.uid = m_uid;
    body.gid = m_gid;
    castor::utils::copyString(body.vid, vid.c_str());
    body.drvOrd = parsedSlot.drvOrd;
    body.force = 0;
    writeRmcUnmountMsg(fd.get(), body);

    const MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Internal ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          parsedSlot.rmcHostName << ": code=" << header.lenOrStatus;
        throw ex;
      }
      break;
    case MSG_ERR:
      {
        char errorBuf[1024];
        const int nbBytesToRead = header.lenOrStatus > sizeof(errorBuf) ?
          sizeof(errorBuf) : header.lenOrStatus;
        castor::io::readBytes(fd.get(), m_netTimeout, nbBytesToRead, errorBuf);
        errorBuf[sizeof(errorBuf) - 1] = '\0';
        castor::exception::Internal ex;
        ex.getMessage() << "Received error message from rmc running on " <<
          parsedSlot.rmcHostName << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << parsedSlot.rmcHostName <<
          " has an unexpected request type"
          ": reqType=0x" << header.reqType;
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to " << task.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToRmc
//-----------------------------------------------------------------------------
int castor::legacymsg::RmcProxyTcpIp::connectToRmc(const std::string &hostName) const throw(castor::exception::Exception) {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(hostName, RMC_PORT, m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to connect to rmc on host " << hostName
      << " port " << RMC_PORT << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeRmcMountMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::writeRmcMountMsg(const int fd, const RmcMountMsgBody &body)
  throw(castor::exception::Exception) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write RMC_SCSI_MOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readRmcMsgHeader
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::RmcProxyTcpIp::readRmcMsgHeader(const int fd) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  MessageHeader header;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read message header: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  unmarshal(bufPtr, bufLen, header);

  if(RMC_MAGIC != header.magic) {
    castor::exception::Internal ex;
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
void castor::legacymsg::RmcProxyTcpIp::writeRmcUnmountMsg(const int fd, const RmcUnmountMsgBody &body)
  throw(castor::exception::Exception) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write RMC_SCSI_UNMOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}
