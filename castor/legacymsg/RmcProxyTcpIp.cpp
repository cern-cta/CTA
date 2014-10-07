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
#include "castor/mediachanger/ScsiLibrarySlot.hpp"
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
castor::legacymsg::RmcProxyTcpIp::RmcProxyTcpIp(log::Logger &log,
  const unsigned short rmcPort, const int netTimeout) throw():
  m_log(log),
  m_rmcPort(rmcPort),
  m_netTimeout(netTimeout) {
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
  const std::string &vid, const mediachanger::ConfigLibrarySlot &librarySlot) {
  // SCSI does not support read-only mounts
  mountTapeReadWrite(vid, librarySlot);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeReadWrite(
  const std::string &vid, const mediachanger::ConfigLibrarySlot &librarySlot) {
  try {
    // Dispatch the appropriate helper method depending on library slot type
    switch(librarySlot.getLibraryType()) {
    case mediachanger::TAPE_LIBRARY_TYPE_ACS:
      mountTapeAcs(vid, librarySlot.str());
      break;
    case mediachanger::TAPE_LIBRARY_TYPE_MANUAL:
      mountTapeManual(vid);
      break;
    case mediachanger::TAPE_LIBRARY_TYPE_SCSI:
      mountTapeScsi(vid, librarySlot.str());
      break;
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() << "Unexpected library type"
          ": vid=" << vid << " librarySlot=" << librarySlot.str();
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to mount tape in SCSI library for read/write access:" <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeAcs
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeAcs(const std::string &vid,
  const std::string &librarySlot) {
}

//------------------------------------------------------------------------------
// mountTapeManual
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeManual(const std::string &vid) {
}

//------------------------------------------------------------------------------
// mountTapeScsi
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::mountTapeScsi(const std::string &vid,
  const std::string &librarySlot) {
  std::ostringstream task;
  task << "mount tape " << vid << " in " << librarySlot;

  try {
    const mediachanger::ScsiLibrarySlot parsedSlot(librarySlot);

    castor::utils::SmartFd fd(connectToRmc(parsedSlot.getRmcHostName()));

    RmcMountMsgBody body;
    body.uid = geteuid();
    body.gid = getegid();
    castor::utils::copyString(body.vid, vid);
    body.drvOrd = parsedSlot.getDrvOrd();
    writeRmcMountMsg(fd.get(), body);

    const MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Exception ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          parsedSlot.getRmcHostName() << ": code=" << header.lenOrStatus;
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
        castor::exception::Exception ex;
        ex.getMessage() << "Received error message from rmc running on " <<
          parsedSlot.getRmcHostName() << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << parsedSlot.getRmcHostName() <<
          " has an unexpected request type"
          ": reqType=0x" << header.reqType;
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::dismountTape(const std::string &vid,
  const mediachanger::ConfigLibrarySlot &librarySlot) {
  // Dispatch the appropriate helper method depending on library slot type
  switch(librarySlot.getLibraryType()) {
  case mediachanger::TAPE_LIBRARY_TYPE_ACS:
    unmountTapeAcs(vid, librarySlot.str());
    break;
  case mediachanger::TAPE_LIBRARY_TYPE_MANUAL:
    unmountTapeManual(vid);
    break;
  case mediachanger::TAPE_LIBRARY_TYPE_SCSI:
    unmountTapeScsi(vid, librarySlot.str());
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to unmount tape: Unexpected library slot type"
        ": vid=" << vid << " librarySlot=" << librarySlot.str();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// unmountTapeAcs
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeAcs(const std::string &vid,
  const std::string &librarySlot) {
}

//------------------------------------------------------------------------------
// unmountTapeManual
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeManual(
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// unmountTapeScsi
//------------------------------------------------------------------------------
void castor::legacymsg::RmcProxyTcpIp::unmountTapeScsi(const std::string &vid,
  const std::string &librarySlot) {
  std::ostringstream task;
  task << "unmount tape " << vid << " from " << librarySlot;

  try {
    const mediachanger::ScsiLibrarySlot parsedSlot(librarySlot);

    castor::utils::SmartFd fd(connectToRmc(parsedSlot.getRmcHostName()));

    RmcUnmountMsgBody body;
    body.uid = geteuid();
    body.gid = getegid();
    castor::utils::copyString(body.vid, vid);
    body.drvOrd = parsedSlot.getDrvOrd();
    body.force = 0;
    writeRmcUnmountMsg(fd.get(), body);

    const MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Exception ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          parsedSlot.getRmcHostName() << ": code=" << header.lenOrStatus;
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
        castor::exception::Exception ex;
        ex.getMessage() << "Received error message from rmc running on " <<
          parsedSlot.getRmcHostName() << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << parsedSlot.getRmcHostName() <<
          " has an unexpected request type"
          ": reqType=0x" << header.reqType;
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToRmc
//-----------------------------------------------------------------------------
int castor::legacymsg::RmcProxyTcpIp::connectToRmc(const std::string &hostName)
  const {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(hostName, m_rmcPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to rmc on host " << hostName
      << " port " << RMC_PORT << ": " << ne.getMessage().str();
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
