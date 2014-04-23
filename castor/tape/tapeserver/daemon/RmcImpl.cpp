/******************************************************************************
 *                castor/tape/tapeserver/daemon/RmcImpl.cpp
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
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RmcMarshal.hpp"
#include "castor/tape/tapeserver/daemon/RmcImpl.hpp"
#include "castor/tape/tapeserver/daemon/ScsiLibraryDriveName.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/rmc_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::RmcImpl::RmcImpl(log::Logger &log, const int netTimeout) throw():
    m_uid(getuid()),
    m_gid(getgid()),
    m_log(log),
    m_netTimeout(netTimeout) {
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::RmcImpl::~RmcImpl() throw() {
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::mountTape(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
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
  if(drive.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to mount tape: drive is an empty string"
      ": vid=" << vid;
    throw ex;
  }

  // Dispatch the appropriate helper method depending on drive type
  switch(getDriveType(drive)) {
  case RMC_DRIVE_TYPE_ACS:
    mountTapeAcs(vid, drive);
    break;
  case RMC_DRIVE_TYPE_MANUAL:
    mountTapeManual(vid);
    break;
  case RMC_DRIVE_TYPE_SCSI:
    mountTapeScsi(vid, drive);
    break;
  default:
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to mount tape: Unknown drive type"
        ": vid=" << vid << " drive=" << drive;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// getDriveType
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::RmcImpl::RmcDriveType castor::tape::tapeserver::daemon::RmcImpl::getDriveType(const std::string &drive) throw() {
  if(0 == drive.find("acs@")) {
    return RMC_DRIVE_TYPE_ACS;
  } else if(0 == drive.find("manual")) {
    return RMC_DRIVE_TYPE_MANUAL;
  } else if(0 == drive.find("smc@")) {
    return RMC_DRIVE_TYPE_SCSI;
  } else {
    return RMC_DRIVE_TYPE_UNKNOWN;
  }
}

//------------------------------------------------------------------------------
// mountTapeAcs
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::mountTapeAcs(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// mountTapeManual
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::mountTapeManual(const std::string &vid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// mountTapeScsi
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::mountTapeScsi(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
  std::ostringstream task;
  task << "mount tape " << vid << " in " << drive;

  try {
    const ScsiLibraryDriveName libraryDriveName(drive);

    castor::utils::SmartFd fd(connectToRmc(libraryDriveName.rmcHostName));

    legacymsg::RmcMountMsgBody body;
    body.uid = m_uid;
    body.gid = m_gid;
    castor::utils::copyString(body.vid, vid.c_str());
    body.drvOrd = libraryDriveName.drvOrd;
    writeRmcMountMsg(fd.get(), body);

    const legacymsg::MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Internal ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          libraryDriveName.rmcHostName << ": code=" << header.lenOrStatus;
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
          libraryDriveName.rmcHostName << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << libraryDriveName.rmcHostName <<
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
void castor::tape::tapeserver::daemon::RmcImpl::unmountTape(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
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
  if(drive.empty()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to unmount tape: drive is an empty string"
      ": vid=" << vid;
    throw ex;
  }

  // Dispatch the appropriate helper method depending on drive type
  switch(getDriveType(drive)) {
  case RMC_DRIVE_TYPE_ACS:
    unmountTapeAcs(vid, drive);
    break;
  case RMC_DRIVE_TYPE_MANUAL:
    unmountTapeManual(vid);
    break;
  case RMC_DRIVE_TYPE_SCSI:
    unmountTapeScsi(vid, drive);
    break;
  default:
    {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to unmount tape: Unknown drive type"
        ": vid=" << vid << " drive=" << drive;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// unmountTapeAcs
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::unmountTapeAcs(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// unmountTapeManual
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::unmountTapeManual(const std::string &vid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// unmountTapeScsi
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::RmcImpl::unmountTapeScsi(const std::string &vid, const std::string &drive) throw(castor::exception::Exception) {
  std::ostringstream task;
  task << "unmount tape " << vid << " from " << drive;

  try {
    const ScsiLibraryDriveName libraryDriveName(drive);

    castor::utils::SmartFd fd(connectToRmc(libraryDriveName.rmcHostName));

    legacymsg::RmcUnmountMsgBody body;
    body.uid = m_uid;
    body.gid = m_gid;
    castor::utils::copyString(body.vid, vid.c_str());
    body.drvOrd = libraryDriveName.drvOrd;
    body.force = 0;
    writeRmcUnmountMsg(fd.get(), body);

    const legacymsg::MessageHeader header = readRmcMsgHeader(fd.get());
    switch(header.reqType) {
    case RMC_RC:
      if(0 != header.lenOrStatus) {
        castor::exception::Internal ex;
        ex.getMessage() << "Received error code from rmc running on " <<
          libraryDriveName.rmcHostName << ": code=" << header.lenOrStatus;
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
          libraryDriveName.rmcHostName << ": " << errorBuf;
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Reply message from rmc running on " << libraryDriveName.rmcHostName <<
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
int castor::tape::tapeserver::daemon::RmcImpl::connectToRmc(const std::string &hostName) const throw(castor::exception::Exception) {
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
void castor::tape::tapeserver::daemon::RmcImpl::writeRmcMountMsg(const int fd, const legacymsg::RmcMountMsgBody &body)
  throw(castor::exception::Exception) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write RMC_MOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readRmcMsgHeader
//-----------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader castor::tape::tapeserver::daemon::RmcImpl::readRmcMsgHeader(const int fd) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader header;

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
  legacymsg::unmarshal(bufPtr, bufLen, header);

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
void castor::tape::tapeserver::daemon::RmcImpl::writeRmcUnmountMsg(const int fd, const legacymsg::RmcUnmountMsgBody &body)
  throw(castor::exception::Exception) {
  char buf[RMC_MSGBUFSIZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write RMC_UNMOUNT message: "
      << ne.getMessage().str();
    throw ex;
  }
}
