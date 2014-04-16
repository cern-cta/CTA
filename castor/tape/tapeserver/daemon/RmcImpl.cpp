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
  ScsiLibraryDriveName parsedDrive;
  try {
    parsedDrive = ScsiLibraryDriveName(drive);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to mount tape in SCSI library: " << ne.getMessage().str();
    throw ex;
  }

  castor::utils::SmartFd fd(connectToRmc(parsedDrive.rmcHostName));
  legacymsg::RmcMountMsgBody body;
  body.uid = m_uid;
  body.gid = m_gid;
  castor::utils::copyString(body.vid, vid.c_str());
  body.drvOrd = parsedDrive.drvOrd;
  writeRmcMountMsg(fd.get(), body);
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
void castor::tape::tapeserver::daemon::RmcImpl::writeRmcMountMsg(const int fd, const legacymsg::RmcMountMsgBody &body) throw(castor::exception::Exception) {
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
