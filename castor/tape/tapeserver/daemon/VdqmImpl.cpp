/******************************************************************************
 *                castor/tape/tapeserver/daemon/VdqmImpl.cpp
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
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/VdqmMarshal.hpp"
#include "castor/tape/tapeserver/daemon/VdqmImpl.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmImpl::VdqmImpl(log::Logger &log, const std::string &vdqmHostName, const unsigned short vdqmPort, const int netTimeout) throw():
    m_log(log),
    m_vdqmHostName(vdqmHostName),
    m_vdqmPort(vdqmPort),
    m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmImpl::~VdqmImpl() throw() {
}

//------------------------------------------------------------------------------
// setDriveStatusDown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::setDriveStatusDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_DOWN;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setTapeDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set drive status to down: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveStatusUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::setDriveStatusUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_UP;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setTapeDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set drive status to up: " << 
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// assignDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t childPid) throw(castor::exception::Exception) {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_ASSIGN;
    body.volReqId = mountTransactionId;
    body.jobId = childPid;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setTapeDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set drive status to up: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// releaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t childPid) throw(castor::exception::Exception) {
  int status = VDQM_UNIT_RELEASE;
  if(forceUnmount) {
    status |= VDQM_FORCE_UNMOUNT;
  }

  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = status;
    body.jobId = childPid;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setTapeDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set drive status to up: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setTapeDriveStatus
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::setTapeDriveStatus(const legacymsg::VdqmDrvRqstMsgBody &body) throw(castor::exception::Exception) {
  castor::utils::SmartFd fd(connectToVdqm());
  writeDriveStatusMsg(fd.get(), body);
  readCommitAck(fd.get());
  const legacymsg::MessageHeader header = readDriveStatusMsgHeader(fd.get());
  readDriveStatusMsgBody(fd.get(), header.lenOrStatus);
  writeCommitAck(fd.get());
}

//-----------------------------------------------------------------------------
// connectToVdqm
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmImpl::connectToVdqm() const throw(castor::exception::Exception) {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_vdqmHostName, m_vdqmPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to connect to vdqm on host " << m_vdqmHostName
      << " port " << m_vdqmPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeDriveStatusMsg
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::writeDriveStatusMsg(const int fd, const legacymsg::VdqmDrvRqstMsgBody &body) throw(castor::exception::Exception) {
  char buf[VDQM_MSGBUFSIZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write drive status message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readCommitAck
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::readCommitAck(const int fd) throw(castor::exception::Exception) {
  legacymsg::MessageHeader ack;

  try {
    ack = readAck(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: " <<
      ne.getMessage().str();
    throw ex;
  }

  if(VDQM_MAGIC != ack.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: Invalid magic"
      ": expected=0x" << std::hex << VDQM_MAGIC << " actual=" << ack.magic;
    throw ex;
  }

  if(VDQM_COMMIT == ack.reqType) {
    // Read a successful VDQM_COMMIT ack
    return;
  } else if(0 < ack.reqType) {
    // VDQM_COMMIT ack is reporting an error
    char errBuf[80];
    sstrerror_r(ack.reqType, errBuf, sizeof(errBuf));
    castor::exception::Internal ex;
    ex.getMessage() << "VDQM_COMMIT ack reported an error: " << errBuf;
    throw ex;
  } else {
    // VDQM_COMMIT ack contains an invalid request type
    castor::exception::Internal ex;
    ex.getMessage() << "VDQM_COMMIT ack contains an invalid request type"
      ": reqType=" << ack.reqType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readAck
//-----------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader castor::tape::tapeserver::daemon::VdqmImpl::readAck(const int fd) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader ack;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read ack: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, ack);

  return ack;
}

//-----------------------------------------------------------------------------
// readDriveStatusMsgHeader
//-----------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader castor::tape::tapeserver::daemon::VdqmImpl::readDriveStatusMsgHeader(const int fd) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader header;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read header of drive status message: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(VDQM_MAGIC != header.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read header of drive status message"
      ": Invalid magic: expected=0x" << std::hex << VDQM_MAGIC << " actual=0x"
      << header.magic;
    throw ex;
  }
  if(VDQM_DRV_REQ != header.reqType) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read header of drive status message"
      ": Invalid request type: expected=0x" << std::hex << VDQM_DRV_REQ <<
      " actual=0x" << header.reqType;
    throw ex;
  }

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//-----------------------------------------------------------------------------
// readDriveStatusMsgBody
//-----------------------------------------------------------------------------
castor::tape::legacymsg::VdqmDrvRqstMsgBody castor::tape::tapeserver::daemon::VdqmImpl::readDriveStatusMsgBody(const int fd, const uint32_t bodyLen) throw(castor::exception::Exception) {
  char buf[VDQM_MSGBUFSIZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of drive status message"
      ": Maximum body length exceeded: max=" << sizeof(buf) <<
      " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of drive status message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::VdqmDrvRqstMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//-----------------------------------------------------------------------------
// writeCommitAck
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::writeCommitAck(const int fd) throw(castor::exception::Exception) {
  legacymsg::MessageHeader ack;
  ack.magic = VDQM_MAGIC;
  ack.reqType = VDQM_COMMIT;
  ack.lenOrStatus = 0;

  char buf[12]; // magic + reqType + len
  legacymsg::marshal(buf, ack);

  try {
    io::writeBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write VDQM_COMMIT ack: " <<
      ne.getMessage().str();
    throw ex;
  }
}
