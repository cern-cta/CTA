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
/* Temporarily include h/Cnetdb.h */
#include "h/Cnetdb.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmImpl::VdqmImpl(
  log::Logger &log,
  const std::string &vdqmHostName,
  const unsigned short vdqmPort,
  const int netTimeout) throw():
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
// setTapeDriveStatusDown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::setTapeDriveStatusDown(
  const std::string &server, const std::string &unitName,
  const std::string &dgn) throw(castor::exception::Exception) {
  try {
    setTapeDriveStatus(server, unitName, dgn, VDQM_UNIT_DOWN);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set drive status to down: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setTapeDriveStatusUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::setTapeDriveStatusUp(
  const std::string &server, const std::string &unitName,
  const std::string &dgn) throw(castor::exception::Exception) {
  try {
    setTapeDriveStatus(server, unitName, dgn, VDQM_UNIT_UP);
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
void castor::tape::tapeserver::daemon::VdqmImpl::setTapeDriveStatus(
  const std::string &server, const std::string &unitName,
  const std::string &dgn, const int status)
  throw(castor::exception::Exception) {

  castor::utils::SmartFd connection(connectToVdqm());
  writeDriveStatusMsg(connection.get(), server, unitName, dgn, status);
  readCommitAck(connection.get());
  const legacymsg::MessageHeader header =
    readDriveStatusMsgHeader(connection.get());
  readDriveStatusMsgBody(connection.get(), header.lenOrStatus);
  writeCommitAck(connection.get());
}

//-----------------------------------------------------------------------------
// connectToVdqm
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmImpl::connectToVdqm()
  const throw(castor::exception::Exception) {
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
void castor::tape::tapeserver::daemon::VdqmImpl::writeDriveStatusMsg(
  const int connection, const std::string &server, const std::string &unitName,
  const std::string &dgn, const int status)
  throw(castor::exception::Exception) {
  legacymsg::VdqmDrvRqstMsgBody drvRqstMsgBody;
  drvRqstMsgBody.status = status;
  castor::utils::copyString(drvRqstMsgBody.server, server.c_str());
  castor::utils::copyString(drvRqstMsgBody.drive, unitName.c_str());
  castor::utils::copyString(drvRqstMsgBody.dgn, dgn.c_str());
  char drvRqstBuf[VDQM_MSGBUFSIZ];
  const size_t drvRqstMsgLen = legacymsg::marshal(drvRqstBuf, drvRqstMsgBody);

  try {
    io::writeBytes(connection, m_netTimeout, drvRqstMsgLen, drvRqstBuf);
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
void castor::tape::tapeserver::daemon::VdqmImpl::readCommitAck(
  const int connection) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + status
  legacymsg::MessageHeader ackMsg;

  try {
    io::readBytes(connection, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, ackMsg);

  if(VDQM_MAGIC != ackMsg.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: Invalid magic"
      ": expected=0x" << std::hex << VDQM_MAGIC << " actual=" << ackMsg.magic;
    throw ex;
  }
  if(VDQM_COMMIT != ackMsg.reqType) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: Invalid request type"
      ": expected=0x" << std::hex << VDQM_COMMIT << " actual=0x" <<
      ackMsg.reqType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readDriveStatusMsgHeader
//-----------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::VdqmImpl::readDriveStatusMsgHeader(
  const int connection) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader header;

  try {
    io::readBytes(connection, m_netTimeout, sizeof(buf), buf);
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
castor::tape::legacymsg::VdqmDrvRqstMsgBody 
  castor::tape::tapeserver::daemon::VdqmImpl::readDriveStatusMsgBody(
  const int connection, const uint32_t bodyLen)
  throw(castor::exception::Exception) {
  char buf[VDQM_MSGBUFSIZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of drive status message"
      ": Maximum body length exceeded: max=" << VDQM_MSGBUFSIZ << " actual=" <<
      bodyLen;
    throw ex;
  }

  try {
    io::readBytes(connection, m_netTimeout, bodyLen, buf);
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
void castor::tape::tapeserver::daemon::VdqmImpl::writeCommitAck(
  const int connection) throw(castor::exception::Exception) {
  legacymsg::MessageHeader ack;
  ack.magic = VDQM_MAGIC;
  ack.reqType = VDQM_COMMIT;
  ack.lenOrStatus = 0;

  char buf[12]; // magic + reqType + len
  legacymsg::marshal(buf, ack);

  try {
    io::writeBytes(connection, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write VDQM_COMMIT ack: " <<
      ne.getMessage().str();
    throw ex;
  }
}
