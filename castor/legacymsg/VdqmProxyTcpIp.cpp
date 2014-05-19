/******************************************************************************
 *                castor/legacymsg/VdqmProxyTcpIp.cpp
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
#include "castor/legacymsg/RtcpMarshal.hpp"
#include "castor/legacymsg/VdqmMarshal.hpp"
#include "castor/legacymsg/VdqmProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::VdqmProxyTcpIp::VdqmProxyTcpIp(log::Logger &log, const std::string &vdqmHostName, const unsigned short vdqmPort, const int netTimeout) throw():
  m_log(log),
  m_vdqmHostName(vdqmHostName),
  m_vdqmPort(vdqmPort),
  m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::VdqmProxyTcpIp::~VdqmProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// setDriveDown
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::setDriveDown(const std::string &server, const std::string &unitName, const std::string &dgn)  {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_DOWN;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set status of tape drive " << unitName <<
      " to down: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveUp
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::setDriveUp(const std::string &server, const std::string &unitName, const std::string &dgn)  {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_UP;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set status of tape drive " << unitName <<
      " to up: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// assignDrive
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid)  {
  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_UNIT_ASSIGN;
    body.volReqId = mountTransactionId;
    body.jobId = sessionPid;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to assign mount-session with process ID " <<
      sessionPid << "to tape drive " << unitName << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMounted
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::tapeMounted(const std::string &server,
  const std::string &unitName, const std::string &dgn, const std::string &vid,
  const pid_t sessionPid)  {

  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = VDQM_VOL_MOUNT;
    body.jobId = sessionPid;
    castor::utils::copyString(body.volId, vid.c_str());
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify vdqm that tape " << vid <<
      " was mounted on tape drive " << unitName << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// releaseDrive
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::releaseDrive(const std::string &server,
  const std::string &unitName, const std::string &dgn, const bool forceUnmount,
  const pid_t sessionPid)  {

  int status = VDQM_UNIT_RELEASE;
  if(forceUnmount) {
    status |= VDQM_FORCE_UNMOUNT;
  }

  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = status;
    body.jobId = sessionPid;
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to release tape drive " << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::setDriveStatus(const legacymsg::VdqmDrvRqstMsgBody &body)  {
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
int castor::legacymsg::VdqmProxyTcpIp::connectToVdqm() const  {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_vdqmHostName, m_vdqmPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to vdqm on host " << m_vdqmHostName
      << " port " << m_vdqmPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeDriveStatusMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::writeDriveStatusMsg(const int fd, const legacymsg::VdqmDrvRqstMsgBody &body)  {
  char buf[VDQM_MSGBUFSIZ];
  const size_t len = legacymsg::marshal(buf, body);

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
// readCommitAck
//-----------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyTcpIp::readCommitAck(const int fd)  {
  legacymsg::MessageHeader ack;

  try {
    ack = readAck(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: " <<
      ne.getMessage().str();
    throw ex;
  }

  if(VDQM_MAGIC != ack.magic) {
    castor::exception::Exception ex;
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
    castor::exception::Exception ex;
    ex.getMessage() << "VDQM_COMMIT ack reported an error: " << errBuf;
    throw ex;
  } else {
    // VDQM_COMMIT ack contains an invalid request type
    castor::exception::Exception ex;
    ex.getMessage() << "VDQM_COMMIT ack contains an invalid request type"
      ": reqType=" << ack.reqType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readAck
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::VdqmProxyTcpIp::readAck(const int fd)  {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader ack;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
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
castor::legacymsg::MessageHeader castor::legacymsg::VdqmProxyTcpIp::readDriveStatusMsgHeader(const int fd)  {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader header;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read header of drive status message: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(VDQM_MAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read header of drive status message"
      ": Invalid magic: expected=0x" << std::hex << VDQM_MAGIC << " actual=0x"
      << header.magic;
    throw ex;
  }
  if(VDQM_DRV_REQ != header.reqType) {
    castor::exception::Exception ex;
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
castor::legacymsg::VdqmDrvRqstMsgBody castor::legacymsg::VdqmProxyTcpIp::readDriveStatusMsgBody(const int fd, const uint32_t bodyLen)  {
  char buf[VDQM_MSGBUFSIZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of drive status message"
      ": Maximum body length exceeded: max=" << sizeof(buf) <<
      " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
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
void castor::legacymsg::VdqmProxyTcpIp::writeCommitAck(const int fd)  {
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

//-----------------------------------------------------------------------------
// tapeUnmounted
//-----------------------------------------------------------------------------
void  castor::legacymsg::VdqmProxyTcpIp::tapeUnmounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid)  {
  int status = VDQM_VOL_UNMOUNT;

  try {
    legacymsg::VdqmDrvRqstMsgBody body;
    body.status = status;
    castor::utils::copyString(body.volId, vid.c_str());
    castor::utils::copyString(body.server, server.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::copyString(body.dgn, dgn.c_str());

    setDriveStatus(body);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify vdqm that tape " << vid <<
      " was unmounted from tape drive " << unitName << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}
