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
// receiveJob
//------------------------------------------------------------------------------
castor::tape::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::VdqmImpl::receiveJob(const int connection)
  throw(castor::exception::Exception) {
  const legacymsg::MessageHeader header = receiveJobMsgHeader(connection);
  const legacymsg::RtcpJobRqstMsgBody body =
    receiveJobMsgBody(connection, header.lenOrStatus);

  return body;
}

//------------------------------------------------------------------------------
// receiveJobMsgHeader
//------------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::VdqmImpl::receiveJobMsgHeader(
    const int connection) throw(castor::exception::Exception) {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(connection, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  checkJobMsgMagic(header.magic);
  checkJobMsgReqType(header.reqType);
  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// checkJobMsgMagic
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::checkJobMsgMagic(
  const uint32_t magic) const throw(castor::exception::Exception) {
  const uint32_t expectedMagic = RTCOPY_MAGIC_OLD0;
  if(expectedMagic != magic) {
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << "Invalid vdqm job message: Invalid magic number"
      ": expected=0x" << std::hex << expectedMagic <<
      " actual: 0x" << std::hex << magic;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkJobMsgReqType
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::checkJobMsgReqType(
  const uint32_t reqType) const throw(castor::exception::Exception) {
  const uint32_t expectedReqType = VDQM_CLIENTINFO;
  if(expectedReqType != reqType) {
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << "Invalid vdqm job message: Invalid request type"
       ": expected=0x" << std::hex << expectedReqType <<
       " actual=0x" << std::hex << reqType;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// receiveJobMsgBody
//------------------------------------------------------------------------------
castor::tape::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::VdqmImpl::receiveJobMsgBody(
    const int connection, const uint32_t len)
    throw(castor::exception::Exception) {
  char buf[1024];

  checkJobMsgLen(sizeof(buf), len);
  io::readBytes(connection, m_netTimeout, len, buf);

  legacymsg::RtcpJobRqstMsgBody body;
  memset(&body, '\0', sizeof(body));
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//------------------------------------------------------------------------------
// checkJobMsgLen
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmImpl::checkJobMsgLen(
  const size_t maxLen, const size_t len) const
  throw(castor::exception::Exception) {
  if(maxLen < len) {
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << "Invalid vdqm job message"
       ": Maximum message length exceeded"
       ": maxLen=" << maxLen << " len=" << len;
    throw ex;
  }
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

  legacymsg::VdqmDrvRqstMsgBody drvRqstMsgBody;
  drvRqstMsgBody.status = status;
  castor::utils::copyString(drvRqstMsgBody.server, server.c_str());
  castor::utils::copyString(drvRqstMsgBody.drive, unitName.c_str());
  castor::utils::copyString(drvRqstMsgBody.dgn, dgn.c_str());
  char drvRqstBuf[VDQM_MSGBUFSIZ];
  const size_t drvRqstMsgLen = legacymsg::marshal(drvRqstBuf, drvRqstMsgBody);

  castor::utils::SmartFd connection(connectToVdqm());
  io::writeBytes(connection.get(), m_netTimeout, drvRqstMsgLen, drvRqstBuf);

  char ackBuf[12]; // Magic + type + status
  legacymsg::MessageHeader ackMsg;
  io::readBytes(connection.get(), m_netTimeout, sizeof(ackBuf), ackBuf);
  const char *ackBufPtr = ackBuf;
  size_t ackBufLen = sizeof(ackBuf);
  legacymsg::unmarshal(ackBufPtr, ackBufLen, ackMsg);
  {
    log::Param params[] = {
      log::Param("magic", ackMsg.magic),
      log::Param("reqType", ackMsg.reqType),
      log::Param("status", ackMsg.lenOrStatus)};
    m_log(LOG_INFO, "setTapeDriveStatus() received ack from vdqm", params);
  }
}

//-----------------------------------------------------------------------------
// connectToVdqm
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmImpl::connectToVdqm()
  const throw(castor::exception::Exception) {
  timeval connectStartTime = {0, 0};
  timeval connectEndTime   = {0, 0};
  castor::utils::getTimeOfDay(&connectStartTime, NULL);

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

  const double connectDurationSecs = castor::utils::timevalToDouble(
    castor::utils::timevalAbsDiff(connectStartTime, connectEndTime));
  {
    log::Param params[] = {
      log::Param("vdqmHostName", m_vdqmHostName),
      log::Param("vdqmPort", m_vdqmPort),
      log::Param("connectDurationSecs", connectDurationSecs)};
    m_log(LOG_INFO, "Connected to vdqm", params);
  }

  return smartConnectSock.release();
}
