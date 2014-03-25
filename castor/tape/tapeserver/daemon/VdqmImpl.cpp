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
  const std::string &unitName, const std::string &dgn) 
  throw(castor::exception::Exception) {
  try {
    setTapeDriveStatus(unitName, dgn, VDQM_UNIT_DOWN);
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
  const std::string &unitName, const std::string &dgn)
  throw(castor::exception::Exception) {
  try {
    setTapeDriveStatus(unitName, dgn, VDQM_UNIT_UP);
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
  const std::string &unitName, const std::string &dgn, const int status)
  throw(castor::exception::Exception) {

  castor::utils::SmartFd connection(connectToVdqm());
}

//-----------------------------------------------------------------------------
// connectToVdqm
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmImpl::connectToVdqm()
  const throw(castor::exception::Exception) {

  // Temporarily call Cgethostbyname(), this will be replaced in the future
  // by code using getaddrinfo()
  struct hostent *const host = Cgethostbyname(m_vdqmHostName.c_str());
  if(NULL == host) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to connect to vdqm on host " << m_vdqmHostName
      << " port " << m_vdqmPort << ": Cgethostbyname() failed";
    throw ex;
  }

  struct sockaddr_in networkAddress;
  memset(&networkAddress, '\0', sizeof(networkAddress));
  memcpy(&networkAddress.sin_addr, host->h_addr, host->h_length);
  networkAddress.sin_port = htons(m_vdqmPort);
  networkAddress.sin_family = AF_INET;

  timeval connectStartTime = {0, 0};
  timeval connectEndTime   = {0, 0};
  castor::utils::getTimeOfDay(&connectStartTime, NULL);

  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(
      AF_INET,
      SOCK_STREAM,
      0, // sockProtocol
      (const struct sockaddr *)&networkAddress,
      sizeof(networkAddress),
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
