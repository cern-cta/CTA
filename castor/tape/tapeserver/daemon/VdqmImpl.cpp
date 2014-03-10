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

#include "castor/io/io.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/tapeserver/daemon/VdqmImpl.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmImpl::~VdqmImpl() throw() {
}

//------------------------------------------------------------------------------
// receiveJob
//------------------------------------------------------------------------------
castor::tape::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::VdqmImpl::receiveJob(const int connection,
    const int netTimeout) throw(castor::exception::Exception) {
  const legacymsg::MessageHeader header =
    receiveJobMsgHeader(connection, netTimeout);
  const legacymsg::RtcpJobRqstMsgBody body =
    receiveJobMsgBody(connection, netTimeout, header.lenOrStatus);

  return body;
}

//------------------------------------------------------------------------------
// receiveJobMsgHeader
//------------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::VdqmImpl::receiveJobMsgHeader(
    const int connection, const int netTimeout)
    throw(castor::exception::Exception) {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(connection, netTimeout, sizeof(buf), buf);

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
    const int connection, const int netTimeout, const uint32_t len)
    throw(castor::exception::Exception) {
  char buf[1024];

  checkJobMsgLen(sizeof(buf), len);
  io::readBytes(connection, netTimeout, len, buf);

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
