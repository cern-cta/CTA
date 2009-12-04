/******************************************************************************
 *                      castor/tape/aggregator/RtcpJobSubmitter.cpp
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
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/RtcpJobSubmitter.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/net.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// submit
//------------------------------------------------------------------------------
void castor::tape::aggregator::RtcpJobSubmitter::submit(
  const std::string              &host,
  const unsigned int             port,
  const int                      netReadWriteTimeout,
  const char                     *remoteCopyType,
  const u_signed64               volReqId,
  const std::string              &clientUserName,
  const std::string              &clientHost,
  const int                      clientPort,
  const int                      clientEuid,
  const int                      clientEgid,
  const std::string              &deviceGroupName,
  const std::string              &driveUnit,
  legacymsg::RtcpJobReplyMsgBody &reply)
  throw(castor::exception::Exception) {

  legacymsg::RtcpJobRqstMsgBody request;

  // Check the validity of the parameters of this function
  if(clientHost.length() > sizeof(request.clientHost) - 1) {
    TAPE_THROW_CODE(EINVAL,
      ": Length of clientHost string is too large"
      ": Maximum: " << (sizeof(request.clientHost) - 1) << 
      ": Actual: " << clientHost.length());
  }
  if(deviceGroupName.length() > sizeof(request.deviceGroupName) - 1) {
    castor::exception::Exception ex(EINVAL);

    TAPE_THROW_CODE(EINVAL,
      ": Length of deviceGroupName string is too large"
      ": Maximum: " << (sizeof(request.deviceGroupName) - 1) <<
      ": Actual: " << deviceGroupName.length());
  }
  if(driveUnit.length() > sizeof(request.driveUnit) - 1) {
    TAPE_THROW_CODE(EINVAL,
      ": Length of driveUnit string is too large"
      ": Maximum: " << (sizeof(request.driveUnit) - 1) <<
      ": Actual: " << driveUnit.length());
  }
  if(clientUserName.length() > sizeof(request.clientUserName) - 1) {
    TAPE_THROW_CODE(EINVAL,
      ": Length of clientUserName string is too large"
      ": Maximum: " << (sizeof(request.clientUserName) - 1) <<
      ": Actual: " << clientUserName.length());
  }

  // Prepare the job submission request information ready for marshalling
  // The validity of the string length were check above
  request.volReqId   = volReqId;
  request.clientPort = clientPort;
  request.clientEuid = clientEuid;
  request.clientEgid = clientEgid;
  strcpy(request.clientHost     , clientHost.c_str());
  strcpy(request.deviceGroupName, deviceGroupName.c_str());
  strcpy(request.driveUnit      , driveUnit.c_str());
  strcpy(request.clientUserName , clientUserName.c_str());

  // Marshal the job submission request message
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;

  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to marshal RCP job submission request message"
      ": " << ex.getMessage().str());
  }

  // Connect to the RTCPD or tape aggregator daemon
  castor::io::ClientSocket sock(port, host);
  sock.setTimeout(CLIENTNETRWTIMEOUT);

  try {
    sock.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECONNABORTED,
      ": Failed to connect to " << remoteCopyType <<
      ": Host: " << host <<
      ": Port: " << port <<
      ": " << ex.getMessage().str());
  }

  // Send the job submission request message to the RTCPD or tape aggregator
  // daemon
  try {
    net::writeBytes(sock.socket(), netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to send the job submission request message to " <<
      remoteCopyType <<
      ": " << ex.getMessage().str());
  }

  // Read the reply from the RTCPD or tape aggregator daemon
  readReply(sock, netReadWriteTimeout, remoteCopyType, reply);
}


//------------------------------------------------------------------------------
// readReply
//------------------------------------------------------------------------------
void castor::tape::aggregator::RtcpJobSubmitter::readReply(
  castor::io::AbstractTCPSocket &sock, const int netReadWriteTimeout,
  const char *remoteCopyType, legacymsg::RtcpJobReplyMsgBody &reply)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytes(sock.socket(), RTCPDNETRWTIMEOUT, sizeof(headerBuf),
      headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to read message header from " << remoteCopyType <<
      ": " << ex.getMessage().str());
  }

  // Unmarshal the messager header
  legacymsg::MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to unmarshal message header from " << remoteCopyType <<
      ": " << ex.getMessage().str());
  }

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC && header.magic != RTCOPY_MAGIC_OLD0) {
    TAPE_THROW_CODE(EBADMSG,
       std::hex <<
       ": Invalid magic number from " << remoteCopyType <<
       ": Expected: 0x" << RTCOPY_MAGIC << " or 0x" << RTCOPY_MAGIC_OLD0 <<
       ": Received: 0x" << header.magic);
  }

  // If the request type is invalid
  if(header.reqType != VDQM_CLIENTINFO) {
    TAPE_THROW_CODE(EBADMSG,
      std::hex <<
      ": Invalid request type from " << remoteCopyType <<
      ": Expected: 0x" << VDQM_CLIENTINFO <<
      ": Received: 0x" << header.reqType);
  }

  // If the message body is not large enough for a status number and an empty
  // error string
  {
    const size_t minimumLen = sizeof(uint32_t) + 1;
    if(header.lenOrStatus < minimumLen) {
      TAPE_THROW_CODE(EMSGSIZE,
        ": Invalid header from " << remoteCopyType <<
        ": Message body not large enough for a status number and an empty "
        "string: Minimum size expected: " << minimumLen <<
        ": Received: " << header.lenOrStatus);
    }
  }

  // Length of message header = 3 * sizeof(uint32_t)
  char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

  // If the message body is larger than the message body buffer
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Message body from " <<  remoteCopyType << " is too large" <<
      ": Maximum: " << sizeof(bodyBuf) <<
      ": Received: " << header.lenOrStatus);
  }

  // Read the message body
  try {
    net::readBytes(sock.socket(), RTCPDNETRWTIMEOUT, header.lenOrStatus,
      bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to read message body from " << remoteCopyType <<
      ": " << ex.getMessage().str());
  }

  // Unmarshal the job submission reply
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    legacymsg::unmarshal(p, remainingLen, reply);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unmarshal job submission reply"
      ": " << ex.getMessage().str());
  }
}
