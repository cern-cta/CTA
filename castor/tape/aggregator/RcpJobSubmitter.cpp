/******************************************************************************
 *                      RcpJobSubmitter.cpp
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
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/RcpJobRequestMessage.hpp"
#include "castor/tape/aggregator/RcpJobReplyMessage.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
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
void castor::tape::aggregator::RcpJobSubmitter::submit(
  const std::string  &host,
  const unsigned int  port,
  const int           netReadWriteTimeout,
  const char         *remoteCopyType,
  const u_signed64    tapeRequestId,
  const std::string  &clientUserName,
  const std::string  &clientHost,
  const int           clientPort,
  const int           clientEuid,
  const int           clientEgid,
  const std::string  &deviceGroupName,
  const std::string  &driveName,
  RcpJobReplyMessage &reply)
  throw(castor::exception::Exception) {

  RcpJobRequestMessage request;

  // Check the validity of the parameters of this function
  if(clientHost.length() > sizeof(request.clientHost) - 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Length of clientHost string is too large: "
      "Maximum: " << (sizeof(request.clientHost) - 1) << " Actual: "
      << clientHost.length();

    throw ex;
  }
  if(deviceGroupName.length() > sizeof(request.deviceGroupName) - 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Length of deviceGroupName string is too large: "
      "Maximum: " << (sizeof(request.deviceGroupName) - 1) << " Actual: "
      << deviceGroupName.length();

    throw ex;
  }
  if(driveName.length() > sizeof(request.driveName) - 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Length of driveName string is too large: "
      "Maximum: " << (sizeof(request.driveName) - 1) << " Actual: "
      << driveName.length();

    throw ex;
  }
  if(clientUserName.length() > sizeof(request.clientUserName) - 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Length of clientUserName string is too large: "
      "Maximum: " << (sizeof(request.clientUserName) - 1) << " Actual: "
      << clientUserName.length();

    throw ex;
  }

  // Prepare the job submission request information ready for marshalling
  // The validity of the string length were check above
  request.tapeRequestId = tapeRequestId;
  request.clientPort    = clientPort;
  request.clientEuid    = clientEuid;
  request.clientEgid    = clientEgid;
  strcpy(request.clientHost     , clientHost.c_str());
  strcpy(request.deviceGroupName, deviceGroupName.c_str());
  strcpy(request.driveName      , driveName.c_str());
  strcpy(request.clientUserName , clientUserName.c_str());

  // Marshall the job submission request message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  try {
    totalLen = Marshaller::marshallRcpJobRequestMessage(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall RCP job submission request message"
      << ": " << ex.getMessage().str();

    throw ie;
  }

  // Connect to the RTCPD or tape aggregator daemon
  castor::io::ClientSocket socket(port, host);

  try {
    socket.connect();
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex(ECONNABORTED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to connect to " << remoteCopyType
      << ": Host: " << host
      << ": Port: " << port
      << ": " << ex.getMessage().str();
  }

  // Send the job submission request message to the RTCPD or tape aggregator
  // daemon
  try {
    Net::writeBytes(socket.socket(), netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send the job submission request message to "
      << remoteCopyType
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Read the reply from the RTCPD or tape aggregator daemon
  readReply(socket, netReadWriteTimeout, remoteCopyType, reply);
}


//------------------------------------------------------------------------------
// readReply
//------------------------------------------------------------------------------
void castor::tape::aggregator::RcpJobSubmitter::readReply(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  const char *remoteCopyType, RcpJobReplyMessage &reply)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    Net::readBytes(socket.socket(), NETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << "Failed to read message header from " << remoteCopyType
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the messager header
  MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EBADMSG);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << "Failed to unmarshall message header from " << remoteCopyType
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC && header.magic != RTCOPY_MAGIC_OLD0) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid magic number from " << remoteCopyType
       << ": Expected: 0x" << RTCOPY_MAGIC << " or 0x"
       << RTCOPY_MAGIC_OLD0
       << ": Received: 0x" << header.magic;

     throw ex;
  }

  // If the request type is invalid
  if(header.reqtype != VDQM_CLIENTINFO) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from " << remoteCopyType
      << ": Expected: 0x" << VDQM_CLIENTINFO
      << ": Received: 0x" << header.reqtype;

    throw ex;
  }

  // If the message body is not large enough for a status number and an empty
  // error string
  {
    const size_t minimumLen = sizeof(uint32_t) + 1;
    if(header.len < minimumLen) {
      castor::exception::Exception ex(EMSGSIZE);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Invalid header from " << remoteCopyType
        << ": Message body not large enough for a status number and an empty "
           "string: Minimum size expected: " << minimumLen
        << ": Received: " << header.len;

      throw ex;
    }
  }

  // Length of message header = 3 * sizeof(uint32_t)
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is larger than the message body buffer
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from " <<  remoteCopyType << " is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body
  try {
    Net::readBytes(socket.socket(), NETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from " << remoteCopyType
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the job submission reply
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRcpJobReplyMessageBody(p, remainingLen, reply);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() <<  __PRETTY_FUNCTION__
      << ": Failed to unmarshall job submission reply: "
      << ex.getMessage().str();

    throw ie;
  }
}
