/******************************************************************************
 *                      RCPJobSubmitter.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/RCPJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
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
void castor::tape::aggregator::RCPJobSubmitter::submit(
  const std::string  &host,
  const unsigned int  port,
  const int           netReadWriteTimeout,
  const char         *remoteCopyType,
  const u_signed64    tapeRequestID,
  const std::string  &clientUserName,
  const std::string  &clientHost,
  const int           clientPort,
  const int           clientEuid,
  const int           clientEgid,
  const std::string  &deviceGroupName,
  const std::string  &tapeDriveName)
  throw (castor::tape::aggregator::exception::RTCPDErrorMessage,
    castor::exception::Exception) {

  castor::io::ClientSocket socket(port, host);

  try {
    socket.connect();
  } catch(castor::exception::Exception &connectex) {
    castor::exception::Exception ex(ECONNABORTED);

    ex.getMessage() << "Failed to connect to " << remoteCopyType
      << ": host: " << host << ": port: " << port << ": "
      << connectex.getMessage().str();
  }

  // Let's count the length of the message for the header.
  // Please notice: Normally the ID is a 64Bit number!!
  // But for historical reasons, we will do a downcast of the
  // id, which is then still unique, because a tapeRequest has
  // a very short lifetime, compared to the number of new IDs
  uint32_t len =
    4*sizeof(uint32_t)       +
    clientHost.length()      +
    deviceGroupName.length() +
    tapeDriveName.length()   +
    clientUserName.length()  +
    4; // 4 = the number of string termination characters

  // If the contents for the message are larger than the message body buffer
  if(len > MSGBUFSIZ - 3 * sizeof(uint32_t)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << "Contents for job submission message are larger than "
      "the message body buffer: Message body buffer length: "
      << (MSGBUFSIZ - 3 * sizeof(uint32_t)) << ": Contents length: " << len;

    throw ex;
  }

  char buf[MSGBUFSIZ];
  char *p = buf;

  Marshaller::marshallUint32(RTCOPY_MAGIC_OLD0, p); // Magic number
  Marshaller::marshallUint32(VDQM_CLIENTINFO  , p); // Request type
  Marshaller::marshallUint32(len              , p);
  Marshaller::marshallUint32(tapeRequestID    , p); // Downcast to 32-bits
  Marshaller::marshallUint32(clientPort       , p);
  Marshaller::marshallUint32(clientEuid       , p);
  Marshaller::marshallUint32(clientEgid       , p);
  Marshaller::marshallString(clientHost       , p);
  Marshaller::marshallString(deviceGroupName  , p);
  Marshaller::marshallString(tapeDriveName    , p);
  Marshaller::marshallString(clientUserName   , p);

  // Calculate the length of header + body ready for netwrite
  len += 3 * sizeof(uint32_t);

  // After marshalling we can send the information to the RTCPD or tape
  // aggregator daemon
  const int rc = netwrite_timeout(socket.socket(), buf, len,
    netReadWriteTimeout);

  if(rc == -1) {
    castor::exception::Exception ex(SECOMERR);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ) to " << remoteCopyType << ": " << neterror();
    throw ex;
  } else if(rc == 0) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ) to " << remoteCopyType << ": Connection dropped";
    throw ex;
  }

  readReply(socket, netReadWriteTimeout, remoteCopyType);
}


//------------------------------------------------------------------------------
// readReply
//------------------------------------------------------------------------------
void castor::tape::aggregator::RCPJobSubmitter::readReply(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  const char *remoteCopyType)
  throw (castor::tape::aggregator::exception::RTCPDErrorMessage,
    castor::exception::Exception) {

  // Read and unmarshall the magic number of the request
  uint32_t magic = 0;
  try {
    magic = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read magic number from " << remoteCopyType
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // If the magic number is invalid
  if(magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid header from " << remoteCopyType
       << ": Invalid magic number: Expected: 0x" << RTCOPY_MAGIC << " or 0x"
       << RTCOPY_MAGIC_OLD0 << ": Received: 0x" << magic;

     throw ex;
  }

  // Read and unmarshall the type of the request
  uint32_t reqtype = 0;
  try {
    reqtype = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read request type from " << remoteCopyType
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // If the request type is invalid
  if(reqtype != VDQM_CLIENTINFO) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid header from " << remoteCopyType
      << ": Invalid request type: Expected: 0x" << VDQM_CLIENTINFO
      << ": Received 0x" << reqtype;

    throw ex;
  }

  // Read and unmarshall the type of the request
  uint32_t len = 0;
  try {
    len = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body length from " << remoteCopyType
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // If the message body is not large enough for a status number and an empty
  // error string
  if(len < sizeof(uint32_t) + 1) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid header from " << remoteCopyType
      << ": Message body not large enough for a status number and an empty "
      "string: Minimum size expected: " << (sizeof(uint32_t) + 1)
      << ": Received: " << len;

    throw ex;
  }

  // Only need a buffer for the message body, the header has already been read
  // from the socket and unmarshalled
  char body[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(len > sizeof(body)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid header from " << remoteCopyType
      << ": Message body too large: Maximum length: "
      << sizeof(body) << " Actual length: " << len;

    throw ex;
  }

  // Read the message body from the socket
  try {
    SocketHelper::readBytes(socket, NETRW_TIMEOUT, len, body);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from " << remoteCopyType
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the status number
  char     *p           = body;
  size_t   remainingLen = len;
  uint32_t status       = 0;
  Marshaller::unmarshallUint32(p, remainingLen, status);

  // Unmarshall the error message
  char errMsg[1024];
  const size_t errMsgSize = remainingLen < sizeof(errMsg) ?  remainingLen :
    sizeof(errMsg);
  errMsg[0] = '\0';
  strncpy(errMsg, p, errMsgSize);
  errMsg[errMsgSize - 1] = '\0';

  // If RTCOPY or tape aggregator daemon returned an error message
  if(status != 0 || errMsgSize > 1) {
    castor::exception::Exception ex(status);

    ex.getMessage() << errMsg;

    throw ex;
  }
}
