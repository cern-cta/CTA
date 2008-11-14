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
#include "h/net.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// submit
//------------------------------------------------------------------------------
void castor::tape::aggregator::RCPJobSubmitter::submit(
  const std::string &host,
  const unsigned     port,
  const char        *remoteCopyType,
  const u_signed64   tapeRequestID,
  const std::string &clientUserName,
  const std::string &clientHost,
  const int          clientPort,
  const int          clientEuid,
  const int          clientEgid,
  const std::string &deviceGroupName,
  const std::string &tapeDriveName)
  throw (castor::exception::Exception) {

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
  int len =
    4*sizeof(uint32_t)       +
    clientHost.length()      +
    deviceGroupName.length() +
    tapeDriveName.length()   +
    clientUserName.length()  +
    4; // 4 = the number of string termination characters

  char buf[MSGBUFSIZ];
  char *p = buf;

  p = Marshaller::marshallUint32(p, RTCOPY_MAGIC_OLD0); // Magic number
  p = Marshaller::marshallUint32(p, VDQM_CLIENTINFO); // Request type
  p = Marshaller::marshallUint32(p, len);

  p = Marshaller::marshallUint32(p, tapeRequestID); // Downcast to 32-bits
  p = Marshaller::marshallUint32(p, clientPort);
  p = Marshaller::marshallUint32(p, clientEuid);
  p = Marshaller::marshallUint32(p, clientEgid);
  p = Marshaller::marshallString(p, clientHost);
  p = Marshaller::marshallString(p, deviceGroupName);
  p = Marshaller::marshallString(p, tapeDriveName);
  p = Marshaller::marshallString(p, clientUserName);

  // Calculate the length of header + body ready for netwrite
  len += 3 * sizeof(uint32_t);

  // After marshalling we can send the information to the RTCPD or tape
  // aggregator daemon
  int rc = netwrite_timeout(socket.socket(), buf, len, NETRW_TIMEOUT);

  if (rc == -1) {
    serrno = SECOMERR;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ) to " << remoteCopyType << ": " << neterror();
    throw ex;
  }
  else if (rc == 0) {
    serrno = SECONNDROP;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ) to " << remoteCopyType << ": connection dropped";
    throw ex;
  }

  readReply(socket, remoteCopyType);
}


//------------------------------------------------------------------------------
// readReply
//------------------------------------------------------------------------------
void castor::tape::aggregator::RCPJobSubmitter::readReply(
  castor::io::AbstractTCPSocket &socket, const char *remoteCopyType)
  throw (castor::exception::Exception) {

  char buf[MSGBUFSIZ];

  int rc = netread_timeout(socket.socket(), buf, sizeof(uint32_t)*3,
    NETRW_TIMEOUT);

  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << __PRETTY_FUNCTION__
        << ": netread(HDR) from " << remoteCopyType << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << __PRETTY_FUNCTION__
        << ": netread(HDR) from " << remoteCopyType << ": connection dropped";
      throw ex;
    }
  }

  char *p = buf;

  // Unmarshall the magic number
  uint32_t magic = 0;
  p = Marshaller::unmarshallUint32(p, magic);

  // If the magic number is invalid
  if(magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": invalid header from " << remoteCopyType
       << ": invalid magic number: expected: 0x" << RTCOPY_MAGIC << " or 0x"
       << RTCOPY_MAGIC_OLD0 << ": received: 0x" << magic;

     throw ex;
  }

  // Unmarshall the request type
  uint32_t reqtype = 0;
  p = Marshaller::unmarshallUint32(p, reqtype);

  // If the request type is invalid
  if(reqtype != VDQM_CLIENTINFO) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": invalid header from " << remoteCopyType
      << ": invalid request type: expected: 0x" << VDQM_CLIENTINFO
      << ": received 0x" << reqtype;

    throw ex;
  }

  // Unmarshall the length of the message body
  uint32_t len = 0;
  p = Marshaller::unmarshallUint32(p, len);

  // If the message body is not large enough for a status number and an empty
  // error string
  if(len < sizeof(uint32_t) + 1) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": invalid header from " << remoteCopyType
      << ": message body not large enough for a status number and an empty "
      "string: minimum size expected: " << (sizeof(uint32_t) + 1)
      << ": received: " << len;

    throw ex;
  }

  // If the message body is too large
  if(len > MSGBUFSIZ - 3*sizeof(uint32_t) ) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": invalid header from " << remoteCopyType
      << ": message body too large: maximum length: "
      << (MSGBUFSIZ-3*sizeof(uint32_t)) << " actual length: " << len;

    throw ex;
  }

  rc = netread_timeout(socket.socket(), p, len, NETRW_TIMEOUT);
  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << __PRETTY_FUNCTION__
        << ": netread(HDR) from " << remoteCopyType << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
        ex.getMessage() << __PRETTY_FUNCTION__
        << ": netread(HDR) from " << remoteCopyType << ": connection dropped";
      throw ex;
    }
  }

  // Unmarshall the status number
  uint32_t status = 0;
  p = Marshaller::unmarshallUint32(p, status);

  // The size of the received error message is the size of the mesaage body
  // minus the size of the status number
  const size_t receivedErrMsgSize = len - sizeof(uint32_t);

  // Unmarshall the error message
  char errMsg[1024];
  const size_t errMsgSize = receivedErrMsgSize < sizeof(errMsg) ?
    receivedErrMsgSize : sizeof(errMsg);
  errMsg[0] = '\0';
  strncpy(errMsg, p, errMsgSize);
  errMsg[errMsgSize - 1] = '\0';

  // If RTCOPY or tape aggregator daemon returned an error message
  if(status != 0 || errMsgSize > 1) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": " << remoteCopyType << " returned an error message: status: "
      << status << ": error message: " << errMsg;

    throw ex;
  }
}
