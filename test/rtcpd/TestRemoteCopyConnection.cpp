/******************************************************************************
 *                      RemoteCopyConnection.cpp
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
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/RemoteCopyConnection.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/vdqmMacros.h"  // Needed for marshalling
#include "h/net.h"
#include "h/osdep.h" //for LONGSIZE
#include "h/rtcp_constants.h" /* Definition of RTCOPY_MAGIC   */
#include "h/serrno.h"
#include "h/vdqm_messages.h"

#include <errno.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RemoteCopyConnection::RemoteCopyConnection(int socket) throw () :
  AbstractTCPSocket(socket) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RemoteCopyConnection::RemoteCopyConnection(
  const unsigned short port, const std::string host)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(port, host, false) {
  createSocket();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::RemoteCopyConnection::RemoteCopyConnection(
  const unsigned short port, const unsigned long ip)
  throw (castor::exception::Exception) :
  AbstractTCPSocket(port, ip, false) {
    createSocket();
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::RemoteCopyConnection::~RemoteCopyConnection() throw () {
  ::shutdown(m_socket, SD_BOTH);
  this->close();
}


//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::vdqm::RemoteCopyConnection::connect()
  throw (castor::exception::Exception) {
  // Connects the socket
  if (::connect(m_socket, (struct sockaddr *)&m_saddr, sizeof(m_saddr)) < 0) {
    int tmpserrno = errno;
    int tmperrno = errno;
    if (errno != ECONNREFUSED) {
      tmpserrno = SECOMERR;
    }
    castor::exception::Exception ex(tmpserrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Unable to connect socket";
    if (m_saddr.sin_family == AF_INET) {
      unsigned long ip = m_saddr.sin_addr.s_addr;
      ex.getMessage() << " to "
                      << (ip%256) << "."
                      << ((ip >> 8)%256) << "."
                      << ((ip >> 16)%256) << "."
                      << (ip >> 24) << ":"
                      << ntohs(m_saddr.sin_port);
    }
    this->close();
    errno = tmperrno;

    throw ex;
  }
}


//------------------------------------------------------------------------------
// sendJob
//------------------------------------------------------------------------------
bool castor::vdqm::RemoteCopyConnection::sendJob(
  const Cuuid_t     &cuuid,
  const char        *remoteCopyType,
  const u_signed64   tapeRequestID,
  const std::string &clientUserName,
  const std::string &clientMachine,
  const int          clientPort,
  const int          clientEuid,
  const int          clientEgid,
  const std::string &deviceGroupName,
  const std::string &tapeDriveName)
  throw (castor::exception::Exception) {

  bool acknSucc = true; // The return value

  vdqmDrvReq_t vdqmDrvReq;
  vdqmVolReq_t vdqmVolReq;

  char buf[VDQM_MSGBUFSIZ];
  int len, rc;
  int magic = RTCOPY_MAGIC_OLD0;
  int reqtype = VDQM_CLIENTINFO;
  char* p;

  unsigned int castValue;
  int intValue;
  char* stringValue;


  // Let's count the length of the message for the header.
  // Please notice: Normally the ID is a 64Bit number!!
  // But for historical reasons, we will do a downcast of the
  // id, which is then still unique, because a tapeRequest has
  // a very short lifetime, compared to the number of new IDs
  len = 4*LONGSIZE + clientUserName.length() +
    clientMachine.length()  +
    deviceGroupName.length()  +
    tapeDriveName.length()  + 4;

  p = buf;

  DO_MARSHALL(LONG, p, magic, SendTo);
  DO_MARSHALL(LONG, p, reqtype, SendTo);
  DO_MARSHALL(LONG, p, len, SendTo);

  // We must do a downcast because of the old Protocol.
  // Of course we do later the same in case of a comparison
  castValue = (unsigned int)tapeRequestID;
  DO_MARSHALL(LONG, p, castValue, SendTo);

  intValue = clientPort;
  DO_MARSHALL(LONG, p, intValue, SendTo);

  intValue = clientEuid;
  DO_MARSHALL(LONG, p, intValue, SendTo);

  intValue = clientEgid;
  DO_MARSHALL(LONG, p, intValue, SendTo);

  stringValue = (char *)clientMachine.c_str();
  DO_MARSHALL_STRING(p, stringValue, SendTo, sizeof(vdqmVolReq.client_host));

  stringValue = (char *)deviceGroupName.c_str();
  DO_MARSHALL_STRING(p, stringValue, SendTo, sizeof(vdqmDrvReq.dgn));

  stringValue = (char *)tapeDriveName.c_str();
  DO_MARSHALL_STRING(p, stringValue, SendTo, sizeof(vdqmDrvReq.drive));

  stringValue = (char *)clientUserName.c_str();
  DO_MARSHALL_STRING(p, stringValue, SendTo, sizeof(vdqmVolReq.client_name));

  len += 3 * LONGSIZE;

  // After marshalling we can send the information to the RTCPD or tape
  // aggregator daemon
  rc = netwrite_timeout(m_socket, buf, len, VDQM_TIMEOUT);

  if (rc == -1) {
    serrno = SECOMERR;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ) from " << remoteCopyType << ": " << neterror();
    throw ex;
  }
  else if (rc == 0) {
    serrno = SECONNDROP;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netwrite(REQ): connection dropped";
    throw ex;
  }


  acknSucc = readAnswer(cuuid, remoteCopyType);

  return acknSucc;
}


//------------------------------------------------------------------------------
// readAnswer
//------------------------------------------------------------------------------
bool castor::vdqm::RemoteCopyConnection::readAnswer(const Cuuid_t &cuuid,
  const char *remoteCopyType) throw (castor::exception::Exception) {

  char buffer[VDQM_MSGBUFSIZ];


  int rc = netread_timeout(m_socket, buffer, LONGSIZE*3, VDQM_TIMEOUT);

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

  LONG magic   = 0;
  LONG reqtype = 0;
  LONG len     = 0;

  // Unmarshall the header to get the length of the message body
  char *p = buffer;
  unmarshall_LONG(p, magic);
  unmarshall_LONG(p, reqtype);
  unmarshall_LONG(p, len);

  if ( len > 0 ) {
    if ( len > VDQM_MSGBUFSIZ - 3 * LONGSIZE ) {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << __PRETTY_FUNCTION__ <<
        ": message too large" <<
        ": remoteCopyType=" << remoteCopyType <<
        " maximum=" << (VDQM_MSGBUFSIZ - 3 * LONGSIZE) <<
        " actual=" << len;
      throw ex;
    }

    rc = netread_timeout(m_socket, p, len, VDQM_TIMEOUT);
    switch (rc) {
    case -1:
      {
        serrno = SECOMERR;
        castor::exception::Exception ex(serrno);
        ex.getMessage() << __PRETTY_FUNCTION__
          << ": netread(REQ) from " << remoteCopyType << ": " << neterror();
        throw ex;
      }
    case 0:
      {
        serrno = SECONNDROP;
        castor::exception::Exception ex(serrno);
        ex.getMessage() << __PRETTY_FUNCTION__
          << ": netread(REQ) from " << remoteCopyType << ": connection dropped";
        throw ex;
      }
    }

    // Unmarshall the header, this time for the magic number, request type and
    // message body length
    p = buffer;
    unmarshall_LONG(p, magic);
    unmarshall_LONG(p, reqtype);
    unmarshall_LONG(p, len);

    // If the magic number and request type are not valid
    if ( (magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) ||
         reqtype != VDQM_CLIENTINFO ) {
      return false;
    }

    // If the message body is not large enough for a status number and an empty
    // error string
    if(len < LONGSIZE + 1) {
      castor::exception::Exception ex(EMSGSIZE);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Invalid header from " << remoteCopyType
        << ": Message body not large enough for a status number and an empty "
        "string: Minimum size expected: " << (LONGSIZE + 1)
        << ": Received: " << len;

      throw ex;
    }

    LONG status = 0;
    DO_MARSHALL(LONG, p, status, ReceiveFrom);

    const size_t receivedMsgLen = len - LONGSIZE - 1; // minus status minus '\0'
    char errmsg[1024];
    const size_t msgLen = receivedMsgLen < sizeof(errmsg) ? receivedMsgLen :
      sizeof(errmsg) - 1;

    memcpy(errmsg, p, msgLen);
    errmsg[msgLen] = '\0';

    // Checking the size of the error message because the status maybe non-zero
    // even if there is no error
    if ( msgLen > 0 ) {
      serrno = ECANCELED;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << __PRETTY_FUNCTION__ <<
        ": received an error message" <<
        ": remoteCopyType=" << remoteCopyType <<
        " status=" <<  status <<
        " error msg=" << errmsg;
      throw ex;
    }
  }

  return true;
}
