/******************************************************************************
 *                      SocketHelper.cpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/io/AbstractTCPSocket.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "h/net.h"

#include <errno.h>


//------------------------------------------------------------------------------
// printIp
//------------------------------------------------------------------------------
void castor::tape::aggregator::SocketHelper::printIp(std::ostream &os,
  const unsigned long ip) throw() {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}


//------------------------------------------------------------------------------
// printSocketDescription
//------------------------------------------------------------------------------
void castor::tape::aggregator::SocketHelper::printSocketDescription(
  std::ostream &os, castor::io::AbstractTCPSocket &socket) throw() {
  unsigned short localPort = 0;
  unsigned long  localIp   = 0;
  unsigned short peerPort  = 0;
  unsigned long  peerIp    = 0;

  socket.getPortIp(localPort, localIp);
  socket.getPeerIp(peerPort , peerIp );

  os << "local=";
  printIp(os, localIp);
  os << ":" << localPort;
  os << " peer=";
  printIp(os, peerIp);
  os << ":" << peerPort;
}


//------------------------------------------------------------------------------
// readUint32
//------------------------------------------------------------------------------
uint32_t castor::tape::aggregator::SocketHelper::readUint32(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout)
  throw (castor::exception::Exception) {

  uint32_t value = 0;
  char buf[sizeof(value)];

  const int netreadRc = netread_timeout(socket.socket(), buf, sizeof(value),
    netReadWriteTimeout);
  const int netreadErrno = errno;

  switch(netreadRc) {
  case -1:
    {
      char strerrbuf[STRERRORBUFLEN];
      if(strerror_r(netreadErrno, strerrbuf, sizeof(strerrbuf))) {
        strncpy(strerrbuf, "Uknown", sizeof(strerrbuf));
        strerrbuf[sizeof(strerrbuf)-1] = '\0';
      }

      castor::exception::Exception ex(serrno);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to read unsigned 32-bit integer from socket: ";
      printSocketDescription(os, socket);
      os << ": " << netreadErrno << " - " << strerrbuf;
      throw ex;
    }
  case 0:
    {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to read unsigned 32-bit integer from socket: ";
      printSocketDescription(os, socket);
      os << ": Connection was closed by the remote end";
      throw ex;
    }
  default:
    if (netreadRc != sizeof(value)) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to read unsigned 32-bit from socket: ";
      printSocketDescription(os, socket);
      os << ": Read the wrong number of bytes"
        << ": Expected: " << sizeof(value)
        << ": Read: " << netreadRc;
      throw ex;
    }
  }

  const char *p     = buf;
  size_t     bufLen = sizeof(buf);

  Marshaller::unmarshallUint32(p, bufLen, value);

  return value;
}


//------------------------------------------------------------------------------
// writeUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::SocketHelper::writeUint32(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  uint32_t value) throw(castor::exception::Exception) {

;
  const int netwriteRc = netwrite_timeout(socket.socket(), &value,
    sizeof(value), netReadWriteTimeout);

  switch(netwriteRc) {
  case -1:
    {
      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write unsigned 32-bit integer to socket: ";
      printSocketDescription(os, socket);
      os << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      castor::exception::Exception ex(SECONNDROP);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write unsigned 32-bit integer to socket: ";
      printSocketDescription(os, socket);
      os << ": Connection dropped";
      throw ex;
    }
  default:
    if(netwriteRc != sizeof(value)) {
      castor::exception::Exception ex(SECOMERR);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to write unsigned 32-bit integer to socket: ";
      printSocketDescription(os, socket);
      os << "Wrote the wrong number of bytes"
        << ": Expected: " << sizeof(value)
        << ": Wrote: " << netwriteRc;
      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// readBytes
//------------------------------------------------------------------------------
void castor::tape::aggregator::SocketHelper::readBytes(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  const int nbBytes, char *buf) throw (castor::exception::Exception) {

  const int netreadRc = netread_timeout(socket.socket(), buf, nbBytes,
    netReadWriteTimeout);
  const int netreadErrno = errno;

  switch(netreadRc) {
  case -1:
    {
      char strerrbuf[STRERRORBUFLEN];
      if(strerror_r(netreadErrno, strerrbuf, sizeof(strerrbuf))) {
        strncpy(strerrbuf, "Uknown", sizeof(strerrbuf));
        strerrbuf[sizeof(strerrbuf)-1] = '\0';
      }

      castor::exception::Exception ex(serrno);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socket);
      os << ": " << netreadErrno << " - " << strerrbuf;
      throw ex;
    }
  case 0:
    {
      castor::exception::Exception ex(SECONNDROP);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socket);
      os << ": Connection was closed by the remote end";
      throw ex;
    }
  default:
    if (netreadRc != nbBytes) {
      castor::exception::Exception ex(SECOMERR);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to read " << nbBytes << " bytes from socket: ";
      printSocketDescription(os, socket);
      os << "Read the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Read: " << netreadRc;
      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// writeBytes
//------------------------------------------------------------------------------
void castor::tape::aggregator::SocketHelper::writeBytes(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  const int nbBytes, char *const buf)
  throw (castor::exception::Exception) {

  const int netwriteRc = netwrite_timeout(socket.socket(), buf, nbBytes,
    netReadWriteTimeout);

  switch(netwriteRc) {
  case -1:
    {
      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socket);
      os << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      castor::exception::Exception ex(SECONNDROP);
      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << ": Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socket);
      os << ": Connection dropped";
      throw ex;
    }
  default:
    if(netwriteRc != nbBytes) {
      castor::exception::Exception ex(SECOMERR);

      ex.getMessage() << __PRETTY_FUNCTION__;
      std::ostream &os = ex.getMessage();
      os << "Failed to write " << nbBytes << " bytes to socket: ";
      printSocketDescription(os, socket);
      os << "Wrote the wrong number of bytes"
        << ": Expected: " << nbBytes
        << ": Wrote: " << netwriteRc;
      throw ex;
    }
  }
}
