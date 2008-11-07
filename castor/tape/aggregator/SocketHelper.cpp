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
 * @author castor dev team
 *****************************************************************************/

/*
#include <net.h>
#include <netdb.h>
#include <errno.h>
#include <serrno.h>
#include <unistd.h> // for close()
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
*/

#include "castor/exception/Internal.hpp"
#include "castor/io/ServerSocket.hpp"
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
  std::ostream &os, castor::io::ServerSocket &socket) throw() {
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
// readMagicNumber
//------------------------------------------------------------------------------
uint32_t castor::tape::aggregator::SocketHelper::readMagicNumber(
  castor::io::ServerSocket &socket) throw (castor::exception::Exception) {

  uint32_t magic = 0;
  char     buffer[sizeof(magic)];

  // Read the magic number from the socket
  int rc = netread(socket.socket(), buffer, sizeof(magic));

  switch(rc) {
  case -1:
    {
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      printSocketDescription(os, socket);
      os << ": " << errno << " - " << strerror(errno);
      throw ex;
    }
  case 0:
    {
      castor::exception::Internal ex;
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      printSocketDescription(os, socket);
      os << ": connection was closed by the remote end";
      throw ex;
    }
  default:
    if (rc != sizeof(unsigned int)) {
      castor::exception::Internal ex;
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      printSocketDescription(os, socket);
      os << ": received the wrong number of bytes: received: " << rc
         << " expected: " << sizeof(unsigned int);
      throw ex;
    }
  }

  Marshaller::unmarshallUint32(buffer, magic);

  return magic;
}
