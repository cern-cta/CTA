/******************************************************************************
 *                      Utils.cpp
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

#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Utils.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>


//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Utils::copyString(char *const dst,
  const char *src, const size_t n) throw (castor::exception::Exception) {

  const size_t srcLen = strlen(src);

  if(srcLen >= n) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Source string is longer than destination.  Source length: "
      << srcLen << " Max destination length: " << (n-1);

    throw ex;
  }

  strncpy(dst, src, n);
    *(dst+n-1) = '\0'; // Ensure destination is null terminated
}


//-----------------------------------------------------------------------------
// createListenerSocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::Utils::createListenerSocket(
  const unsigned short port) throw (castor::exception::Exception) {
  int    socketFd = 0;
  struct sockaddr_in address;

  if ((socketFd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to create a listener socket";
    
    throw ex;
  }

  setBytes(address, 0);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);

  if(bind(socketFd, (struct sockaddr *) &address, sizeof(address)) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to bind listener socket";

    throw ex;
  }

  if(listen(socketFd, LISTENBACKLOG) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to mark listener socket as being a listener socket";

    throw ex;
  }

  return socketFd;
}


//-----------------------------------------------------------------------------
// getLocalIpAndPort
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Utils::getLocalIpAndPort(const int socketFd,
  unsigned long& ip, unsigned short& port)
  throw (castor::exception::Exception) {

  struct sockaddr_in address;
  socklen_t addressLen = sizeof(address);

  if(getsockname(socketFd, (struct sockaddr*)&address, &addressLen) < 0) {
    castor::exception::Exception ex(errno);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get socket name";

    throw ex;
  }

  ip   = ntohl(address.sin_addr.s_addr);
  port = ntohs(address.sin_port);
}


//------------------------------------------------------------------------------
// printIp
//------------------------------------------------------------------------------
void castor::tape::aggregator::Utils::printIp(std::ostream &os,
  const unsigned long ip) throw() {
  os << ((ip >> 24) & 0x000000FF) << "."
     << ((ip >> 16) & 0x000000FF) << "."
     << ((ip >>  8) & 0x000000FF) << "."
     << ( ip        & 0x000000FF);
}
