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

#include "castor/exception/Internal.hpp"

#include <vdqm_constants.h>
#include "osdep.h" //for LONGSIZE
#include "Cnetdb.h"
#include <common.h>

// Local Includes
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/SocketHelper.hpp"
#include "castor/vdqm/vdqmMacros.h"  // Needed for marshalling

// definition of some constants
#define STG_CALLBACK_BACKLOG 2
#define VDQMSERV 1


//------------------------------------------------------------------------------
// readMagicNumber
//------------------------------------------------------------------------------
unsigned int castor::vdqm::SocketHelper::readMagicNumber(
  castor::io::ServerSocket *const socket)
  throw (castor::exception::Exception) {

  char buffer[sizeof(unsigned int)];

  // Read the magic number from the socket
  int rc = netread(socket->socket(), buffer, sizeof(unsigned int));

  switch(rc) {
  case -1:
    {
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": " << errno << " - " << strerror(errno);
      throw ex;
    }
  case 0:
    {
      castor::exception::Internal ex;
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": connection was closed by the remote end";
      throw ex;
    }
  default:
    if (rc != sizeof(unsigned int)) {
      castor::exception::Internal ex;
      std::ostream &os = ex.getMessage();
      os << "Failed to read Magic Number from socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": received the wrong number of bytes: received: " << rc
         << " expected: " << sizeof(unsigned int);
      throw ex;
    }
  }

  char *p = buffer;
  unsigned int magic;
  DO_MARSHALL(LONG, p, magic, ReceiveFrom);

  return magic;
}


//------------------------------------------------------------------------------
// netWriteVdqmHeader
//------------------------------------------------------------------------------
void castor::vdqm::SocketHelper::netWriteVdqmHeader(
  castor::io::ServerSocket *const socket, void *hdrbuf)
  throw (castor::exception::Exception) {

  int rc = netwrite_timeout(socket->socket(), hdrbuf, VDQM_HDRBUFSIZ,
    VDQM_TIMEOUT);

  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to write VDQM header to socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to write VDQM header to socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": connection was closed by the remote end";
      throw ex;
    }
  }

#ifdef PRINT_NETWORK_MESSAGES
  castor::vdqm::DevTools::printMessage(std::cout, true, true, socket->socket(),
    hdrbuf);
#endif
}


//------------------------------------------------------------------------------
// netReadVdqmHeader
//------------------------------------------------------------------------------
void castor::vdqm::SocketHelper::netReadVdqmHeader(
  castor::io::ServerSocket *const socket, void* hdrbuf)
  throw (castor::exception::Exception) {

  int rc = netread_timeout(socket->socket(), hdrbuf, VDQM_HDRBUFSIZ,
    VDQM_TIMEOUT);

  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to read VDQM header from socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": " << neterror();
      throw ex;
    }
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
      std::ostream &os = ex.getMessage();
      os << "Failed to read VDQM header from socket: ";
      castor::vdqm::DevTools::printSocketDescription(os, socket);
      os << ": connection was closed by the remote end";
      throw ex;
    }
  }

#ifdef PRINT_NETWORK_MESSAGES
  castor::vdqm::DevTools::printMessage(std::cout, false, true, socket->socket(),
    hdrbuf);
#endif
}
