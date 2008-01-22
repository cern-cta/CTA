/******************************************************************************
 *                      VdqmSocketHelper.cpp
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
#include "castor/vdqm/VdqmSocketHelper.hpp"
#include "castor/vdqm/vdqmMacros.h"  // Needed for marshalling

// definition of some constants
#define STG_CALLBACK_BACKLOG 2
#define VDQMSERV 1


//------------------------------------------------------------------------------
// readMagicNumber
//------------------------------------------------------------------------------
unsigned int castor::vdqm::VdqmSocketHelper::readMagicNumber(const int socket)
  throw (castor::exception::Exception) {

  char buffer[sizeof(unsigned int)];
  char *p;
  int ret;
  unsigned int magic;

  // Read the magic number from the socket
  ret = netread(socket, buffer, sizeof(unsigned int));

  if (ret != sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "VdqmSocketHelper::readMagicNumber(): "
                      << "Unable to receive Magic Number" << std::endl
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmSocketHelper::readMagicNumber(): "
                      << "Unable to receive Magic Number: "
                      << errno << " - " << strerror(errno) << std::endl;
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "VdqmSocketHelper::readMagicNumber(): "
                      << "Received Magic Number is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }


  /**
   * This is code for the C++ world, which is not compatible with the
   * C Marshalling of the magic number.
   */
  //  message = std::string(buffer, sizeof(unsigned int));
  //  castor::io::biniostream rcvStream(message);
  // castor::io::biniostream& addrPointer = rcvStream;
  //
  // /**
  //  * Do the unmarshalling of the message
  //  */
  //  addrPointer >> magic;

  p = buffer;
  DO_MARSHALL(LONG, p, magic, ReceiveFrom);

  return magic;
}


//------------------------------------------------------------------------------
// vdqmNetwrite
//------------------------------------------------------------------------------
void castor::vdqm::VdqmSocketHelper::vdqmNetwrite(const int socket,
  void* hdrbuf)
  throw (castor::exception::Exception) {
  int rc;

  castor::vdqm::DevTools::printMessage(std::cout, true, true, socket, hdrbuf);

  rc = netwrite_timeout(socket, hdrbuf, VDQM_HDRBUFSIZ, VDQM_TIMEOUT);
  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmSocketHelper: "
                      << "netwrite(HDR): "
                      << neterror() << std::endl;
      throw ex;
    }
    break;
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmSocketHelper: "
                      << "netwrite(HDR): connection dropped"
                      << std::endl;
      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// vdqmNetread
//------------------------------------------------------------------------------
void castor::vdqm::VdqmSocketHelper::vdqmNetread(const int socket, void* hdrbuf)
  throw (castor::exception::Exception) {
  int rc;

  rc = netread_timeout(socket, hdrbuf, VDQM_HDRBUFSIZ, VDQM_TIMEOUT);
  switch (rc) {
  case -1:
    {
      serrno = SECOMERR;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmSocketHelper: "
                      << "netread(HDR): "
                      << neterror() << std::endl;
      throw ex;
    }
  case 0:
    {
      serrno = SECONNDROP;
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "VdqmSocketHelper: "
                      << "netread(HDR): connection dropped"
                      << std::endl;
      throw ex;
    }
  }

  castor::vdqm::DevTools::printMessage(std::cout, false, true, socket, hdrbuf);
}
