/******************************************************************************
 *                      VdqmMagic3ProtocolInterpreter.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/vdqmMacros.h"
#include "castor/vdqm/VdqmMagic3ProtocolInterpreter.hpp"
#include "h/vdqm_constants.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmMagic3ProtocolInterpreter::VdqmMagic3ProtocolInterpreter(
  castor::io::ServerSocket *const sock, const Cuuid_t &cuuid)
  throw(castor::exception::Exception) : m_sock(sock), m_cuuid(cuuid) {

  if(0 == sock) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "sock argument is NULL";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// readHeader
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readHeader(
  const unsigned int magic, vdqmHdr_t *const header)
  throw(castor::exception::Exception) {

  if(header == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "VdqmMagic3ProtocolInterpreter::readHeader(): header "
      "argument is NULL" << std::endl;
    throw ex;
  }

  // Fill in the magic number which has already been read out from the socket
  header->magic = magic;

  // Header buffer is shorter, because the magic number should already be read
  // out
  char buf[VDQM_HDRBUFSIZ - LONGSIZE];

  // Read rest of header. The magic number is already read out
  const int rc = netread_timeout(m_sock->socket(), buf, sizeof(buf),
    VDQM_TIMEOUT);

  if(rc == -1) {
    castor::exception::Exception ex(SECOMERR);
    ex.getMessage() << "VdqmMagic3ProtocolInterpreter::readHeader() "
      "netread_timeout: " << neterror() << std::endl;
    throw ex;
  } else if(rc == 0) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << "VdqmMagic3ProtocolInterpreter::readHeader() "
      "netread_timeout: " << "connection dropped" << std::endl;
    throw ex;
  }

  // Un-marshall the message request type and length
  char *p = buf;
  DO_MARSHALL(LONG, p, header->reqtype, ReceiveFrom);
  DO_MARSHALL(LONG, p, header->len    , ReceiveFrom);
}


//------------------------------------------------------------------------------
// readDelDrv
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readDelDrv(const int len,
  vdqmDelDrv_t *const msg) throw(castor::exception::Exception) {
}


//------------------------------------------------------------------------------
// readDedicate
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readDedicate(const int len,
  vdqmDedicate_t *const msg) throw(castor::exception::Exception) {
}
