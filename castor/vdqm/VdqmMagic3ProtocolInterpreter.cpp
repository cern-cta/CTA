/******************************************************************************
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
#include "castor/vdqm/vdqmMacros.h"
#include "castor/vdqm/VdqmMagic3ProtocolInterpreter.hpp"
#include "h/serrno.h"
#include "h/vdqm_constants.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmMagic3ProtocolInterpreter::VdqmMagic3ProtocolInterpreter(
  castor::io::ServerSocket &socket, const Cuuid_t &cuuid)
   : m_socket(socket), m_cuuid(cuuid) {
}


//------------------------------------------------------------------------------
// readHeader
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readHeader(
  const unsigned int magic, vdqmHdr_t &header)
   {

  // Fill in the magic number which has already been read out from the socket
  header.magic = magic;

  // Header buffer is shorter, because the magic number should already be read
  // out
  char buf[VDQM_HDRBUFSIZ - LONGSIZE];

  // Read rest of header. The magic number is already read out
  const int rc = netread_timeout(m_socket.socket(), buf, sizeof(buf),
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
  DO_MARSHALL(LONG, p, header.reqtype, ReceiveFrom);
  DO_MARSHALL(LONG, p, header.len    , ReceiveFrom);
}


//------------------------------------------------------------------------------
// readDelDrv
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readDelDrv(const int len,
  vdqmDelDrv_t &msg)  {

  if(!VALID_VDQM_MSGLEN(len)) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netread(REQ): connection dropped" << std::endl;
    throw ex;
  }

  char buf[VDQM_MSGBUFSIZ];
  int rc = 0;

  // Read the message body
  rc = netread_timeout(m_socket.socket(), buf, len, VDQM_TIMEOUT);

  if(rc == -1) {
    castor::exception::Exception ex(SECOMERR);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ":netread_timeout: " << neterror() << std::endl;
    throw ex;
  } else if(rc == 0) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netread_timeout: " << "connection dropped" << std::endl;
    throw ex;
  }

  // Un-marshall the message body
  char *p = buf;

  DO_MARSHALL(LONG,p,msg.clientUID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.clientGID,ReceiveFrom);
  if(unmarshall_STRINGN(p,msg.clientHost, sizeof(msg.clientHost))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN clientHost" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.server, sizeof(msg.server))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN server" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.drive, sizeof(msg.drive))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN drive" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.dgn, sizeof(msg.dgn))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN dgn" << std::endl;
  }
}


//------------------------------------------------------------------------------
// readDedicate
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic3ProtocolInterpreter::readDedicate(const int len,
  vdqmDedicate_t &msg)  {

  if(!VALID_VDQM_MSGLEN(len)) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netread(REQ): connection dropped" << std::endl;
    throw ex;
  }

  char buf[VDQM_MSGBUFSIZ];
  int rc = 0;

  // Read the message body
  rc = netread_timeout(m_socket.socket(), buf, len, VDQM_TIMEOUT);

  if(rc == -1) {
    castor::exception::Exception ex(SECOMERR);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ":netread_timeout: " << neterror() << std::endl;
    throw ex;
  } else if(rc == 0) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": netread_timeout: " << "connection dropped" << std::endl;
    throw ex;
  }

  // Un-marshall the message body
  char *p = buf;

  DO_MARSHALL(LONG,p,msg.clientUID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.clientGID,ReceiveFrom);
  if(unmarshall_STRINGN(p,msg.clientHost, sizeof(msg.clientHost))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN clientHost" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.server, sizeof(msg.server))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN server" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.drive, sizeof(msg.drive))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN drive" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.dgn, sizeof(msg.dgn))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN dgn" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.dedicate, sizeof(msg.dedicate))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN dedicate" << std::endl;
  }
}
