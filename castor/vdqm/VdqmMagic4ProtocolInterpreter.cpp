/******************************************************************************
 *                      VdqmMagic4ProtocolInterpreter.cpp
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
#include "castor/vdqm/Utils.hpp"
#include "castor/vdqm/vdqmMacros.h"
#include "castor/vdqm/VdqmMagic4ProtocolInterpreter.hpp"
#include "h/vdqm_constants.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmMagic4ProtocolInterpreter::VdqmMagic4ProtocolInterpreter(
  castor::io::ServerSocket &socket, const Cuuid_t &cuuid)
  throw(castor::exception::Exception) : m_socket(socket), m_cuuid(cuuid) {
}


//------------------------------------------------------------------------------
// readHeader
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic4ProtocolInterpreter::readHeader(
  const unsigned int magic, vdqmHdr_t &header)
  throw(castor::exception::Exception) {

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
    ex.getMessage() << "VdqmMagic4ProtocolInterpreter::readHeader() "
      "netread_timeout: " << neterror() << std::endl;
    throw ex;
  } else if(rc == 0) {
    castor::exception::Exception ex(SECONNDROP);
    ex.getMessage() << "VdqmMagic4ProtocolInterpreter::readHeader() "
      "netread_timeout: " << "connection dropped" << std::endl;
    throw ex;
  }

  // Un-marshall the message request type and length
  char *p = buf;
  DO_MARSHALL(LONG, p, header.reqtype, ReceiveFrom);
  DO_MARSHALL(LONG, p, header.len    , ReceiveFrom);
}


//------------------------------------------------------------------------------
// readAggregatorVolReq
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic4ProtocolInterpreter::readAggregatorVolReq(
  const int len, vdqmVolReq_t &msg) throw(castor::exception::Exception) {

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

  DO_MARSHALL(LONG,p,msg.VolReqID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.DrvReqID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.priority,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.client_port,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.clientUID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.clientGID,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.mode,ReceiveFrom);
  DO_MARSHALL(LONG,p,msg.recvtime,ReceiveFrom);
  if(unmarshall_STRINGN(p,msg.client_host,sizeof(msg.client_host))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN clientHost" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.volid,sizeof(msg.volid))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN volid" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.server,sizeof(msg.server))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN server" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.drive,sizeof(msg.drive))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN drive" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.dgn,sizeof(msg.dgn))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN dgn" << std::endl;
  }
  if(unmarshall_STRINGN(p,msg.client_name,sizeof(msg.client_name))) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall_STRINGN client_name" << std::endl;
  }
}


//------------------------------------------------------------------------------
// sendAggregatorVolReq
//------------------------------------------------------------------------------
void castor::vdqm::VdqmMagic4ProtocolInterpreter::sendAggregatorVolReqToClient
(vdqmHdr_t&, vdqmVolReq_t &msg) throw(castor::exception::Exception) {

  char *p = NULL;


  // Marshall the message body
  char buf[VDQM_MSGBUFSIZ];
  p = buf;
  DO_MARSHALL(LONG,p,msg.VolReqID,SendTo);
  DO_MARSHALL(LONG,p,msg.DrvReqID,SendTo);
  DO_MARSHALL(LONG,p,msg.priority,SendTo);
  DO_MARSHALL(LONG,p,msg.client_port,SendTo);
  DO_MARSHALL(LONG,p,msg.clientUID,SendTo);
  DO_MARSHALL(LONG,p,msg.clientGID,SendTo);
  DO_MARSHALL(LONG,p,msg.mode,SendTo);
  DO_MARSHALL(LONG,p,msg.recvtime,SendTo);
  p = Utils::marshallString(p, msg.client_host);
  p = Utils::marshallString(p, msg.volid);
  p = Utils::marshallString(p, msg.server);
  p = Utils::marshallString(p, msg.drive);
  p = Utils::marshallString(p, msg.dgn);
  Utils::marshallString(p, msg.client_name);

  // Marshall the message header
  int          magic   = VDQM_MAGIC4;
  int          reqtype = VDQM4_TAPEBRIDGE_VOL_REQ;
  vdqmVolReq_t *msgPtr = &msg;
  int          len     = VDQM_VOLREQLEN(msgPtr);
  char hdrbuf[VDQM_HDRBUFSIZ];
  p = hdrbuf;
  DO_MARSHALL(LONG,p,magic,SendTo);
  DO_MARSHALL(LONG,p,reqtype,SendTo);
  DO_MARSHALL(LONG,p,len,SendTo);

  // Send the message header to the client
  int rc = netwrite_timeout(m_socket.socket(), hdrbuf, VDQM_HDRBUFSIZ,
    VDQM_TIMEOUT);
  if (rc == -1) {
    serrno = SECOMERR;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "sendAggregatorVolReqToClient(): netwrite(HDR): "
      << neterror();
    throw ex;
  }
  else if (rc == 0) {
    serrno = SECONNDROP;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "sendAggregatorVolReqToClient(): netwrite(HDR): "
      "connection dropped" << std::endl;
    throw ex;
  }

  // Send the message body to the client
  rc = netwrite_timeout(m_socket.socket(), buf, len, VDQM_TIMEOUT);
  if (rc == -1) {
    serrno = SECOMERR;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "sendAggregatorVolReqToClient(): netwrite(REQ): "
      << neterror();
    throw ex;
  }
  else if (rc == 0) {
    serrno = SECONNDROP;
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "sendAggregatorVolReqToClient(): netwrite(REQ): "
      "connection dropped" << std::endl;
    throw ex;
  }
}
