/******************************************************************************
 *                      castor/legacymsg/TapeMarshal.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"
#include "h/serrno.h"

#include <errno.h>
#include <string.h>

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const TapeStatRequestMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeStatRequestMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(int32_t) + // uid
    sizeof(int32_t);// gid

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);
    ex.getMessage() << "Failed to marshal TapeStatRequestMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(TPMAGIC , p); // Magic number
  io::marshalUint32(TPSTAT, p); // Request type  
  char *msg_len_field_pointer = p;  
  io::marshalUint32(0, p); // Temporary length

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;
  io::marshalUint32(nbBytesMarshalled, msg_len_field_pointer); // Actual length

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeStatRequestMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const TapeConfigRequestMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeConfigRequestMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(int32_t) + // uid
    sizeof(int32_t) + // gid
    strlen(src.drive) + 1 + // drive
    sizeof(int16_t); // status

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);
    ex.getMessage() << "Failed to marshal TapeConfigRequestMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(TPMAGIC , p); // Magic number
  io::marshalUint32(TPCONF, p); // Request type  
  char *msg_len_field_pointer = p;  
  io::marshalUint32(0, p); // Temporary length

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);
  io::marshalString(src.drive, p);
  io::marshalUint16(src.status, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;
  io::marshalUint32(nbBytesMarshalled, msg_len_field_pointer); // Actual length

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeConfigRequestMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const SetVidRequestMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeConfigRequestMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    strlen(src.vid) + 1 + // vid
    strlen(src.drive) + 1; // drive

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);
    ex.getMessage() << "Failed to marshal TapeConfigRequestMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(TPMAGIC, p); // Magic number
  io::marshalUint32(SETVID, p); // Request type  
  char *msg_len_field_pointer = p;  
  io::marshalUint32(0, p); // Temporary length

  // Marshall message body
  io::marshalString(src.vid, p);
  io::marshalString(src.drive, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;
  io::marshalUint32(nbBytesMarshalled, msg_len_field_pointer); // Actual length

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal SetVidRequestMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, SetVidRequestMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalString(src, srcLen, dst.vid);
  io::unmarshalString(src, srcLen, dst.drive);  
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeConfigRequestMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalString(src, srcLen, dst.drive);
  io::unmarshalInt16(src, srcLen, dst.status);
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeStatRequestMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
}
