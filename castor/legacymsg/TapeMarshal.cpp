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
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeStatRequestMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeStatRequestMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the total length of the message (header + body)
  const size_t totalLen =
    sizeof(uint32_t) + // Magic
    sizeof(uint32_t) + // Request type
    sizeof(uint32_t) + // Length of header + body
    sizeof(src.uid) +
    sizeof(src.gid);

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
  io::marshalUint32(totalLen, p); // Length of header + body

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeStatRequestMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeStatReplyMsgBody &body) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeStatReplyMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  uint32_t len = sizeof(body.number_of_drives);
  for(uint16_t i = 0; i<body.number_of_drives; i++) {
    len +=
      sizeof(body.drives[i].uid)         +
      sizeof(body.drives[i].jid)         +
      strlen(body.drives[i].dgn) + 1     +
      sizeof(body.drives[i].up)          +
      sizeof(body.drives[i].asn)         +
      sizeof(body.drives[i].asn_time)    +
      strlen(body.drives[i].drive) + 1   +
      sizeof(body.drives[i].mode)        +
      strlen(body.drives[i].lblcode) + 1 +
      sizeof(body.drives[i].tobemounted) +
      strlen(body.drives[i].vid) + 1     +
      strlen(body.drives[i].vsn) + 1     +
      sizeof(body.drives[i].cfseq);
  }

  // Calculate the total length of the message (header + body)
  size_t totalLen =
    sizeof(uint32_t) + // Magic number
    sizeof(uint32_t) + // Request type
    sizeof(uint32_t) + // Length of message body
    len;

  // Check that the message header buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeStatReplyMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshal message header
  char *p = dst;
  io::marshalUint32(TPMAGIC, p); // Magic number
  io::marshalUint32(MSG_DATA, p); // Request type
  io::marshalUint32(len, p); // Length of message body

  // Marshal message body
  io::marshalUint16(body.number_of_drives, p);

  // Marshal the info of each unit
  for(int i=0; i<body.number_of_drives; i++) {
    io::marshalUint32(body.drives[i].uid, p);
    io::marshalUint32(body.drives[i].jid, p);
    io::marshalString(body.drives[i].dgn, p);
    io::marshalUint16(body.drives[i].up, p);
    io::marshalUint16(body.drives[i].asn, p);
    io::marshalUint32(body.drives[i].asn_time, p);
    io::marshalString(body.drives[i].drive, p);
    io::marshalUint16(body.drives[i].mode, p);
    io::marshalString(body.drives[i].lblcode, p);
    io::marshalUint16(body.drives[i].tobemounted, p);
    io::marshalString(body.drives[i].vid, p);
    io::marshalString(body.drives[i].vsn, p);
    io::marshalUint32(body.drives[i].cfseq, p);
  }

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeStatReplyMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeConfigRequestMsgBody &src) throw(castor::exception::Exception) {

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
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const TapeLabelRqstMsgBody &src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeLabelRqstMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    sizeof(int32_t) + // uid
    sizeof(int32_t) + // gid
    strlen(src.vid) + 1 + // vid
    strlen(src.drive) + 1 + // drive
    strlen(src.dgn) + 1 + // dgn
    strlen(src.density) + 1; // density

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);
    ex.getMessage() << "Failed to marshal TapeLabelRqstMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall message header
  char *p = dst;
  io::marshalUint32(TPMAGIC , p); // Magic number
  io::marshalUint32(TPLABEL, p); // Request type  
  char *msg_len_field_pointer = p;  
  io::marshalUint32(0, p); // Temporary length

  // Marshall message body
  io::marshalUint32(src.uid, p);
  io::marshalUint32(src.gid, p);
  io::marshalString(src.vid, p);
  io::marshalString(src.drive, p);
  io::marshalString(src.dgn, p);
  io::marshalString(src.density, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;
  io::marshalUint32(nbBytesMarshalled, msg_len_field_pointer); // Actual length

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to marshal TapeLabelRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen, const TapeUpdateDriveRqstMsgBody &src) throw(castor::exception::Exception) {
  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to marshal TapeUpdateDriveRqstMsgBody"
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
    ex.getMessage() << "Failed to marshal TapeUpdateDriveRqstMsgBody"
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
    ex.getMessage() << "Failed to marshal TapeUpdateDriveRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeStatReplyMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint16(src, srcLen, dst.number_of_drives);
  for(int i=0; i<dst.number_of_drives; i++) {
    io::unmarshalUint32(src, srcLen, dst.drives[i].uid);
    io::unmarshalUint32(src, srcLen, dst.drives[i].jid);
    io::unmarshalString(src, srcLen, dst.drives[i].dgn);
    io::unmarshalUint16(src, srcLen, dst.drives[i].up);
    io::unmarshalUint16(src, srcLen, dst.drives[i].asn);
    io::unmarshalUint32(src, srcLen, dst.drives[i].asn_time);
    io::unmarshalString(src, srcLen, dst.drives[i].drive);
    io::unmarshalUint16(src, srcLen, dst.drives[i].mode);
    io::unmarshalString(src, srcLen, dst.drives[i].lblcode);
    io::unmarshalUint16(src, srcLen, dst.drives[i].tobemounted);
    io::unmarshalString(src, srcLen, dst.drives[i].vid);
    io::unmarshalString(src, srcLen, dst.drives[i].vsn);
    io::unmarshalUint32(src, srcLen, dst.drives[i].cfseq);
  }
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeUpdateDriveRqstMsgBody &dst) throw(castor::exception::Exception) {
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
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeLabelRqstMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
  io::unmarshalString(src, srcLen, dst.vid);
  io::unmarshalString(src, srcLen, dst.drive);
  io::unmarshalString(src, srcLen, dst.dgn);
  io::unmarshalString(src, srcLen, dst.density);
}

//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen, TapeStatRequestMsgBody &dst) throw(castor::exception::Exception) {
  io::unmarshalUint32(src, srcLen, dst.uid);
  io::unmarshalUint32(src, srcLen, dst.gid);
}
