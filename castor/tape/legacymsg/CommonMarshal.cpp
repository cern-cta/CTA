/******************************************************************************
 *                      castor/tape/legacymsg/Common.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>
#include <iostream>
#include <string.h>


//------------------------------------------------------------------------------
// marshalUint8
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalUint8(uint8_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalUint8
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalUint8(const char * &src,
  size_t &srcLen, uint8_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalUint16
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalUint16(uint16_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalUint16
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalUint16(const char * &src,
  size_t &srcLen, uint16_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalUint32
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalUint32(uint32_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalUint32
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalUint32(const char * &src,
  size_t &srcLen, uint32_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalInt32
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalInt32(int32_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalInt32
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalInt32(const char * &src,
  size_t &srcLen, int32_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalUint64
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalUint64(uint64_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalUint64
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalUint64(const char * &src,
  size_t &srcLen, uint64_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalTime
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalTime(time_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshalValue(src, dst);
}


//------------------------------------------------------------------------------
// unmarshalTime
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalTime(const char * &src,
  size_t &srcLen, time_t &dst) throw(castor::exception::Exception) {

  unmarshalValue(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshalString
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalString(const char * src,
  char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  strcpy(dst, src);

  dst += strlen(src) + 1;
}


//------------------------------------------------------------------------------
// marshalString
//------------------------------------------------------------------------------
void castor::tape::legacymsg::marshalString(
  const std::string &src, char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  strcpy(dst, src.c_str());

  dst += strlen(src.c_str()) + 1;
}


//------------------------------------------------------------------------------
// unmarshalString
//------------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshalString(const char * &src,
  size_t &srcLen, char *dst, const size_t dstLen)
  throw(castor::exception::Exception) {

  if(src == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to source buffer is NULL");
  }

  if(srcLen == 0) {
    TAPE_THROW_CODE(EINVAL,
      ": Source buffer length is 0");
  }

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  if(dstLen == 0) {
    TAPE_THROW_CODE(EINVAL,
      ": Destination buffer length is 0");
  }

  // Calculate the maximum number of bytes that could be unmarshalled
  size_t maxlen = 0;
  if(srcLen < dstLen) {
    maxlen = srcLen;
  } else {
    maxlen = dstLen;
  }

  bool strTerminatorReached = false;

  // While there are potential bytes to copy and the string terminator has not
  // been reached
  for(size_t i=0; i<maxlen && !strTerminatorReached; i++) {
    // If the string terminator has been reached
    if((*dst++ = *src++) == '\0') {
      strTerminatorReached = true;
    }

    srcLen--;
  }

  // If all potential bytes were copied but the string terminator was not
  // reached
  if(!strTerminatorReached) {
    TAPE_THROW_CODE(EINVAL,
      ": String terminator of source buffer was not reached");
  }
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *const dst,
  const size_t dstLen, const MessageHeader &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  // Calculate the length of the message header
  const uint32_t totalLen = 3 * sizeof(uint32_t);  // magic + reqType + len

  // Check that the message header buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for message header"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshal the message header
  char *p = dst;
  marshalUint32(src.magic      , p);
  marshalUint32(src.reqType    , p);
  marshalUint32(src.lenOrStatus, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "message header and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, MessageHeader &dst) throw(castor::exception::Exception) {

  unmarshalUint32(src, srcLen, dst.magic);
  unmarshalUint32(src, srcLen, dst.reqType);
  unmarshalUint32(src, srcLen, dst.lenOrStatus);
}
