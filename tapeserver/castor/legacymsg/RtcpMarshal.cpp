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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/RtcpMarshal.hpp"
#include "h/rtcp_constants.h"

#include <errno.h>
#include <iostream>
#include <shift/vdqm_constants.h>
#include <string.h>


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst, const size_t dstLen,
  const RtcpJobRqstMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobRqstMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    4*sizeof(uint32_t)          +
    strlen(src.clientHost)      +
    strlen(src.dgn)             +
    strlen(src.driveUnit)       +
    strlen(src.clientUserName)  +
    4; // 4 = the number of string termination characters

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobRqstMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(RTCOPY_MAGIC_OLD0  , p); // Magic number
  io::marshalUint32(VDQM_CLIENTINFO    , p); // Request type
  io::marshalUint32(len                , p); // Length of message body
  io::marshalUint32(src.volReqId       , p);
  io::marshalUint32(src.clientPort     , p);
  io::marshalUint32(src.clientEuid     , p);
  io::marshalUint32(src.clientEgid     , p);
  io::marshalString(src.clientHost     , p);
  io::marshalString(src.dgn            , p);
  io::marshalString(src.driveUnit      , p);
  io::marshalString(src.clientUserName , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpJobRqstMsgBody &dst)  {

  io::unmarshalUint32(src, srcLen, dst.volReqId);
  io::unmarshalUint32(src, srcLen, dst.clientPort);
  io::unmarshalUint32(src, srcLen, dst.clientEuid);
  io::unmarshalUint32(src, srcLen, dst.clientEgid);
  io::unmarshalString(src, srcLen, dst.clientHost);
  io::unmarshalString(src, srcLen, dst.dgn);
  io::unmarshalString(src, srcLen, dst.driveUnit);
  io::unmarshalString(src, srcLen, dst.clientUserName);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *const dst,
  const size_t dstLen, const RtcpJobReplyMsgBody &src)
   {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobReplyMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Minimum buffer length = magic number + request type + length + status code
  // + terminating null character
  const size_t minimumLen = 4*sizeof(uint32_t) + 1;
  if(dstLen < minimumLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobReplyMsgBody"
      ": Buffer too small: required=" << minimumLen << " actual=" << dstLen;
    throw ex;
  }

  // Calculate the length of the error message to be sent taking into account
  // the fact that the error message should be right trimmed if it is too large
  // to fit in the destination buffer.
  const size_t headerLen = 3 * sizeof(uint32_t); // magic + reqType + len
  // free space = total - header - status - terminating null character
  const size_t freeSpace = dstLen - headerLen - sizeof(uint32_t) - 1;
  const size_t errLenToSend = strlen(src.errorMessage) < freeSpace ?
    strlen(src.errorMessage) : freeSpace;

  // Calculate the length of the message body
  // Message body = status + error message + terminating null character
  const uint32_t len = sizeof(src.status) + errLenToSend + 1;

  // Calculate the total length of the message (header + body)
  const size_t totalLen = headerLen + len;

  char *p = dst;
  io::marshalUint32(RTCOPY_MAGIC_OLD0, p); // Magic number
  io::marshalUint32(VDQM_CLIENTINFO  , p); // Request type
  io::marshalUint32(len              , p); // Length
  io::marshalUint32(src.status       , p); // status code

  strncpy(p, src.errorMessage, errLenToSend);
  *(p+errLenToSend) = '0';
  p = p + errLenToSend + 1;

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpJobReplyMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << " actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  RtcpJobReplyMsgBody &dst)  {

  // Unmarshal the status number
  io::unmarshalInt32(src, srcLen, dst.status);

  // The error message will be right trimmed if it is too large
  const size_t maxBytesToUnmarshal = srcLen < sizeof(dst.errorMessage) ?
    srcLen : sizeof(dst.errorMessage);

  // Unmarshal the error message
  if(maxBytesToUnmarshal > 0) {
    strncpy(dst.errorMessage, src, maxBytesToUnmarshal);
    dst.errorMessage[maxBytesToUnmarshal - 1] = '\0';
    src    = src + strlen(dst.errorMessage) + 1;
    srcLen = srcLen - strlen(dst.errorMessage) - 1;
  } else {
    dst.errorMessage[0] = '\0';
    // No need to update src or srcLen
  }
}

