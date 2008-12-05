/******************************************************************************
 *                      Marshaller.cpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <string.h>


//------------------------------------------------------------------------------
// marshallUint8
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint8(uint8_t src,
  char * &dst) throw (castor::exception::Exception) {

  marshall(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint8
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint8(const char * &src,
  size_t &srcLen, uint8_t &dst) throw(castor::exception::Exception) {

  unmarshall(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallUint16
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint16(uint16_t src,
  char * &dst) throw (castor::exception::Exception) {

  marshall(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint16
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint16(const char * &src,
  size_t &srcLen, uint16_t &dst) throw(castor::exception::Exception) {

  unmarshall(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint32(uint32_t src,
  char * &dst) throw (castor::exception::Exception) {

  marshall(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint32(const char * &src,
  size_t &srcLen, uint32_t &dst) throw(castor::exception::Exception) {

  unmarshall(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallString(const char * src,
  char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  strcpy(dst, src);

  dst += strlen(src) + 1;
}


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallString(
  const std::string &src, char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  strcpy(dst, src.c_str());

  dst += strlen(src.c_str()) + 1;
}


//------------------------------------------------------------------------------
// unmarshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallString(const char * &src,
  size_t &srcLen, char *dst, const size_t dstLen)
  throw (castor::exception::Exception) {

  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to source buffer is NULL";
    throw ex;
  }

  if(srcLen == 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Source buffer length is 0";
    throw ex;
  }

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  if(dstLen == 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Destination buffer length is 0";
    throw ex;
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
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": String terminator of source buffer was not reached";
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// marshallRcpJobRequest
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRcpJobRequest(
  char *const dst, const size_t dstLen, const RcpJobRequest &request)
  throw (castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    4*sizeof(uint32_t)              +
    strlen(request.clientHost)      +
    strlen(request.deviceGroupName) +
    strlen(request.driveName)       +
    strlen(request.clientUserName)  +
    4; // 4 = the number of string termination characters

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqtype + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Buffer too small for job submission request message: "
      "Required size: " << totalLen << " Actual size: " << dstLen;

    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC_OLD0      , p);
  marshallUint32(VDQM_CLIENTINFO        , p);
  marshallUint32(len                    , p);
  marshallUint32(request.tapeRequestID  , p);
  marshallUint32(request.clientPort     , p);
  marshallUint32(request.clientEuid     , p);
  marshallUint32(request.clientEgid     , p);
  marshallString(request.clientHost     , p);
  marshallString(request.deviceGroupName, p);
  marshallString(request.driveName      , p);
  marshallString(request.clientUserName , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Mismatch between the expected total length of the "
      "RCP job submission request message and the actual number of bytes "
      "marshalled: Expected length: " << totalLen << " Marshalled: "
      << nbBytesMarshalled;

    throw ie;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRcpJobRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRcpJobRequest(
  const char * &src, size_t &srcLen, RcpJobRequest &dst)
  throw(castor::exception::Exception) {

  unmarshallUint32(src, srcLen, dst.tapeRequestID);
  unmarshallUint32(src, srcLen, dst.clientPort);
  unmarshallUint32(src, srcLen, dst.clientEuid);
  unmarshallUint32(src, srcLen, dst.clientEgid);
  unmarshallString(src, srcLen, dst.clientHost, sizeof(dst.clientHost));
  unmarshallString(src, srcLen, dst.deviceGroupName,
    sizeof(dst.deviceGroupName));
  unmarshallString(src, srcLen, dst.driveName, sizeof(dst.driveName));
  unmarshallString(src, srcLen, dst.clientUserName, sizeof(dst.clientUserName));
}


//-----------------------------------------------------------------------------
// unmarshallRcpJobReply
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRcpJobReply(
  const char * &src, size_t &srcLen, RcpJobReply &dst)
  throw(castor::exception::Exception) {

  // Unmarshall the status number
  Marshaller::unmarshallUint32(src, srcLen, dst.status);

  // The error message will be right trimmed if it is too large
  const size_t maxBytesToUnmarshall = srcLen < sizeof(dst.errorMessage) ?
    srcLen : sizeof(dst.errorMessage);

  // Unmarshall the error message
  if(maxBytesToUnmarshall > 0) {
    strncpy(dst.errorMessage, src, maxBytesToUnmarshall);
    dst.errorMessage[maxBytesToUnmarshall - 1] = '\0';
    src    = src + strlen(dst.errorMessage) + 1;
    srcLen = srcLen - strlen(dst.errorMessage) - 1;
  } else {
    dst.errorMessage[0] = '\0';
    // No need to update src or srcLen
  }
}


//-----------------------------------------------------------------------------
// marshallRtcpAckn
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpAckn(char *const dst,
  const size_t dstLen, const uint32_t status, const char *errorMsg)
  throw (castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Minimum buffer length = magic number + request type + length + status code
  // + the string termination character '\0'
  if(dstLen < 4*sizeof(uint32_t) + 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Destination buffer length is too small: "
      "Expected minimum length: " << (4*sizeof(uint32_t) + 1)
       << ": Actual length: " << dstLen;
    throw ex;
  }

  if(errorMsg == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to error message is NULL";
    throw ex;
  }

  const size_t errorMsgLen      = strlen(errorMsg);
  const size_t maxErrorMsgLen   = dstLen - 4*sizeof(uint32_t) - 1;
  const size_t errorMsg2SendLen = errorMsgLen > maxErrorMsgLen ? maxErrorMsgLen
    : errorMsgLen;

  // Length of message body equals the size fo the status code plus the length
  // of the error message string plus the size of the string termination
  // characater '\0'
  const uint32_t len = sizeof(uint32_t) + errorMsg2SendLen + 1;
 
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC_OLD0, p); // Magic number
  marshallUint32(VDQM_CLIENTINFO  , p); // Request type
  marshallUint32(len              , p); // Length
  marshallUint32(status           , p); // status code

  // Marshall the error message without the terminatiing null byte
  memcpy(p, errorMsg, errorMsg2SendLen);
  p += errorMsg2SendLen;

  // Marshall the terminatiing null byte of the error message
  *p = '\0';
  p += 1;

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqtype + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Mismatch between the expected total length of the "
      "RTCP acknowledge message and the actual number of bytes marshalled: "
      "Expected length: " << totalLen << " Marshalled: " << nbBytesMarshalled;

    throw ie;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// marshallRtcpTapeRequest
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpTapeRequest(char *dst,
  const size_t dstLen, const RtcpTapeRequest &request)
  throw (castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len = 14 * sizeof(uint32_t) + strlen(request.vid) +
    strlen(request.vsn) + strlen(request.label) + strlen(request.devtype) +
    strlen(request.density) + strlen(request.unit) + 6 + sizeof(Cuuid_t) +
    // Plus the size of the err member which is of type rtcpErrMsg_t
    4 * sizeof(uint32_t) + strlen(request.err.errmsgtxt) + 1;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqtype + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Buffer too small for tape request message: "
      "Required size: " << totalLen << " Actual size: " << dstLen;

    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC                              , p);
  marshallUint32(RTCP_TAPEERR_REQ                          , p);
  marshallUint32(len                                       , p);
  marshallString(request.vid                               , p);
  marshallString(request.vsn                               , p);
  marshallString(request.label                             , p);
  marshallString(request.devtype                           , p);
  marshallString(request.density                           , p);
  marshallString(request.unit                              , p);
  marshallUint32(request.VolReqID                          , p);
  marshallUint32(request.jobID                             , p);
  marshallUint32(request.mode                              , p);
  marshallUint32(request.start_file                        , p);
  marshallUint32(request.end_file                          , p);
  marshallUint32(request.side                              , p);
  marshallUint32(request.tprc                              , p);
  marshallUint32(request.TStartRequest                     , p);
  marshallUint32(request.TEndRequest                       , p);
  marshallUint32(request.TStartRtcpd                       , p);
  marshallUint32(request.TStartMount                       , p);
  marshallUint32(request.TEndMount                         , p);
  marshallUint32(request.TStartUnmount                     , p);
  marshallUint32(request.TEndUnmount                       , p);
  marshallUint32(request.rtcpReqId.time_low                , p);
  marshallUint16(request.rtcpReqId.time_mid                , p);
  marshallUint16(request.rtcpReqId.time_hi_and_version     , p);
  marshallUint8(request.rtcpReqId.clock_seq_hi_and_reserved, p);
  marshallUint8(request.rtcpReqId.clock_seq_low            , p);
  marshallUint8(request.rtcpReqId.node[0]                  , p);
  marshallUint8(request.rtcpReqId.node[1]                  , p);
  marshallUint8(request.rtcpReqId.node[2]                  , p);
  marshallUint8(request.rtcpReqId.node[3]                  , p);
  marshallUint8(request.rtcpReqId.node[4]                  , p);
  marshallUint8(request.rtcpReqId.node[5]                  , p);
  marshallString(request.err.errmsgtxt                     , p);
  marshallUint32(request.err.severity                      , p);
  marshallUint32(request.err.errorcode                     , p);
  marshallUint32(request.err.max_tpretry                   , p);
  marshallUint32(request.err.max_cpretry                   , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Mismatch between the expected total length of the "
      "RTCP tape request message and the actual number of bytes marshalled: "
      "Expected length: " << totalLen << " Marshalled: " << nbBytesMarshalled;

    throw ie;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRtcpTapeRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRtcpTapeRequest(
  const char * &src, size_t &srcLen, RtcpTapeRequest &request)
  throw(castor::exception::Exception) {

/*
  unmarshallString(src, srcLen, request.vid);
  unmarshallString(src, srcLen, request.vsn);
  unmarshallString(src, srcLen, request.label);
  unmarshallString(src, srcLen, request.devtype);
  unmarshallString(src, srcLen, request.density);
  unmarshallString(src, srcLen, request.unit);
  unmarshallUint32(src, srcLen, request.VolReqID);
  unmarshallUint32(src, srcLen, request.jobID);
  unmarshallUint32(src, srcLen, request.mode);
  unmarshallUint32(src, srcLen, request.start_file);
  unmarshallUint32(src, srcLen, request.end_file);
  unmarshallUint32(src, srcLen, request.side);
  unmarshallUint32(src, srcLen, request.tprc);
  unmarshallUint32(src, srcLen, request.TStartRequest);
  unmarshallUint32(src, srcLen, request.TEndRequest);
  unmarshallUint32(src, srcLen, request.TStartRtcpd);
  unmarshallUint32(src, srcLen, request.TStartMount);
  unmarshallUint32(src, srcLen, request.TEndMount);
  unmarshallUint32(src, srcLen, request.TStartUnmount);
  unmarshallUint32(src, srcLen, request.TEndUnmount);
  unmarshallUint32(src, srcLen, request.rtcpReqId.time_low);
  unmarshallUint16(src, srcLen, request.rtcpReqId.time_mid);
  unmarshallUint16(src, srcLen, request.rtcpReqId.time_hi_and_version);
  unmarshallUint8(src, srcLen, request.rtcpReqId.clock_seq_hi_and_reserved);
  unmarshallUint8(src, srcLen, request.rtcpReqId.clock_seq_low);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[0]);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[1]);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[2]);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[3]);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[4]);
  unmarshallUint8(src, srcLen, request.rtcpReqId.node[5]);
  unmarshallString(src, srcLen, request.err.errmsgtxt);
  unmarshallUint32(src, srcLen, request.err.severity);
  unmarshallUint32(src, srcLen, request.err.errorcode);
  unmarshallUint32(src, srcLen, request.err.max_tpretry);
  unmarshallUint32(src, srcLen, request.err.max_cpretry);
*/
}
