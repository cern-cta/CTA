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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <iostream>
#include <string.h>


//------------------------------------------------------------------------------
// marshallUint8
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint8(uint8_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshallInteger(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint8
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint8(const char * &src,
  size_t &srcLen, uint8_t &dst) throw(castor::exception::Exception) {

  unmarshallInteger(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallUint16
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint16(uint16_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshallInteger(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint16
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint16(const char * &src,
  size_t &srcLen, uint16_t &dst) throw(castor::exception::Exception) {

  unmarshallInteger(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint32(uint32_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshallInteger(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint32(const char * &src,
  size_t &srcLen, uint32_t &dst) throw(castor::exception::Exception) {

  unmarshallInteger(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallInt32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallInt32(int32_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshallInteger(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallInt32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallInt32(const char * &src,
  size_t &srcLen, int32_t &dst) throw(castor::exception::Exception) {

  unmarshallInteger(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallUint64
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint64(uint64_t src,
  char * &dst) throw(castor::exception::Exception) {

  marshallInteger(src, dst);
}


//------------------------------------------------------------------------------
// unmarshallUint64
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint64(const char * &src,
  size_t &srcLen, uint64_t &dst) throw(castor::exception::Exception) {

  unmarshallInteger(src, srcLen, dst);
}


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallString(const char * src,
  char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
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
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  strcpy(dst, src.c_str());

  dst += strlen(src.c_str()) + 1;
}


//------------------------------------------------------------------------------
// unmarshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallString(const char * &src,
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
// marshallMessageHeader
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallMessageHeader(
  char *const dst, const size_t dstLen, const MessageHeader &src)
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

  // Marshall the message header
  char *p = dst;
  marshallUint32(src.magic      , p);
  marshallUint32(src.reqType    , p);
  marshallUint32(src.lenOrStatus, p);

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
// unmarshallMessageHeader
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallMessageHeader(
  const char * &src, size_t &srcLen, MessageHeader &dst)
  throw(castor::exception::Exception) {

  unmarshallUint32(src, srcLen, dst.magic);
  unmarshallUint32(src, srcLen, dst.reqType);
  unmarshallUint32(src, srcLen, dst.lenOrStatus);
}


//-----------------------------------------------------------------------------
// marshallRcpJobRqstMsgBody
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRcpJobRqstMsgBody(
  char *const dst, const size_t dstLen, const RcpJobRqstMsgBody &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  // Calculate the length of the message body
  const uint32_t len =
    4*sizeof(uint32_t)          +
    strlen(src.clientHost)      +
    strlen(src.deviceGroupName) +
    strlen(src.driveName)       +
    strlen(src.clientUserName)  +
    4; // 4 = the number of string termination characters

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for job submission request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC_OLD0  , p); // Magic number
  marshallUint32(VDQM_CLIENTINFO    , p); // Request type
  marshallUint32(len                , p); // Length of message body
  marshallUint32(src.tapeRequestId  , p);
  marshallUint32(src.clientPort     , p);
  marshallUint32(src.clientEuid     , p);
  marshallUint32(src.clientEgid     , p);
  marshallString(src.clientHost     , p);
  marshallString(src.deviceGroupName, p);
  marshallString(src.driveName      , p);
  marshallString(src.clientUserName , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RCP job submission request message and the actual number of bytes "
      "marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRcpJobRqstMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRcpJobRqstMsgBody(
  const char * &src, size_t &srcLen, RcpJobRqstMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshallUint32(src, srcLen, dst.tapeRequestId);
  unmarshallUint32(src, srcLen, dst.clientPort);
  unmarshallUint32(src, srcLen, dst.clientEuid);
  unmarshallUint32(src, srcLen, dst.clientEgid);
  unmarshallString(src, srcLen, dst.clientHost);
  unmarshallString(src, srcLen, dst.deviceGroupName);
  unmarshallString(src, srcLen, dst.driveName);
  unmarshallString(src, srcLen, dst.clientUserName);
}


//-----------------------------------------------------------------------------
// marshallRcpJobReplyMsgBody
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRcpJobReplyMsgBody(
  char *const dst, const size_t dstLen, const RcpJobReplyMsgBody &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  // Minimum buffer length = magic number + request type + length + status code
  // + terminating null character
  const size_t minimumLen = 4*sizeof(uint32_t) + 1;
  if(dstLen < minimumLen) {
    TAPE_THROW_CODE(EINVAL,
      ": Destination buffer length is too small"
      ": Expected minimum: " << minimumLen <<
      ": Actual: " << dstLen);
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
  marshallUint32(RTCOPY_MAGIC_OLD0, p); // Magic number
  marshallUint32(VDQM_CLIENTINFO  , p); // Request type
  marshallUint32(len              , p); // Length
  marshallUint32(src.status       , p); // status code

  strncpy(p, src.errorMessage, errLenToSend);
  *(p+errLenToSend) = '0';
  p = p + errLenToSend + 1;

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RCP job reply message and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRcpJobReplyMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRcpJobReplyMsgBody(
  const char * &src, size_t &srcLen, RcpJobReplyMsgBody &dst)
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
// marshallRtcpTapeRqstErrMsgBody
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpTapeRqstErrMsgBody(
  char *dst, const size_t dstLen, const RtcpTapeRqstErrMsgBody &src)
  throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len =
    14 * sizeof(uint32_t)     +
    strlen(src.vid)           +
    strlen(src.vsn)           +
    strlen(src.label)         +
    strlen(src.devtype)       +
    strlen(src.density)       +
    strlen(src.unit)          +
    sizeof(Cuuid_t)           +
    4 * sizeof(uint32_t)      + // 4 uint32_t's of RtcpErrorAppendix
    strlen(src.err.errorMsg)  +
    7;                          // 7 = number of null terminator characters

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for tape request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC                          , p);
  marshallUint32(RTCP_TAPEERR_REQ                      , p);
  marshallUint32(len                                   , p);
  marshallString(src.vid                               , p);
  marshallString(src.vsn                               , p);
  marshallString(src.label                             , p);
  marshallString(src.devtype                           , p);
  marshallString(src.density                           , p);
  marshallString(src.unit                              , p);
  marshallUint32(src.volReqId                          , p);
  marshallUint32(src.jobId                             , p);
  marshallUint32(src.mode                              , p);
  marshallUint32(src.start_file                        , p);
  marshallUint32(src.end_file                          , p);
  marshallUint32(src.side                              , p);
  marshallUint32(src.tprc                              , p);
  marshallUint32(src.tStartRequest                     , p);
  marshallUint32(src.tEndRequest                       , p);
  marshallUint32(src.tStartRtcpd                       , p);
  marshallUint32(src.tStartMount                       , p);
  marshallUint32(src.tEndMount                         , p);
  marshallUint32(src.tStartUnmount                     , p);
  marshallUint32(src.tEndUnmount                       , p);
  marshallUint32(src.rtcpReqId.time_low                , p);
  marshallUint16(src.rtcpReqId.time_mid                , p);
  marshallUint16(src.rtcpReqId.time_hi_and_version     , p);
  marshallUint8(src.rtcpReqId.clock_seq_hi_and_reserved, p);
  marshallUint8(src.rtcpReqId.clock_seq_low            , p);
  marshallUint8(src.rtcpReqId.node[0]                  , p);
  marshallUint8(src.rtcpReqId.node[1]                  , p);
  marshallUint8(src.rtcpReqId.node[2]                  , p);
  marshallUint8(src.rtcpReqId.node[3]                  , p);
  marshallUint8(src.rtcpReqId.node[4]                  , p);
  marshallUint8(src.rtcpReqId.node[5]                  , p);
  marshallString(src.err.errorMsg                      , p);
  marshallUint32(src.err.severity                      , p);
  marshallUint32(src.err.errorCode                     , p);
  marshallUint32(src.err.maxTpRetry                    , p);
  marshallUint32(src.err.maxCpRetry                    , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RTCP tape request message and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRtcpTapeRqstErrMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRtcpTapeRqstErrMsgBody(
  const char * &src, size_t &srcLen, RtcpTapeRqstErrMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshallRtcpTapeRqstMsgBody(src, srcLen, (RtcpTapeRqstMsgBody&)dst);
  unmarshallString(src, srcLen, dst.err.errorMsg);
  unmarshallUint32(src, srcLen, dst.err.severity);
  unmarshallUint32(src, srcLen, dst.err.errorCode);
  unmarshallInt32(src, srcLen, dst.err.maxTpRetry);
  unmarshallInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshallRtcpTapeRqstMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRtcpTapeRqstMsgBody(
  const char * &src, size_t &srcLen, RtcpTapeRqstMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshallString(src, srcLen, dst.vid);
  unmarshallString(src, srcLen, dst.vsn);
  unmarshallString(src, srcLen, dst.label);
  unmarshallString(src, srcLen, dst.devtype);
  unmarshallString(src, srcLen, dst.density);
  unmarshallString(src, srcLen, dst.unit);
  unmarshallUint32(src, srcLen, dst.volReqId);
  unmarshallUint32(src, srcLen, dst.jobId);
  unmarshallUint32(src, srcLen, dst.mode);
  unmarshallUint32(src, srcLen, dst.start_file);
  unmarshallUint32(src, srcLen, dst.end_file);
  unmarshallUint32(src, srcLen, dst.side);
  unmarshallUint32(src, srcLen, dst.tprc);
  unmarshallUint32(src, srcLen, dst.tStartRequest);
  unmarshallUint32(src, srcLen, dst.tEndRequest);
  unmarshallUint32(src, srcLen, dst.tStartRtcpd);
  unmarshallUint32(src, srcLen, dst.tStartMount);
  unmarshallUint32(src, srcLen, dst.tEndMount);
  unmarshallUint32(src, srcLen, dst.tStartUnmount);
  unmarshallUint32(src, srcLen, dst.tEndUnmount);
  unmarshallUint32(src, srcLen, dst.rtcpReqId.time_low);
  unmarshallUint16(src, srcLen, dst.rtcpReqId.time_mid);
  unmarshallUint16(src, srcLen, dst.rtcpReqId.time_hi_and_version);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.clock_seq_hi_and_reserved);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.clock_seq_low);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[0]);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[1]);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[2]);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[3]);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[4]);
  unmarshallUint8(src, srcLen, dst.rtcpReqId.node[5]);
}


//-----------------------------------------------------------------------------
// marshallRtcpFileRqstErrMsgBody
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpFileRqstErrMsgBody(
  char *dst, const size_t dstLen, const RtcpFileRqstErrMsgBody &src)
  throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len =
    // Fields before segAttr
    strlen(src.filePath)  +
    strlen(src.tapePath)  +
    strlen(src.recfm)     +
    strlen(src.fid)       +
    strlen(src.ifce)      +
    strlen(src.stageId)   +
    24 * sizeof(uint32_t) + // Number of 32-bit integers before segAttr
    4                     + // 4 = blockId[4]
    8 * sizeof(uint64_t)  + // Number of 64-bit integers before segAttr
    6                     + // Number of string terminators before segAttr

    // segAttr
    strlen(src.segAttr.nameServerHostName) +
    strlen(src.segAttr.segmCksumAlgorithm) +
    sizeof(uint32_t)          + // 1 uint32_t of RtcpSegmentAttributes
    sizeof(uint64_t)          + // 1 uint64_t of RtcpSegmentAttributes
    2                         + // Number of segAttr string terminators

    // stgRedId
    sizeof(Cuuid_t)           +

    // err
    4 * sizeof(uint32_t)      + // 4 uint32_t's of RtcpErrorAppendix
    strlen(src.err.errorMsg)  +
    1;                          // err.errmsgtxt string terminator

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for file request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;

  marshallUint32(RTCOPY_MAGIC                         , p);
  marshallUint32(RTCP_FILEERR_REQ                     , p);
  marshallUint32(len                                  , p);
  marshallString(src.filePath                         , p);
  marshallString(src.tapePath                         , p);
  marshallString(src.recfm                            , p);
  marshallString(src.fid                              , p);
  marshallString(src.ifce                             , p);
  marshallString(src.stageId                          , p);
  marshallUint32(src.volReqId                         , p);
  marshallUint32(src.jobId                            , p);
  marshallUint32(src.stageSubReqId                    , p);
  marshallUint32(src.umask                            , p);
  marshallUint32(src.positionMethod                   , p);
  marshallUint32(src.tapeFseq                         , p);
  marshallUint32(src.diskFseq                         , p);
  marshallUint32(src.blockSize                        , p);
  marshallUint32(src.recordLength                     , p);
  marshallUint32(src.retention                        , p);
  marshallUint32(src.defAlloc                         , p);
  marshallUint32(src.rtcpErrAction                    , p);
  marshallUint32(src.tpErrAction                      , p);
  marshallUint32(src.convert                          , p);
  marshallUint32(src.checkFid                         , p);
  marshallUint32(src.concat                           , p);
  marshallUint32(src.procStatus                       , p);
  marshallUint32(src.cprc                             , p);
  marshallUint32(src.tStartPosition                   , p);
  marshallUint32(src.tEndPosition                     , p);
  marshallUint32(src.tStartTransferDisk               , p);
  marshallUint32(src.tEndTransferDisk                 , p);
  marshallUint32(src.tStartTransferTape               , p);
  marshallUint32(src.tEndTransferTape                 , p);
  marshallUint8(src.blockId[0]                        , p);
  marshallUint8(src.blockId[1]                        , p);
  marshallUint8(src.blockId[2]                        , p);
  marshallUint8(src.blockId[3]                        , p);
  marshallUint64(src.offset                           , p);
  marshallUint64(src.bytesIn                          , p);
  marshallUint64(src.bytesOut                         , p);
  marshallUint64(src.hostBytes                        , p);
  marshallUint64(src.nbRecs                           , p);
  marshallUint64(src.maxNbRec                         , p);
  marshallUint64(src.maxSize                          , p);
  marshallUint64(src.startSize                        , p);
  marshallString(src.segAttr.nameServerHostName       , p);
  marshallString(src.segAttr.segmCksumAlgorithm       , p);
  marshallUint32(src.segAttr.segmCksum                , p);
  marshallUint64(src.segAttr.castorFileId             , p);
  marshallUint32(src.stgReqId.time_low                , p);
  marshallUint16(src.stgReqId.time_mid                , p);
  marshallUint16(src.stgReqId.time_hi_and_version     , p);
  marshallUint8(src.stgReqId.clock_seq_hi_and_reserved, p);
  marshallUint8(src.stgReqId.clock_seq_low            , p);
  marshallUint8(src.stgReqId.node[0]                  , p);
  marshallUint8(src.stgReqId.node[1]                  , p);
  marshallUint8(src.stgReqId.node[2]                  , p);
  marshallUint8(src.stgReqId.node[3]                  , p);
  marshallUint8(src.stgReqId.node[4]                  , p);
  marshallUint8(src.stgReqId.node[5]                  , p);
  marshallString(src.err.errorMsg                     , p);
  marshallUint32(src.err.severity                     , p);
  marshallUint32(src.err.errorCode                    , p);
  marshallUint32(src.err.maxTpRetry                   , p);
  marshallUint32(src.err.maxCpRetry                   , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RTCP file request message and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshallRtcpFileRqstErrMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRtcpFileRqstErrMsgBody(
  const char * &src, size_t &srcLen, RtcpFileRqstErrMsgBody &dst)
  throw(castor::exception::Exception) {
  unmarshallRtcpFileRqstMsgBody(src, srcLen, (RtcpFileRqstMsgBody&)dst);
  unmarshallString(src, srcLen, dst.err.errorMsg);
  unmarshallUint32(src, srcLen, dst.err.severity);
  unmarshallUint32(src, srcLen, dst.err.errorCode);
  unmarshallInt32(src, srcLen, dst.err.maxTpRetry);
  unmarshallInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshallRtcpFileRqstMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallRtcpFileRqstMsgBody(
  const char * &src, size_t &srcLen, RtcpFileRqstMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshallString(src, srcLen, dst.filePath);
  unmarshallString(src, srcLen, dst.tapePath);
  unmarshallString(src, srcLen, dst.recfm);
  unmarshallString(src, srcLen, dst.fid);
  unmarshallString(src, srcLen, dst.ifce);
  unmarshallString(src, srcLen, dst.stageId);
  unmarshallUint32(src, srcLen, dst.volReqId);
  unmarshallInt32(src, srcLen, dst.jobId);
  unmarshallInt32(src, srcLen, dst.stageSubReqId);
  unmarshallUint32(src, srcLen, dst.umask);
  unmarshallInt32(src, srcLen, dst.positionMethod);
  unmarshallInt32(src, srcLen, dst.tapeFseq);
  unmarshallInt32(src, srcLen, dst.diskFseq);
  unmarshallInt32(src, srcLen, dst.blockSize);
  unmarshallInt32(src, srcLen, dst.recordLength);
  unmarshallInt32(src, srcLen, dst.retention);
  unmarshallInt32(src, srcLen, dst.defAlloc);
  unmarshallInt32(src, srcLen, dst.rtcpErrAction);
  unmarshallInt32(src, srcLen, dst.tpErrAction);
  unmarshallInt32(src, srcLen, dst.convert);
  unmarshallInt32(src, srcLen, dst.checkFid);
  unmarshallUint32(src, srcLen, dst.concat);
  unmarshallUint32(src, srcLen, dst.procStatus);
  unmarshallUint32(src, srcLen, dst.cprc);
  unmarshallUint32(src, srcLen, dst.tStartPosition);
  unmarshallUint32(src, srcLen, dst.tEndPosition);
  unmarshallUint32(src, srcLen, dst.tStartTransferDisk);
  unmarshallUint32(src, srcLen, dst.tEndTransferDisk);
  unmarshallUint32(src, srcLen, dst.tStartTransferTape);
  unmarshallUint32(src, srcLen, dst.tEndTransferTape);
  unmarshallUint8(src, srcLen, dst.blockId[0]);
  unmarshallUint8(src, srcLen, dst.blockId[1]);
  unmarshallUint8(src, srcLen, dst.blockId[2]);
  unmarshallUint8(src, srcLen, dst.blockId[3]);
  unmarshallUint64(src, srcLen, dst.offset);
  unmarshallUint64(src, srcLen, dst.bytesIn);
  unmarshallUint64(src, srcLen, dst.bytesOut);
  unmarshallUint64(src, srcLen, dst.hostBytes);
  unmarshallUint64(src, srcLen, dst.nbRecs);
  unmarshallUint64(src, srcLen, dst.maxNbRec);
  unmarshallUint64(src, srcLen, dst.maxSize);
  unmarshallUint64(src, srcLen, dst.startSize);
  unmarshallString(src, srcLen, dst.segAttr.nameServerHostName);
  unmarshallString(src, srcLen, dst.segAttr.segmCksumAlgorithm);
  unmarshallUint32(src, srcLen, dst.segAttr.segmCksum);
  unmarshallUint64(src, srcLen, dst.segAttr.castorFileId);
  unmarshallUint32(src, srcLen, dst.stgReqId.time_low);
  unmarshallUint16(src, srcLen, dst.stgReqId.time_mid);
  unmarshallUint16(src, srcLen, dst.stgReqId.time_hi_and_version);
  unmarshallUint8(src, srcLen, dst.stgReqId.clock_seq_hi_and_reserved);
  unmarshallUint8(src, srcLen, dst.stgReqId.clock_seq_low);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[0]);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[1]);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[2]);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[3]);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[4]);
  unmarshallUint8(src, srcLen, dst.stgReqId.node[5]);
}


//-----------------------------------------------------------------------------
// unmarshallGiveOutpMsgBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallGiveOutpMsgBody(
  const char * &src, size_t &srcLen, GiveOutpMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshallString(src, srcLen, dst.message);
}


//-----------------------------------------------------------------------------
// marshallRtcpNoMoreRequestsMsgBody
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpNoMoreRequestsMsgBody(
  char *dst, const size_t dstLen)
  throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len = 0;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for file request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC   , p); // Magic number
  marshallUint32(RTCP_NOMORE_REQ, p); // Request type
  marshallUint32(len            , p); // Length of message body

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RTCP no more requests message and the actual number of bytes "
      "marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// marshallRtcpAbortMsg
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::Marshaller::marshallRtcpAbortMsg(
  char *dst, const size_t dstLen) throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len = 0;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for file request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshallUint32(RTCOPY_MAGIC      , p);
  marshallUint32(RTCP_ABORT_REQ    , p);
  marshallUint32(len               , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "RTCP file request message and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}

