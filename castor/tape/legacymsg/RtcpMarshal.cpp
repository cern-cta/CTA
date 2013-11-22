/******************************************************************************
 *                      castor/tape/legacymsg/RtcpMarshal.cpp
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
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <iostream>
#include <string.h>


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *const dst,
  const size_t dstLen, const RtcpJobRqstMsgBody &src)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
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
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for job submission request message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  marshalUint32(RTCOPY_MAGIC_OLD0  , p); // Magic number
  marshalUint32(VDQM_CLIENTINFO    , p); // Request type
  marshalUint32(len                , p); // Length of message body
  marshalUint32(src.volReqId  , p);
  marshalUint32(src.clientPort     , p);
  marshalUint32(src.clientEuid     , p);
  marshalUint32(src.clientEgid     , p);
  marshalString(src.clientHost     , p);
  marshalString(src.dgn            , p);
  marshalString(src.driveUnit      , p);
  marshalString(src.clientUserName , p);

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
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpJobRqstMsgBody &dst) throw(castor::exception::Exception) {

  unmarshalUint32(src, srcLen, dst.volReqId);
  unmarshalUint32(src, srcLen, dst.clientPort);
  unmarshalUint32(src, srcLen, dst.clientEuid);
  unmarshalUint32(src, srcLen, dst.clientEgid);
  unmarshalString(src, srcLen, dst.clientHost);
  unmarshalString(src, srcLen, dst.dgn);
  unmarshalString(src, srcLen, dst.driveUnit);
  unmarshalString(src, srcLen, dst.clientUserName);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *const dst,
  const size_t dstLen, const RtcpJobReplyMsgBody &src)
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
  marshalUint32(RTCOPY_MAGIC_OLD0, p); // Magic number
  marshalUint32(VDQM_CLIENTINFO  , p); // Request type
  marshalUint32(len              , p); // Length
  marshalUint32(src.status       , p); // status code

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
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpJobReplyMsgBody &dst) throw(castor::exception::Exception) {

  // Unmarshal the status number
  legacymsg::unmarshalInt32(src, srcLen, dst.status);

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


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *dst,
  const size_t dstLen, const RtcpTapeRqstErrMsgBody &src)
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
  marshalUint32(RTCOPY_MAGIC                          , p);
  marshalUint32(RTCP_TAPEERR_REQ                      , p);
  marshalUint32(len                                   , p);
  marshalString(src.vid                               , p);
  marshalString(src.vsn                               , p);
  marshalString(src.label                             , p);
  marshalString(src.devtype                           , p);
  marshalString(src.density                           , p);
  marshalString(src.unit                              , p);
  marshalUint32(src.volReqId                          , p);
  marshalUint32(src.jobId                             , p);
  marshalUint32(src.mode                              , p);
  marshalUint32(src.start_file                        , p);
  marshalUint32(src.end_file                          , p);
  marshalUint32(src.side                              , p);
  marshalUint32(src.tprc                              , p);
  marshalUint32(src.tStartRequest                     , p);
  marshalUint32(src.tEndRequest                       , p);
  marshalUint32(src.tStartRtcpd                       , p);
  marshalUint32(src.tStartMount                       , p);
  marshalUint32(src.tEndMount                         , p);
  marshalUint32(src.tStartUnmount                     , p);
  marshalUint32(src.tEndUnmount                       , p);
  marshalUint32(src.rtcpReqId.time_low                , p);
  marshalUint16(src.rtcpReqId.time_mid                , p);
  marshalUint16(src.rtcpReqId.time_hi_and_version     , p);
  marshalUint8(src.rtcpReqId.clock_seq_hi_and_reserved, p);
  marshalUint8(src.rtcpReqId.clock_seq_low            , p);
  marshalUint8(src.rtcpReqId.node[0]                  , p);
  marshalUint8(src.rtcpReqId.node[1]                  , p);
  marshalUint8(src.rtcpReqId.node[2]                  , p);
  marshalUint8(src.rtcpReqId.node[3]                  , p);
  marshalUint8(src.rtcpReqId.node[4]                  , p);
  marshalUint8(src.rtcpReqId.node[5]                  , p);
  marshalString(src.err.errorMsg                      , p);
  marshalUint32(src.err.severity                      , p);
  marshalUint32(src.err.errorCode                     , p);
  marshalUint32(src.err.maxTpRetry                    , p);
  marshalUint32(src.err.maxCpRetry                    , p);

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
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpTapeRqstErrMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshal(src, srcLen, (RtcpTapeRqstMsgBody&)dst);
  unmarshalString(src, srcLen, dst.err.errorMsg);
  unmarshalUint32(src, srcLen, dst.err.severity);
  unmarshalUint32(src, srcLen, dst.err.errorCode);
  unmarshalInt32(src, srcLen, dst.err.maxTpRetry);
  unmarshalInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpTapeRqstMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshalString(src, srcLen, dst.vid);
  unmarshalString(src, srcLen, dst.vsn);
  unmarshalString(src, srcLen, dst.label);
  unmarshalString(src, srcLen, dst.devtype);
  unmarshalString(src, srcLen, dst.density);
  unmarshalString(src, srcLen, dst.unit);
  unmarshalUint32(src, srcLen, dst.volReqId);
  unmarshalUint32(src, srcLen, dst.jobId);
  unmarshalUint32(src, srcLen, dst.mode);
  unmarshalUint32(src, srcLen, dst.start_file);
  unmarshalUint32(src, srcLen, dst.end_file);
  unmarshalUint32(src, srcLen, dst.side);
  unmarshalUint32(src, srcLen, dst.tprc);
  unmarshalUint32(src, srcLen, dst.tStartRequest);
  unmarshalUint32(src, srcLen, dst.tEndRequest);
  unmarshalUint32(src, srcLen, dst.tStartRtcpd);
  unmarshalUint32(src, srcLen, dst.tStartMount);
  unmarshalUint32(src, srcLen, dst.tEndMount);
  unmarshalUint32(src, srcLen, dst.tStartUnmount);
  unmarshalUint32(src, srcLen, dst.tEndUnmount);
  unmarshalUint32(src, srcLen, dst.rtcpReqId.time_low);
  unmarshalUint16(src, srcLen, dst.rtcpReqId.time_mid);
  unmarshalUint16(src, srcLen, dst.rtcpReqId.time_hi_and_version);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.clock_seq_hi_and_reserved);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.clock_seq_low);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[0]);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[1]);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[2]);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[3]);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[4]);
  unmarshalUint8(src, srcLen, dst.rtcpReqId.node[5]);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *dst,
  const size_t dstLen, const RtcpFileRqstErrMsgBody &src)
  throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len =
    // Fields before segAttr
    strlen(src.rqst.filePath)  +
    strlen(src.rqst.tapePath)  +
    strlen(src.rqst.fid)       +
    strlen(src.rqst.ifce)      +
    strlen(src.rqst.stageId)   +
    23 * sizeof(uint32_t) + // Number of 32-bit integers before segAttr
    4                     + // 4 = blockId[4]
    8 * sizeof(uint64_t)  + // Number of 64-bit integers before segAttr
    6                     + // Number of string terminators before segAttr

    // segAttr
    strlen(src.rqst.segAttr.nameServerHostName) +
    strlen(src.rqst.segAttr.segmCksumAlgorithm) +
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

  marshalUint32(RTCOPY_MAGIC                              , p);
  marshalUint32(RTCP_FILEERR_REQ                          , p);
  marshalUint32(len                                       , p);
  marshalString(src.rqst.filePath                         , p);
  marshalString(src.rqst.tapePath                         , p);
  marshalString(src.rqst.fid                              , p);
  marshalString(src.rqst.ifce                             , p);
  marshalString(src.rqst.stageId                          , p);
  marshalUint32(src.rqst.volReqId                         , p);
  marshalUint32(src.rqst.jobId                            , p);
  marshalUint32(src.rqst.stageSubReqId                    , p);
  marshalUint32(src.rqst.umask                            , p);
  marshalUint32(src.rqst.positionMethod                   , p);
  marshalUint32(src.rqst.tapeFseq                         , p);
  marshalUint32(src.rqst.diskFseq                         , p);
  marshalUint32(src.rqst.blockSize                        , p);
  marshalUint32(src.rqst.recordLength                     , p);
  marshalUint32(src.rqst.retention                        , p);
  marshalUint32(src.rqst.defAlloc                         , p);
  marshalUint32(src.rqst.rtcpErrAction                    , p);
  marshalUint32(src.rqst.tpErrAction                      , p);
  marshalUint32(src.rqst.checkFid                         , p);
  marshalUint32(src.rqst.concat                           , p);
  marshalUint32(src.rqst.procStatus                       , p);
  marshalInt32(src.rqst.cprc                              , p);
  marshalUint32(src.rqst.tStartPosition                   , p);
  marshalUint32(src.rqst.tEndPosition                     , p);
  marshalUint32(src.rqst.tStartTransferDisk               , p);
  marshalUint32(src.rqst.tEndTransferDisk                 , p);
  marshalUint32(src.rqst.tStartTransferTape               , p);
  marshalUint32(src.rqst.tEndTransferTape                 , p);
  marshalUint8(src.rqst.blockId[0]                        , p);
  marshalUint8(src.rqst.blockId[1]                        , p);
  marshalUint8(src.rqst.blockId[2]                        , p);
  marshalUint8(src.rqst.blockId[3]                        , p);
  marshalUint64(src.rqst.offset                           , p);
  marshalUint64(src.rqst.bytesIn                          , p);
  marshalUint64(src.rqst.bytesOut                         , p);
  marshalUint64(src.rqst.hostBytes                        , p);
  marshalUint64(src.rqst.nbRecs                           , p);
  marshalUint64(src.rqst.maxNbRec                         , p);
  marshalUint64(src.rqst.maxSize                          , p);
  marshalUint64(src.rqst.startSize                        , p);
  marshalString(src.rqst.segAttr.nameServerHostName       , p);
  marshalString(src.rqst.segAttr.segmCksumAlgorithm       , p);
  marshalUint32(src.rqst.segAttr.segmCksum                , p);
  marshalUint64(src.rqst.segAttr.castorFileId             , p);
  marshalUint32(src.rqst.stgReqId.time_low                , p);
  marshalUint16(src.rqst.stgReqId.time_mid                , p);
  marshalUint16(src.rqst.stgReqId.time_hi_and_version     , p);
  marshalUint8(src.rqst.stgReqId.clock_seq_hi_and_reserved, p);
  marshalUint8(src.rqst.stgReqId.clock_seq_low            , p);
  marshalUint8(src.rqst.stgReqId.node[0]                  , p);
  marshalUint8(src.rqst.stgReqId.node[1]                  , p);
  marshalUint8(src.rqst.stgReqId.node[2]                  , p);
  marshalUint8(src.rqst.stgReqId.node[3]                  , p);
  marshalUint8(src.rqst.stgReqId.node[4]                  , p);
  marshalUint8(src.rqst.stgReqId.node[5]                  , p);
  marshalString(src.err.errorMsg                          , p);
  marshalUint32(src.err.severity                          , p);
  marshalUint32(src.err.errorCode                         , p);
  marshalUint32(src.err.maxTpRetry                        , p);
  marshalUint32(src.err.maxCpRetry                        , p);

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
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpFileRqstErrMsgBody &dst)
  throw(castor::exception::Exception) {
  unmarshal(src, srcLen, (RtcpFileRqstMsgBody&)dst);
  unmarshalString(src, srcLen, dst.err.errorMsg);
  unmarshalUint32(src, srcLen, dst.err.severity);
  unmarshalUint32(src, srcLen, dst.err.errorCode);
  unmarshalInt32(src, srcLen, dst.err.maxTpRetry);
  unmarshalInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpFileRqstMsgBody &dst)
  throw(castor::exception::Exception) {

  unmarshalString(src, srcLen, dst.rqst.filePath);
  unmarshalString(src, srcLen, dst.rqst.tapePath);
  unmarshalString(src, srcLen, dst.rqst.fid);
  unmarshalString(src, srcLen, dst.rqst.ifce);
  unmarshalString(src, srcLen, dst.rqst.stageId);
  unmarshalUint32(src, srcLen, dst.rqst.volReqId);
  unmarshalInt32(src, srcLen, dst.rqst.jobId);
  unmarshalInt32(src, srcLen, dst.rqst.stageSubReqId);
  unmarshalUint32(src, srcLen, dst.rqst.umask);
  unmarshalInt32(src, srcLen, dst.rqst.positionMethod);
  unmarshalInt32(src, srcLen, dst.rqst.tapeFseq);
  unmarshalInt32(src, srcLen, dst.rqst.diskFseq);
  unmarshalInt32(src, srcLen, dst.rqst.blockSize);
  unmarshalInt32(src, srcLen, dst.rqst.recordLength);
  unmarshalInt32(src, srcLen, dst.rqst.retention);
  unmarshalInt32(src, srcLen, dst.rqst.defAlloc);
  unmarshalInt32(src, srcLen, dst.rqst.rtcpErrAction);
  unmarshalInt32(src, srcLen, dst.rqst.tpErrAction);
  unmarshalInt32(src, srcLen, dst.rqst.checkFid);
  unmarshalUint32(src, srcLen, dst.rqst.concat);
  unmarshalUint32(src, srcLen, dst.rqst.procStatus);
  unmarshalInt32(src, srcLen, dst.rqst.cprc);
  unmarshalUint32(src, srcLen, dst.rqst.tStartPosition);
  unmarshalUint32(src, srcLen, dst.rqst.tEndPosition);
  unmarshalUint32(src, srcLen, dst.rqst.tStartTransferDisk);
  unmarshalUint32(src, srcLen, dst.rqst.tEndTransferDisk);
  unmarshalUint32(src, srcLen, dst.rqst.tStartTransferTape);
  unmarshalUint32(src, srcLen, dst.rqst.tEndTransferTape);
  unmarshalUint8(src, srcLen, dst.rqst.blockId[0]);
  unmarshalUint8(src, srcLen, dst.rqst.blockId[1]);
  unmarshalUint8(src, srcLen, dst.rqst.blockId[2]);
  unmarshalUint8(src, srcLen, dst.rqst.blockId[3]);
  unmarshalUint64(src, srcLen, dst.rqst.offset);
  unmarshalUint64(src, srcLen, dst.rqst.bytesIn);
  unmarshalUint64(src, srcLen, dst.rqst.bytesOut);
  unmarshalUint64(src, srcLen, dst.rqst.hostBytes);
  unmarshalUint64(src, srcLen, dst.rqst.nbRecs);
  unmarshalUint64(src, srcLen, dst.rqst.maxNbRec);
  unmarshalUint64(src, srcLen, dst.rqst.maxSize);
  unmarshalUint64(src, srcLen, dst.rqst.startSize);
  unmarshalString(src, srcLen, dst.rqst.segAttr.nameServerHostName);
  unmarshalString(src, srcLen, dst.rqst.segAttr.segmCksumAlgorithm);
  unmarshalUint32(src, srcLen, dst.rqst.segAttr.segmCksum);
  unmarshalUint64(src, srcLen, dst.rqst.segAttr.castorFileId);
  unmarshalUint32(src, srcLen, dst.rqst.stgReqId.time_low);
  unmarshalUint16(src, srcLen, dst.rqst.stgReqId.time_mid);
  unmarshalUint16(src, srcLen, dst.rqst.stgReqId.time_hi_and_version);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.clock_seq_hi_and_reserved);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.clock_seq_low);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[0]);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[1]);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[2]);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[3]);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[4]);
  unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[5]);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, GiveOutpMsgBody &dst) throw(castor::exception::Exception) {

  unmarshalString(src, srcLen, dst.message);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *dst,
  const size_t dstLen, const RtcpNoMoreRequestsMsgBody&)
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
  marshalUint32(RTCOPY_MAGIC   , p); // Magic number
  marshalUint32(RTCP_NOMORE_REQ, p); // Request type
  marshalUint32(len            , p); // Length of message body

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
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *dst,
  const size_t dstLen, const RtcpAbortMsgBody&)
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
  marshalUint32(RTCOPY_MAGIC      , p);
  marshalUint32(RTCP_ABORT_REQ    , p);
  marshalUint32(len               , p);

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
// marshal
//-----------------------------------------------------------------------------
size_t castor::tape::legacymsg::marshal(char *dst,
  const size_t dstLen, const RtcpDumpTapeRqstMsgBody &src)
  throw(castor::exception::Exception) {

  // Calculate the length of the message body
  const uint32_t len = 7 * sizeof(int32_t);

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
  marshalUint32(RTCOPY_MAGIC     , p);
  marshalUint32(RTCP_DUMPTAPE_REQ, p);
  marshalUint32(len              , p);
  marshalUint32(src.maxBytes     , p);
  marshalUint32(src.blockSize    , p);
  marshalUint32(src.tapeErrAction, p);
  marshalUint32(src.startFile    , p);
  marshalUint32(src.maxFiles     , p);
  marshalUint32(src.fromBlock    , p);
  marshalUint32(src.toBlock      , p);

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
