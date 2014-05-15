/******************************************************************************
 *                      castor/legacymsg/RtcpMarshal.cpp
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

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <iostream>
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


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *dst, const size_t dstLen,
  const RtcpTapeRqstErrMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpTapeRqstErrMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

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
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpTapeRqstErrMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(RTCOPY_MAGIC                          , p);
  io::marshalUint32(RTCP_TAPEERR_REQ                      , p);
  io::marshalUint32(len                                   , p);
  io::marshalString(src.vid                               , p);
  io::marshalString(src.vsn                               , p);
  io::marshalString(src.label                             , p);
  io::marshalString(src.devtype                           , p);
  io::marshalString(src.density                           , p);
  io::marshalString(src.unit                              , p);
  io::marshalUint32(src.volReqId                          , p);
  io::marshalUint32(src.jobId                             , p);
  io::marshalUint32(src.mode                              , p);
  io::marshalUint32(src.start_file                        , p);
  io::marshalUint32(src.end_file                          , p);
  io::marshalUint32(src.side                              , p);
  io::marshalUint32(src.tprc                              , p);
  io::marshalUint32(src.tStartRequest                     , p);
  io::marshalUint32(src.tEndRequest                       , p);
  io::marshalUint32(src.tStartRtcpd                       , p);
  io::marshalUint32(src.tStartMount                       , p);
  io::marshalUint32(src.tEndMount                         , p);
  io::marshalUint32(src.tStartUnmount                     , p);
  io::marshalUint32(src.tEndUnmount                       , p);
  io::marshalUint32(src.rtcpReqId.time_low                , p);
  io::marshalUint16(src.rtcpReqId.time_mid                , p);
  io::marshalUint16(src.rtcpReqId.time_hi_and_version     , p);
  io::marshalUint8(src.rtcpReqId.clock_seq_hi_and_reserved, p);
  io::marshalUint8(src.rtcpReqId.clock_seq_low            , p);
  io::marshalUint8(src.rtcpReqId.node[0]                  , p);
  io::marshalUint8(src.rtcpReqId.node[1]                  , p);
  io::marshalUint8(src.rtcpReqId.node[2]                  , p);
  io::marshalUint8(src.rtcpReqId.node[3]                  , p);
  io::marshalUint8(src.rtcpReqId.node[4]                  , p);
  io::marshalUint8(src.rtcpReqId.node[5]                  , p);
  io::marshalString(src.err.errorMsg                      , p);
  io::marshalUint32(src.err.severity                      , p);
  io::marshalUint32(src.err.errorCode                     , p);
  io::marshalUint32(src.err.maxTpRetry                    , p);
  io::marshalUint32(src.err.maxCpRetry                    , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpTapeRqstErrMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  RtcpTapeRqstErrMsgBody &dst)  {

  unmarshal(src, srcLen, (RtcpTapeRqstMsgBody&)dst);
  io::unmarshalString(src, srcLen, dst.err.errorMsg);
  io::unmarshalUint32(src, srcLen, dst.err.severity);
  io::unmarshalUint32(src, srcLen, dst.err.errorCode);
  io::unmarshalInt32(src, srcLen, dst.err.maxTpRetry);
  io::unmarshalInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src, size_t &srcLen,
  RtcpTapeRqstMsgBody &dst)  {

  io::unmarshalString(src, srcLen, dst.vid);
  io::unmarshalString(src, srcLen, dst.vsn);
  io::unmarshalString(src, srcLen, dst.label);
  io::unmarshalString(src, srcLen, dst.devtype);
  io::unmarshalString(src, srcLen, dst.density);
  io::unmarshalString(src, srcLen, dst.unit);
  io::unmarshalUint32(src, srcLen, dst.volReqId);
  io::unmarshalUint32(src, srcLen, dst.jobId);
  io::unmarshalUint32(src, srcLen, dst.mode);
  io::unmarshalUint32(src, srcLen, dst.start_file);
  io::unmarshalUint32(src, srcLen, dst.end_file);
  io::unmarshalUint32(src, srcLen, dst.side);
  io::unmarshalUint32(src, srcLen, dst.tprc);
  io::unmarshalUint32(src, srcLen, dst.tStartRequest);
  io::unmarshalUint32(src, srcLen, dst.tEndRequest);
  io::unmarshalUint32(src, srcLen, dst.tStartRtcpd);
  io::unmarshalUint32(src, srcLen, dst.tStartMount);
  io::unmarshalUint32(src, srcLen, dst.tEndMount);
  io::unmarshalUint32(src, srcLen, dst.tStartUnmount);
  io::unmarshalUint32(src, srcLen, dst.tEndUnmount);
  io::unmarshalUint32(src, srcLen, dst.rtcpReqId.time_low);
  io::unmarshalUint16(src, srcLen, dst.rtcpReqId.time_mid);
  io::unmarshalUint16(src, srcLen, dst.rtcpReqId.time_hi_and_version);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.clock_seq_hi_and_reserved);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.clock_seq_low);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[0]);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[1]);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[2]);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[3]);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[4]);
  io::unmarshalUint8(src, srcLen, dst.rtcpReqId.node[5]);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *dst, const size_t dstLen,
  const RtcpFileRqstErrMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpFileRqstErrMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len =
    // Fields before segAttr
    strlen(src.rqst.filePath)  +
    strlen(src.rqst.tapePath)  +
    strlen(src.rqst.recfm_noLongerUsed) +
    strlen(src.rqst.fid)       +
    strlen(src.rqst.ifce)      +
    strlen(src.rqst.stageId)   +
    24 * sizeof(uint32_t) + // Number of 32-bit integers before segAttr
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
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpFileRqstErrMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;

  io::marshalUint32(RTCOPY_MAGIC                              , p);
  io::marshalUint32(RTCP_FILEERR_REQ                          , p);
  io::marshalUint32(len                                       , p);
  io::marshalString(src.rqst.filePath                         , p);
  io::marshalString(src.rqst.tapePath                         , p);
  io::marshalString(src.rqst.recfm_noLongerUsed               , p);
  io::marshalString(src.rqst.fid                              , p);
  io::marshalString(src.rqst.ifce                             , p);
  io::marshalString(src.rqst.stageId                          , p);
  io::marshalUint32(src.rqst.volReqId                         , p);
  io::marshalUint32(src.rqst.jobId                            , p);
  io::marshalUint32(src.rqst.stageSubReqId                    , p);
  io::marshalUint32(src.rqst.umask                            , p);
  io::marshalUint32(src.rqst.positionMethod                   , p);
  io::marshalUint32(src.rqst.tapeFseq                         , p);
  io::marshalUint32(src.rqst.diskFseq                         , p);
  io::marshalUint32(src.rqst.blockSize                        , p);
  io::marshalUint32(src.rqst.recordLength                     , p);
  io::marshalUint32(src.rqst.retention                        , p);
  io::marshalUint32(src.rqst.defAlloc                         , p);
  io::marshalUint32(src.rqst.rtcpErrAction                    , p);
  io::marshalUint32(src.rqst.tpErrAction                      , p);
  io::marshalUint32(src.rqst.convert_noLongerUsed             , p);
  io::marshalUint32(src.rqst.checkFid                         , p);
  io::marshalUint32(src.rqst.concat                           , p);
  io::marshalUint32(src.rqst.procStatus                       , p);
  io::marshalUint32(src.rqst.cprc                              , p);
  io::marshalUint32(src.rqst.tStartPosition                   , p);
  io::marshalUint32(src.rqst.tEndPosition                     , p);
  io::marshalUint32(src.rqst.tStartTransferDisk               , p);
  io::marshalUint32(src.rqst.tEndTransferDisk                 , p);
  io::marshalUint32(src.rqst.tStartTransferTape               , p);
  io::marshalUint32(src.rqst.tEndTransferTape                 , p);
  io::marshalUint8(src.rqst.blockId[0]                        , p);
  io::marshalUint8(src.rqst.blockId[1]                        , p);
  io::marshalUint8(src.rqst.blockId[2]                        , p);
  io::marshalUint8(src.rqst.blockId[3]                        , p);
  io::marshalUint64(src.rqst.offset                           , p);
  io::marshalUint64(src.rqst.bytesIn                          , p);
  io::marshalUint64(src.rqst.bytesOut                         , p);
  io::marshalUint64(src.rqst.hostBytes                        , p);
  io::marshalUint64(src.rqst.nbRecs                           , p);
  io::marshalUint64(src.rqst.maxNbRec                         , p);
  io::marshalUint64(src.rqst.maxSize                          , p);
  io::marshalUint64(src.rqst.startSize                        , p);
  io::marshalString(src.rqst.segAttr.nameServerHostName       , p);
  io::marshalString(src.rqst.segAttr.segmCksumAlgorithm       , p);
  io::marshalUint32(src.rqst.segAttr.segmCksum                , p);
  io::marshalUint64(src.rqst.segAttr.castorFileId             , p);
  io::marshalUint32(src.rqst.stgReqId.time_low                , p);
  io::marshalUint16(src.rqst.stgReqId.time_mid                , p);
  io::marshalUint16(src.rqst.stgReqId.time_hi_and_version     , p);
  io::marshalUint8(src.rqst.stgReqId.clock_seq_hi_and_reserved, p);
  io::marshalUint8(src.rqst.stgReqId.clock_seq_low            , p);
  io::marshalUint8(src.rqst.stgReqId.node[0]                  , p);
  io::marshalUint8(src.rqst.stgReqId.node[1]                  , p);
  io::marshalUint8(src.rqst.stgReqId.node[2]                  , p);
  io::marshalUint8(src.rqst.stgReqId.node[3]                  , p);
  io::marshalUint8(src.rqst.stgReqId.node[4]                  , p);
  io::marshalUint8(src.rqst.stgReqId.node[5]                  , p);
  io::marshalString(src.err.errorMsg                          , p);
  io::marshalUint32(src.err.severity                          , p);
  io::marshalUint32(src.err.errorCode                         , p);
  io::marshalUint32(src.err.maxTpRetry                        , p);
  io::marshalUint32(src.err.maxCpRetry                        , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpFileRqstErrMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpFileRqstErrMsgBody &dst)
   {
  unmarshal(src, srcLen, (RtcpFileRqstMsgBody&)dst);
  io::unmarshalString(src, srcLen, dst.err.errorMsg);
  io::unmarshalUint32(src, srcLen, dst.err.severity);
  io::unmarshalUint32(src, srcLen, dst.err.errorCode);
  io::unmarshalInt32(src, srcLen, dst.err.maxTpRetry);
  io::unmarshalInt32(src, srcLen, dst.err.maxCpRetry);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, RtcpFileRqstMsgBody &dst)
   {

  io::unmarshalString(src, srcLen, dst.rqst.filePath);
  io::unmarshalString(src, srcLen, dst.rqst.tapePath);
  io::unmarshalString(src, srcLen, dst.rqst.recfm_noLongerUsed);
  io::unmarshalString(src, srcLen, dst.rqst.fid);
  io::unmarshalString(src, srcLen, dst.rqst.ifce);
  io::unmarshalString(src, srcLen, dst.rqst.stageId);
  io::unmarshalUint32(src, srcLen, dst.rqst.volReqId);
  io::unmarshalInt32(src, srcLen, dst.rqst.jobId);
  io::unmarshalInt32(src, srcLen, dst.rqst.stageSubReqId);
  io::unmarshalUint32(src, srcLen, dst.rqst.umask);
  io::unmarshalInt32(src, srcLen, dst.rqst.positionMethod);
  io::unmarshalInt32(src, srcLen, dst.rqst.tapeFseq);
  io::unmarshalInt32(src, srcLen, dst.rqst.diskFseq);
  io::unmarshalInt32(src, srcLen, dst.rqst.blockSize);
  io::unmarshalInt32(src, srcLen, dst.rqst.recordLength);
  io::unmarshalInt32(src, srcLen, dst.rqst.retention);
  io::unmarshalInt32(src, srcLen, dst.rqst.defAlloc);
  io::unmarshalInt32(src, srcLen, dst.rqst.rtcpErrAction);
  io::unmarshalInt32(src, srcLen, dst.rqst.tpErrAction);
  io::unmarshalInt32(src, srcLen, dst.rqst.convert_noLongerUsed);
  io::unmarshalInt32(src, srcLen, dst.rqst.checkFid);
  io::unmarshalUint32(src, srcLen, dst.rqst.concat);
  io::unmarshalUint32(src, srcLen, dst.rqst.procStatus);
  io::unmarshalInt32(src, srcLen, dst.rqst.cprc);
  io::unmarshalUint32(src, srcLen, dst.rqst.tStartPosition);
  io::unmarshalUint32(src, srcLen, dst.rqst.tEndPosition);
  io::unmarshalUint32(src, srcLen, dst.rqst.tStartTransferDisk);
  io::unmarshalUint32(src, srcLen, dst.rqst.tEndTransferDisk);
  io::unmarshalUint32(src, srcLen, dst.rqst.tStartTransferTape);
  io::unmarshalUint32(src, srcLen, dst.rqst.tEndTransferTape);
  io::unmarshalUint8(src, srcLen, dst.rqst.blockId[0]);
  io::unmarshalUint8(src, srcLen, dst.rqst.blockId[1]);
  io::unmarshalUint8(src, srcLen, dst.rqst.blockId[2]);
  io::unmarshalUint8(src, srcLen, dst.rqst.blockId[3]);
  io::unmarshalUint64(src, srcLen, dst.rqst.offset);
  io::unmarshalUint64(src, srcLen, dst.rqst.bytesIn);
  io::unmarshalUint64(src, srcLen, dst.rqst.bytesOut);
  io::unmarshalUint64(src, srcLen, dst.rqst.hostBytes);
  io::unmarshalUint64(src, srcLen, dst.rqst.nbRecs);
  io::unmarshalUint64(src, srcLen, dst.rqst.maxNbRec);
  io::unmarshalUint64(src, srcLen, dst.rqst.maxSize);
  io::unmarshalUint64(src, srcLen, dst.rqst.startSize);
  io::unmarshalString(src, srcLen, dst.rqst.segAttr.nameServerHostName);
  io::unmarshalString(src, srcLen, dst.rqst.segAttr.segmCksumAlgorithm);
  io::unmarshalUint32(src, srcLen, dst.rqst.segAttr.segmCksum);
  io::unmarshalUint64(src, srcLen, dst.rqst.segAttr.castorFileId);
  io::unmarshalUint32(src, srcLen, dst.rqst.stgReqId.time_low);
  io::unmarshalUint16(src, srcLen, dst.rqst.stgReqId.time_mid);
  io::unmarshalUint16(src, srcLen, dst.rqst.stgReqId.time_hi_and_version);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.clock_seq_hi_and_reserved);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.clock_seq_low);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[0]);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[1]);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[2]);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[3]);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[4]);
  io::unmarshalUint8(src, srcLen, dst.rqst.stgReqId.node[5]);
}


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, GiveOutpMsgBody &dst)  {

  io::unmarshalString(src, srcLen, dst.message);
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *dst, const size_t dstLen,
  const RtcpNoMoreRequestsMsgBody&)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpNoMoreRequestsMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len = 0;

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpNoMoreRequestsMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(RTCOPY_MAGIC   , p); // Magic number
  io::marshalUint32(RTCP_NOMORE_REQ, p); // Request type
  io::marshalUint32(len            , p); // Length of message body

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpNoMoreRequestsMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}


//-----------------------------------------------------------------------------
// marshal
//-----------------------------------------------------------------------------
size_t castor::legacymsg::marshal(char *dst, const size_t dstLen,
  const RtcpDumpTapeRqstMsgBody &src)  {

  if(dst == NULL) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpDumpTapeRqstMsgBody"
      ": Pointer to destination buffer is NULL";
    throw ex;
  }

  // Calculate the length of the message body
  const uint32_t len = 8 * sizeof(int32_t);

  // Calculate the total length of the message (header + body)
  // Message header = magic + reqType + len = 3 * sizeof(uint32_t)
  const size_t totalLen = 3 * sizeof(uint32_t) + len;

  // Check that the message buffer is big enough
  if(totalLen > dstLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpDumpTapeRqstMsgBody"
      ": Buffer too small: required=" << totalLen << " actual=" << dstLen;
    throw ex;
  }

  // Marshall the whole message (header + body)
  char *p = dst;
  io::marshalUint32(RTCOPY_MAGIC     , p);
  io::marshalUint32(RTCP_DUMPTAPE_REQ, p);
  io::marshalUint32(len              , p);
  io::marshalUint32(src.maxBytes     , p);
  io::marshalUint32(src.blockSize    , p);
  io::marshalUint32(src.convert_noLongerUsed, p);
  io::marshalUint32(src.tapeErrAction, p);
  io::marshalUint32(src.startFile    , p);
  io::marshalUint32(src.maxFiles     , p);
  io::marshalUint32(src.fromBlock    , p);
  io::marshalUint32(src.toBlock      , p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to marshal RtcpDumpTapeRqstMsgBody"
      ": Mismatch between expected total length and actual"
      ": expected=" << totalLen << "actual=" << nbBytesMarshalled;
    throw ex;
  }

  return totalLen;
}
