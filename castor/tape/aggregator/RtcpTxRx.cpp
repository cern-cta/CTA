/******************************************************************************
 *                castor/tape/aggregator/RtcpTxRx.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/LegacyTxRx.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/rtcp.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <sstream>
#include <string.h>
#include <time.h>


//-----------------------------------------------------------------------------
// getRequestInfoFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::getRequestInfoFromRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd, 
  const int netReadWriteTimeout, legacymsg::RtcpTapeRqstErrMsgBody &reply) 
  throw(castor::exception::Exception) {

 {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_GET_REQUEST_INFO_FROM_RTCPD, params);
  }

  // Prepare logical request for volume request ID
  legacymsg::RtcpTapeRqstErrMsgBody request;
  utils::setBytes(request, '\0');

  // Marshal the request
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal request for volume request ID: "
      << ex.getMessage().str());
  }

  // Send the request
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send request for volume request ID to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, ackMsg.reqType, __FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }

  // Receive reply from RTCPD
  utils::setBytes(reply, '\0');
  try {
    legacymsg::MessageHeader header;
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      header);
    RtcpTxRx::receiveMsgBody(cuuid, volReqId, socketFd, netReadWriteTimeout,
      header, reply);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to receive tape request from RTCPD"
      ": " << ex.getMessage().str());
  }

  // Send acknowledge to RTCPD
  try {
    LegacyTxRx::sendMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout, 
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to send acknowledge to RTCPD"
      ": " << ex.getMessage().str());
  }

  LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_GOT_REQUEST_INFO_FROM_RTCPD, volReqId, socketFd, request);
}


//-----------------------------------------------------------------------------
// giveVolumeToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::giveVolumeToRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd, 
  const int netReadWriteTimeout, legacymsg::RtcpTapeRqstErrMsgBody &request) 
  throw(castor::exception::Exception) {

  LogHelper::logMsgBody(cuuid, DLF_LVL_DEBUG,
    AGGREGATOR_GIVE_VOLUME_TO_RTCPD, volReqId, socketFd, request);

  // Marshal the message
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal volume message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send volume message to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, ackMsg.reqType, __FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }

  LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_GAVE_VOLUME_TO_RTCPD,
    volReqId, socketFd, request);
}


//-----------------------------------------------------------------------------
// giveFileToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::giveFileToRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const uint32_t mode,
  legacymsg::RtcpFileRqstErrMsgBody &request)
  throw(castor::exception::Exception) {

  {
    const int message_no = mode == WRITE_ENABLE ?
      AGGREGATOR_GIVE_MIGRATE_FILE_TO_RTCPD :
      AGGREGATOR_GIVE_RECALL_FILE_TO_RTCPD;

    LogHelper::logMsgBody(cuuid, DLF_LVL_DEBUG, message_no, volReqId,
      socketFd, request);
  }

  // Marshal the message
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal file message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send file message to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_FILEERR_REQ, ackMsg.reqType, __FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }

  {
    const int message_no = mode == WRITE_ENABLE ?
      AGGREGATOR_GAVE_MIGRATE_FILE_TO_RTCPD :
      AGGREGATOR_GAVE_RECALL_FILE_TO_RTCPD;

    LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM, message_no, volReqId,
      socketFd, request);
  }
}


//-----------------------------------------------------------------------------
// tellRtcpdDumpTape
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::tellRtcpdDumpTape(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, legacymsg::RtcpDumpTapeRqstMsgBody &request)
  throw(castor::exception::Exception) {

  LogHelper::logMsgBody(cuuid, DLF_LVL_DEBUG, AGGREGATOR_TELL_RTCPD_DUMP_TAPE,
    volReqId, socketFd, request);

  // Marshal the message
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal file message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send file message to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  {
    const uint32_t expected[] = {RTCP_DUMPTAPE_REQ};
    checkRtcopyReqType(expected, ackMsg.reqType, __FUNCTION__);
  }

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }

  LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_TOLD_RTCPD_DUMP_TAPE,
    volReqId, socketFd, request);
}


//-----------------------------------------------------------------------------
// pingRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::pingRtcpd(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;

  legacymsg::MessageHeader header;
  header.magic       = RTCOPY_MAGIC;
  header.reqType     = RTCP_PING_REQ;
  header.lenOrStatus = 0;

  try {
    // The RTCPD message is a bodiless RTCP message
    totalLen = legacymsg::marshal(buf, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal RCPD ping message : "
      << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_PING_RTCPD, params);
  }

  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to send the RCPD ping message to RTCPD"
      ": " << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_PINGED_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// tellRtcpdEndOfFileList
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::tellRtcpdEndOfFileList(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd, 
  const int netReadWriteTimeout) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_TELL_RTCPD_END_OF_FILE_LIST, params);
  }

  // Marshal the message
  legacymsg::RtcpNoMoreRequestsMsgBody body;
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal \"no more requests\" message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send file message to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  {
    const uint32_t expected[] = {RTCP_NOMORE_REQ, RTCP_TAPEERR_REQ};
    checkRtcopyReqType(expected, ackMsg.reqType, __FUNCTION__);
  }

  switch(ackMsg.reqType) {
  case RTCP_NOMORE_REQ:
    // If the acknowledge is negative
    if(ackMsg.lenOrStatus != 0) {
      TAPE_THROW_CODE(ackMsg.lenOrStatus,
        ": Received negative acknowledge from RTCPD"
        ": Status: " << ackMsg.lenOrStatus);
    }
    break;
  case RTCP_TAPEERR_REQ:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Got an RTCP_TAPEERR_REQ message when expecting an "
        "RTCP_NOMORE_REQ acknowledge");
    }
    break;
  default:
    {
      // Should never reach this point
      TAPE_THROW_EX(castor::exception::Internal,
        ": Unknown RTCOPY_MAGIC request type after successful call to "
        "checkRtcopyReqType");
    }
  } // switch(ackMsg.reqType)
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_TOLD_RTCPD_END_OF_FILE_LIST, params);
  }
}


//-----------------------------------------------------------------------------
// tellRtcpdToAbort
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::tellRtcpdToAbort(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_TELL_RTCPD_TO_ABORT, params);
  }

  // Marshal the message
  legacymsg::RtcpAbortMsgBody body;
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal \"no more requests\" message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send abort message to RTCPD: "
      << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_TOLD_RTCPD_TO_ABORT, params);
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_NOMORE_REQ, ackMsg.reqType, __FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }
}





//-----------------------------------------------------------------------------
// receiveRtcpJobRqst
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRtcpJobRqst(const Cuuid_t &cuuid,
  const int socketFd, const int netReadWriteTimeout,
  legacymsg::RtcpJobRqstMsgBody &request) throw(castor::exception::Exception) {

  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
    AGGREGATOR_RECEIVE_RCP_JOB_RQST);

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from RCP job submitter"
      << ": " << ex.getMessage().str());
  }

  // Unmarshal the messager header
  legacymsg::MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to unmarshal message header from RCP job submitter"
      ": " << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC_OLD0, header.magic, __FUNCTION__);
  checkRtcopyReqType(VDQM_CLIENTINFO, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RCP job submitter is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  // If the message body is too small
  {
    // The minimum size of a RtcpJobRqstMsgBody is 4 uint32_t's and the
    // termination characters of 4 strings
    const size_t minimumLen = 4 * sizeof(uint32_t) + 4;
    if(header.lenOrStatus < minimumLen) {
      TAPE_THROW_CODE(EMSGSIZE,
           ": Message body from RCP job submitter is too small"
           ": Minimum: " << minimumLen
        << ": Received: " << header.lenOrStatus);
    }
  }

  // Read the message body
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from RCP job submitter"
      << ": "<< ex.getMessage().str());
  }

  // Unmarshal the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    legacymsg::unmarshal(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshal message body from RCP job submitter"
      << ": "<< ex.getMessage().str());
  }

  LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_RECEIVED_RCP_JOB_RQST, request.volReqId, socketFd, request);
}

//-----------------------------------------------------------------------------
// askRtcpdToRequestMoreWork
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::askRtcpdToRequestMoreWork(
  const Cuuid_t &cuuid, const uint32_t volReqId, 
  const char *tapePath, const int socketFd, const int netReadWriteTimeout,
  const uint32_t mode) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      mode == WRITE_ENABLE ? AGGREGATOR_ASK_RTCPD_TO_RQST_MORE_MIGRATE_WORK :
      AGGREGATOR_ASK_RTCPD_TO_RQST_MORE_RECALL_WORK, params);
  }

  legacymsg::RtcpFileRqstErrMsgBody request;

  utils::setBytes(request, '\0');

  utils::copyString(request.recfm, "F");

  utils::copyString(request.tapePath, tapePath);
  request.volReqId       =  volReqId;
  request.jobId          = -1;
  request.stageSubReqId  = -1;
  request.umask          = RTCOPYCONSERVATIVEUMASK;
  request.positionMethod = -1;
  request.tapeFseq       = -1;
  request.diskFseq       = -1;
  request.blockSize      = -1;
  request.recordLength   = -1;
  request.retention      = -1;
  request.defAlloc       = -1;
  request.rtcpErrAction  = -1;
  request.tpErrAction    = -1;
  request.convert        = ASCCONV;
  request.checkFid       = -1;
  request.concat         =  1;
  request.procStatus     =  RTCP_REQUEST_MORE_WORK;
  request.err.severity   =  RTCP_OK;
  request.err.maxTpRetry = -1;
  request.err.maxCpRetry = -1;

  // Marshal the message
  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal ask RTCPD to request more "
      << (mode == WRITE_ENABLE ? "migrate" : "recall") << " work message: "
      << ex.getMessage().str());
  }

  // Send the message
  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send ask RTCPD to request more "
      << (mode == WRITE_ENABLE ? "migrate" : "recall")
      << " work message to RTCPD"
         ": " << ex.getMessage().str());
  }

  // Receive acknowledge from RTCPD
  legacymsg::MessageHeader ackMsg;
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to receive acknowledge from RTCPD"
      ": " << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_FILEERR_REQ, ackMsg.reqType, __FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.lenOrStatus != 0) {
    TAPE_THROW_CODE(ackMsg.lenOrStatus,
      ": Received negative acknowledge from RTCPD"
      ": Status: " << ackMsg.lenOrStatus);
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      mode == WRITE_ENABLE ? AGGREGATOR_ASKED_RTCPD_TO_RQST_MORE_MIGRATE_WORK :
      AGGREGATOR_ASKED_RTCPD_TO_RQST_MORE_RECALL_WORK, params);
  }
}


//-----------------------------------------------------------------------------
// giveFileToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::giveFileToRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const uint32_t mode,
  const char *const filePath, const uint64_t fileSize,
  const char *const tapePath, const char *const recordFormat,
  const char *const tapeFileId, const uint32_t umask,
  const int32_t positionMethod, const int32_t tapeFseq, const int32_t diskFseq,
  char (&nameServerHostName)[CA_MAXHOSTNAMELEN+1], const uint64_t castorFileId,
  unsigned char (&blockId)[4]) throw(castor::exception::Exception) {

  legacymsg::RtcpFileRqstErrMsgBody request;


  // Give file information to RTCPD
  utils::setBytes(request, '\0');
  utils::copyString(request.filePath, filePath    );
  utils::copyString(request.tapePath, tapePath    );
  utils::copyString(request.recfm   , recordFormat);
  utils::copyString(request.fid     , tapeFileId  );

  request.volReqId             = volReqId;
  request.jobId                = -1;
  request.stageSubReqId        = -1;
  request.umask                = umask;
  request.positionMethod       = positionMethod;
  request.tapeFseq             = tapeFseq;
  request.diskFseq             = diskFseq;
  request.blockSize            = -1;
  request.recordLength         = -1;
  request.retention            = -1;
  request.defAlloc             = 0;
  request.rtcpErrAction        = -1;
  request.tpErrAction          = -1;
  request.convert              = ASCCONV;
  request.checkFid             = -1;
  request.concat               = mode == WRITE_ENABLE ? NOCONCAT : OPEN_NOTRUNC;
  request.procStatus           = RTCP_WAITING;
  request.blockId[0]           = blockId[0];
  request.blockId[1]           = blockId[1];
  request.blockId[2]           = blockId[2];
  request.blockId[3]           = blockId[3];
  request.bytesIn              = fileSize;
  utils::copyString(request.segAttr.nameServerHostName, nameServerHostName);
  request.segAttr.castorFileId = castorFileId;
  request.err.severity         = RTCP_OK;
  request.err.maxTpRetry       = -1;
  request.err.maxCpRetry       = -1;

  giveFileToRtcpd(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT, mode, request);
}


//-----------------------------------------------------------------------------
// checkMagic
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::checkMagic(const uint32_t expected,
  const uint32_t actual, const char *function)
  throw(castor::exception::Exception) {

  if(expected != actual) {
    TAPE_THROW_CODE(EBADMSG,
         ": Invalid magic number"
         ": Expected: 0x" << std::hex << expected
      << "(" << utils::magicToString(expected) << ")"
         ": Actual: 0x" << std::hex << actual
      << "(" << utils::magicToString(actual) << ")");
  }
}


//-----------------------------------------------------------------------------
// checkRtcopyReqType
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::checkRtcopyReqType(
  const uint32_t expected, const uint32_t actual, const char *function)
  throw(castor::exception::Exception) {

  checkRtcopyReqType(&expected, 1, actual, function);
}


//-----------------------------------------------------------------------------
// checkRtcopyReqType
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::checkRtcopyReqType(
  const uint32_t *expected, const size_t nbExpected, const uint32_t actual,
  const char *function) throw(castor::exception::Exception) {

  size_t i = 0;


  // If actual request type equals one of the expected then return
  for(i=0; i<nbExpected; i++) {
    if(actual == expected[i]) {
      return;
    }
  }

  // This point can only be reached if the actual request type is not expected
  std::stringstream messageStream;

  messageStream
    << ": Invalid RTCOPY_MAGIC request type"
       ": Expected:";

  for(i=0; i<nbExpected; i++) {
    if(i != 0) {
      messageStream << " or";
    }

    messageStream << " 0x" << std::hex << expected[i]
      << "(" << utils::rtcopyReqTypeToString(expected[i]) << ")";
  }

  messageStream << ": Actual: 0x" << std::hex << actual
    << "(" << utils::rtcopyReqTypeToString(actual) << ")";

  TAPE_THROW_CODE(EBADMSG,
    messageStream.str());
}
