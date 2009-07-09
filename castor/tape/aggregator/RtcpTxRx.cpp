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
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
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
  const int netReadWriteTimeout, RtcpTapeRqstErrMsgBody &reply) 
  throw(castor::exception::Exception) {

 {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GET_REQUEST_INFO_FROM_RTCPD, params);
  }

  // Prepare logical request for volume request ID
  RtcpTapeRqstErrMsgBody request;
  utils::setBytes(request, '\0');

  // Marshall the request
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall request for volume request ID: "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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
    MessageHeader header;
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
      header);
    receiveRtcpTapeRqstErrBody(cuuid, volReqId, socketFd, netReadWriteTimeout,
      header, reply);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to receive tape request from RTCPD"
      ": " << ex.getMessage().str());
  }

  // Send acknowledge to RTCPD
  try {
    sendMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout, 
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to send acknowledge to RTCPD"
      ": " << ex.getMessage().str());
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId  ),
      castor::dlf::Param("socketFd", socketFd  ),
      castor::dlf::Param("unit"    , reply.unit)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_REQUEST_INFO_FROM_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// giveVolumeToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::giveVolumeToRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd, 
  const int netReadWriteTimeout, RtcpTapeRqstErrMsgBody &request) 
  throw(castor::exception::Exception) {

 {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", volReqId       ),
        castor::dlf::Param("socketFd", socketFd       ),
        castor::dlf::Param("vid"     , request.vid    ),
        castor::dlf::Param("mode"    , request.mode   ),
        castor::dlf::Param("label"   , request.label  ),
        castor::dlf::Param("density" , request.density),
        castor::dlf::Param("unit"    , request.unit   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GIVE_VOLUME_TO_RTCPD, params);
  }

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall volume message: "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId       ),
      castor::dlf::Param("socketFd", socketFd       ),
      castor::dlf::Param("vid"     , request.vid    ),
      castor::dlf::Param("mode"    , request.mode   ),
      castor::dlf::Param("label"   , request.label  ),
      castor::dlf::Param("density" , request.density),
      castor::dlf::Param("unit"    , request.unit   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GAVE_VOLUME_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// giveFileToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::giveFileToRtcpd(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const uint32_t mode,
  RtcpFileRqstErrMsgBody &request) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"    , volReqId         ),
      castor::dlf::Param("socketFd"    , socketFd         ),
      castor::dlf::Param("filePath"    , request.filePath ),
      castor::dlf::Param("bytesIn"     , request.bytesIn  ), // File size
      castor::dlf::Param("bytesOut"    , request.bytesOut ),
      castor::dlf::Param("hostBytes"   , request.hostBytes),
      castor::dlf::Param("tapePath"    , request.tapePath ),
      castor::dlf::Param("tapeFseq"    , request.tapeFseq ),
      castor::dlf::Param("diskFseq"    , request.diskFseq ),
      castor::dlf::Param("recordFormat", RECORDFORMAT     ),
      castor::dlf::Param("umask"       , MIGRATEUMASK     )};
   
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      mode == WRITE_ENABLE ? AGGREGATOR_GIVE_MIGRATE_FILE_TO_RTCPD :
      AGGREGATOR_GIVE_RECALL_FILE_TO_RTCPD, params);
  }

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpFileRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall file message: "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"    , volReqId         ),
      castor::dlf::Param("socketFd"    , socketFd         ),
      castor::dlf::Param("filePath"    , request.filePath ),
      castor::dlf::Param("bytesIn"     , request.bytesIn  ), // File size
      castor::dlf::Param("bytesOut"    , request.bytesOut ),
      castor::dlf::Param("hostBytes"   , request.hostBytes),
      castor::dlf::Param("tapePath"    , request.tapePath ),
      castor::dlf::Param("tapeFseq"    , request.tapeFseq ),
      castor::dlf::Param("diskFseq"    , request.diskFseq ),
      castor::dlf::Param("recordFormat", RECORDFORMAT     ),
      castor::dlf::Param("umask"       , MIGRATEUMASK     )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      mode == WRITE_ENABLE ? AGGREGATOR_GAVE_MIGRATE_FILE_TO_RTCPD :
      AGGREGATOR_GAVE_RECALL_FILE_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// sendMessageHeader
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::sendMessageHeader(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd, 
  const int netReadWriteTimeout, const MessageHeader &header) 
  throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  try {
    totalLen = Marshaller::marshallMessageHeader(buf, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall RCP acknowledge message: "
      << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SEND_ACK_TO_RTCPD, params);
  }

  try {
    net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to send the RCP acknowledge message to RTCPD"
      ": " << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SENT_ACK_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// pingRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::pingRtcpd(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  MessageHeader header;
  header.magic       = RTCOPY_MAGIC;
  header.reqType     = RTCP_PING_REQ;
  header.lenOrStatus = 0;

  try {
    // The RTCPD message is a bodiless RTCP message
    totalLen = Marshaller::marshallMessageHeader(buf, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall RCPD ping message : "
      << ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
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

    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
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
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_TELL_RTCPD_END_OF_FILE_LIST, params);
  }

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpNoMoreRequestsMsgBody(buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall \"no more requests\" message: "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_TELL_RTCPD_TO_ABORT, params);
  }

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpAbortMsg(buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall \"no more requests\" message: "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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
// receiveRcpJobRqst
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRcpJobRqst(const Cuuid_t &cuuid,
  const int socketFd, const int netReadWriteTimeout,
  RcpJobRqstMsgBody &request) throw(castor::exception::Exception) {

  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
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

  // Unmarshall the messager header
  MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to unmarshall message header from RCP job submitter"
      ": " << ex.getMessage().str());
  }

  checkMagic(RTCOPY_MAGIC_OLD0, header.magic, __FUNCTION__);
  checkRtcopyReqType(VDQM_CLIENTINFO, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RCP job submitter is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  // If the message body is too small
  {
    // The minimum size of a RcpJobRqstMsgBody is 4 uint32_t's and the
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

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    Marshaller::unmarshallRcpJobRqstMsgBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshall message body from RCP job submitter"
      << ": "<< ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , request.tapeRequestId  ),
      castor::dlf::Param("clientPort"     , request.clientPort     ),
      castor::dlf::Param("clientEuid"     , request.clientEuid     ),
      castor::dlf::Param("clientEgid"     , request.clientEgid     ),
      castor::dlf::Param("clientHost"     , request.clientHost     ),
      castor::dlf::Param("deviceGroupName", request.deviceGroupName),
      castor::dlf::Param("driveName"      , request.driveName      ),
      castor::dlf::Param("clientUserName" , request.clientUserName )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_RCP_JOB_RQST, params);
  }
}

//-----------------------------------------------------------------------------
// askRtcpdToRequestMoreWork
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::askRtcpdToRequestMoreWork(
  const Cuuid_t &cuuid, const uint32_t volReqId, 
  char (&tapePath)[CA_MAXPATHLEN+1], const int socketFd, 
  const int netReadWriteTimeout, const uint32_t mode)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      mode == WRITE_ENABLE ? AGGREGATOR_ASK_RTCPD_TO_RQST_MORE_MIGRATE_WORK :
      AGGREGATOR_ASK_RTCPD_TO_RQST_MORE_RECALL_WORK, params);
  }

  RtcpFileRqstErrMsgBody request;

  utils::setBytes(request, '\0');

  utils::copyString(request.recfm, "F");

  utils::copyString(request.tapePath, tapePath);
  request.volReqId       =  volReqId;
  request.jobId          = -1;
  request.stageSubReqId  = -1;
  request.umask          = mode == WRITE_ENABLE ? MIGRATEUMASK : RECALLUMASK;
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

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpFileRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshall ask RTCPD to request more "
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
  MessageHeader ackMsg;
  try {
    receiveMessageHeader(cuuid, volReqId, socketFd, netReadWriteTimeout,
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

  RtcpFileRqstErrMsgBody request;


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
// receiveMessageHeader
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveMessageHeader(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, MessageHeader &header)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from RTCPD"
      << ": " << ex.getMessage().str());
  }

  // Unmarshall the messager header
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
       ": Failed to unmarshall message header from RTCPD"
       ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveMessageHeaderFromCloseableConn
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveMessageHeaderFromCloseable(
  const Cuuid_t &cuuid,  bool &connClosed, const uint32_t volReqId, 
  const int socketFd, const int netReadWriteTimeout, MessageHeader &header)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytesFromCloseable(connClosed, socketFd, RTCPDNETRWTIMEOUT, 
      sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from RTCPD"
      << ": " << ex.getMessage().str());
  }

  // Unmarshall the messager header
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
       ": Failed to unmarshall message header from RTCPD"
       ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpFileRqstErrBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRtcpFileRqstErrBody(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const MessageHeader &header,
  RtcpFileRqstErrMsgBody &body) throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_FILEERR_REQ, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVE_FILERQSTERRBODY_FROM_RTCPD, params);
  }

  // Read the message body
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from RTCPD"
      << ": " << ex.getMessage().str());
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    Marshaller::unmarshallRtcpFileRqstErrMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId      ),
      castor::dlf::Param("socketFd"  , socketFd      ),
      castor::dlf::Param("bytesIn"   , body.bytesIn  ),
      castor::dlf::Param("bytesOut"  , body.bytesOut ),
      castor::dlf::Param("hostBytes" , body.hostBytes),
      castor::dlf::Param("procStatus",
        utils::procStatusToString(body.procStatus))};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_FILERQSTERRBODY_FROM_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpFileRqstBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRtcpFileRqstBody(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const MessageHeader &header,
  RtcpFileRqstMsgBody &body) throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_FILE_REQ, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVE_FILERQSTBODY_FROM_RTCPD, params);
  }

  // Read the message body
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    Marshaller::unmarshallRtcpFileRqstMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId      ),
      castor::dlf::Param("socketFd"  , socketFd      ),
      castor::dlf::Param("bytesIn"   , body.bytesIn  ),
      castor::dlf::Param("bytesOut"  , body.bytesOut ),
      castor::dlf::Param("hostBytes" , body.hostBytes),
      castor::dlf::Param("procStatus",
        utils::procStatusToString(body.procStatus))};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_FILERQSTBODY_FROM_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRqstErrBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRtcpTapeRqstErrBody(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const MessageHeader &header,
  RtcpTapeRqstErrMsgBody &body) throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVE_TAPERQSTERRBODY, params);
  }

  // Read the message body
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    Marshaller::unmarshallRtcpTapeRqstErrMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_TAPERQSTERRBODY, params);
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRqstBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpTxRx::receiveRtcpTapeRqstBody(
  const Cuuid_t &cuuid, const uint32_t volReqId, const int socketFd,
  const int netReadWriteTimeout, const MessageHeader &header,
  RtcpTapeRqstMsgBody &body) throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __FUNCTION__);
  checkRtcopyReqType(RTCP_TAPE_REQ, header.reqType, __FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVE_TAPERQSTBODY, params);
  }

  // Read the message body
  try {
    net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    Marshaller::unmarshallRtcpTapeRqstMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str());
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_TAPERQSTBODY, params);
  }
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
