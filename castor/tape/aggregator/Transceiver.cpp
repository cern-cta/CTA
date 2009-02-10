/******************************************************************************
 *                castor/tape/aggregator/Transceiver.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/Transceiver.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrateResponse.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/FileToRecallResponse.hpp"
#include "castor/tape/tapegateway/StartTransferRequest.hpp"
#include "castor/tape/tapegateway/StartTransferResponse.hpp"
#include "h/common.h"
#include "h/rtcp.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <string.h>
#include <time.h>


//-----------------------------------------------------------------------------
// getRequestInfoFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::getRequestInfoFromRtcpd(
  const int socketFd, const int netReadWriteTimeout,
  RtcpTapeRqstErrMsgBody &reply) throw(castor::exception::Exception) {

  // Prepare logical request for volume request ID
  RtcpTapeRqstErrMsgBody request;
  Utils::setBytes(request, '\0');

  // Marshall the request
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall request for volume request ID: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the request
  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send request for volume request ID to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMsg ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, ackMsg.reqType, __PRETTY_FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.status != 0) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Received negative acknowledge from RTCPD"
         ": Status: " << ackMsg.status;
    throw ex;
  }

  // Receive reply from RTCPD
  Utils::setBytes(reply, '\0');
  try {
    MessageHeader header;
    receiveRtcpMsgHeader(socketFd, netReadWriteTimeout, header);
    receiveRtcpTapeRqstErrBody(socketFd, netReadWriteTimeout, header, reply);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive tape request from RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }

  // Send acknowledge to RTCPD
  try {
    sendRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send acknowledge to RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// giveVolumeInfoToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveVolumeInfoToRtcpd(
  const int socketFd, const int netReadWriteTimeout,
  RtcpTapeRqstErrMsgBody &request, RtcpTapeRqstErrMsgBody &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall volume message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the message
  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send volume message to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMsg ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, ackMsg.reqType, __PRETTY_FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.status != 0) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Received negative acknowledge from RTCPD"
         ": Status: " << ackMsg.status;
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// giveFileInfoToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveFileInfoToRtcpd(
  const int socketFd, const int netReadWriteTimeout,
  RtcpFileRqstErrMsgBody &request, RtcpFileRqstErrMsgBody &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpFileRqstErrMsgBody(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall file message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the message
  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send file message to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMsg ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_FILEERR_REQ, ackMsg.reqType, __PRETTY_FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.status != 0) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Received negative acknowledge from RTCPD"
         ": Status: " << ackMsg.status;
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpAcknowledge(
  const int socketFd, const int netReadWriteTimeout,
  RtcpAcknowledgeMsg &message) throw(castor::exception::Exception) {

  // Read in the RTCPD acknowledge message (there is no separate header and
  // body)
  char messageBuf[3 * sizeof(uint32_t)]; // magic + request type + status
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, sizeof(messageBuf),
      messageBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read RTCPD acknowledge message"
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the RTCPD acknowledge message
  try {
    const char *p           = messageBuf;
    size_t     remainingLen = sizeof(messageBuf);
    Marshaller::unmarshallRtcpAcknowledgeMsg(p, remainingLen, message);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EBADMSG);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall RTCPD acknowledge message"
         ": " << ex.getMessage().str();

    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// sendRtcpAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::sendRtcpAcknowledge(
  const int socketFd, const int netReadWriteTimeout,
  const RtcpAcknowledgeMsg &message) throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  try {
    totalLen = Marshaller::marshallRtcpAcknowledgeMsg(buf, message);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall RCP acknowledge message: "
      << ex.getMessage().str();

    throw ie;
  }

  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send the RCP acknowledge message to RTCPD"
         ": " << ex.getMessage().str();

    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// pingRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::pingRtcpd(
  const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  MessageHeader header;
  header.magic   = RTCOPY_MAGIC;
  header.reqType = RTCP_PING_REQ;
  header.len     = 0;

  try {
    // The RTCPD message is a bodiless RTCP message
    totalLen = Marshaller::marshallMessageHeader(buf, header);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall RCPD ping message : "
      << ex.getMessage().str();

    throw ie;
  }

  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send the RCPD ping message to RTCPD"
         ": " << ex.getMessage().str();

    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// signalNoMoreRequestsToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::signalNoMoreRequestsToRtcpd(
  const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpNoMoreRequestsMsgBody(buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall \"no more requests\" message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the message
  try {
    Net::writeBytes(socketFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send file message to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMsg ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  checkMagic(RTCOPY_MAGIC, ackMsg.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_NOMORE_REQ, ackMsg.reqType, __PRETTY_FUNCTION__);

  // If the acknowledge is negative
  if(ackMsg.status != 0) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Received negative acknowledge from RTCPD"
         ": Status: " << ackMsg.status;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// receiveRcpJobRqst
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRcpJobRqst(
  const int socketFd, const int netReadWriteTimeout,
  RcpJobRqstMsgBody &request) throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message header from RCP job submitter"
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the messager header
  MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EBADMSG);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message header from RCP job submitter"
         ": " << ex.getMessage().str();

    throw ex2;
  }

  checkMagic(RTCOPY_MAGIC_OLD0, header.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(VDQM_CLIENTINFO, header.reqType, __PRETTY_FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RCP job submitter is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // If the message body is too small
  {
    // The minimum size of a RcpJobRqstMsgBody is 4 uint32_t's and the
    // termination characters of 4 strings
    const size_t minimumLen = 4 * sizeof(uint32_t) + 4;
    if(header.len < minimumLen) {
      castor::exception::Exception ex(EMSGSIZE);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Message body from RCP job submitter is too small"
           ": Minimum: " << minimumLen
        << ": Received: " << header.len;

      throw ex;
    }
  }

  // Read the message body
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RCP job submitter"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRcpJobRqstMsgBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RCP job submitter"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// giveRequestForMoreWorkToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveRequestForMoreWorkToRtcpd(
  const int socketFd, const int netReadWriteTimeout, const uint32_t volReqId)
  throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody request;
  RtcpFileRqstErrMsgBody reply;

  Utils::setBytes(request, '\0');
  Utils::copyString(request.recfm, "F");

  request.volReqId       = volReqId;
  request.jobId          = -1;
  request.stageSubReqId  = -1;
  request.tapeFseq       = 1;
  request.diskFseq       = 1;
  request.blockSize      = -1;
  request.recordLength   = -1;
  request.retention      = -1;
  request.defAlloc       = -1;
  request.rtcpErrAction  = -1;
  request.tpErrAction    = -1;
  request.convert        = -1;
  request.checkFid       = -1;
  request.concat         = 1;
  request.procStatus     = RTCP_REQUEST_MORE_WORK;
  request.err.severity   = 1;
  request.err.maxTpRetry = -1;
  request.err.maxCpRetry = -1;

  giveFileInfoToRtcpd(socketFd, RTCPDNETRWTIMEOUT, request, reply);

  // TBD - process reply

  // Signal the end of the file list to RTCPD
  signalNoMoreRequestsToRtcpd(socketFd, RTCPDNETRWTIMEOUT);
}


//-----------------------------------------------------------------------------
// giveFileListToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveFileListToRtcpd(
  const int socketFd, const int netReadWriteTimeout, const uint32_t volReqId,
  const char *const filePath, const char *const tapePath, const uint32_t umask,
  const bool requestMoreWork) throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody request;
  RtcpFileRqstErrMsgBody reply;


  // Give file information to RTCPD
  {
    Utils::setBytes(request, '\0');
    Utils::copyString(request.filePath, filePath);
    Utils::copyString(request.tapePath, tapePath);
    Utils::copyString(request.recfm, "F");

    request.volReqId       = volReqId;
    request.jobId          = -1;
    request.stageSubReqId  = -1;
    request.umask          = umask;
    request.tapeFseq       = 1;
    request.diskFseq       = 1;
    request.blockSize      = -1;
    request.recordLength   = -1;
    request.retention      = -1;
    request.defAlloc       = -1;
    request.rtcpErrAction  = -1;
    request.tpErrAction    = -1;
    request.convert        = -1;
    request.checkFid       = -1;
    request.concat         = 1;
    request.procStatus     = RTCP_WAITING;
    request.err.severity   = 1;
    request.err.maxTpRetry = -1;
    request.err.maxCpRetry = -1;

    giveFileInfoToRtcpd(socketFd, RTCPDNETRWTIMEOUT, request, reply);

    // TBD - process reply
  }

  if(requestMoreWork) {
    Utils::setBytes(request, '\0');
    Utils::copyString(request.recfm, "F");

    request.volReqId       = volReqId;
    request.jobId          = -1;
    request.stageSubReqId  = -1;
    request.tapeFseq       = 1;
    request.diskFseq       = 1;
    request.blockSize      = -1;
    request.recordLength   = -1;
    request.retention      = -1;
    request.defAlloc       = -1;
    request.rtcpErrAction  = -1;
    request.tpErrAction    = -1;
    request.convert        = -1;
    request.checkFid       = -1;
    request.concat         = 1;
    request.procStatus     = RTCP_REQUEST_MORE_WORK;
    request.err.severity   = 1;
    request.err.maxTpRetry = -1;
    request.err.maxCpRetry = -1;

    giveFileInfoToRtcpd(socketFd, RTCPDNETRWTIMEOUT, request, reply);

    // TBD - process reply
  }

  // Signal the end of the file list to RTCPD
  signalNoMoreRequestsToRtcpd(socketFd, RTCPDNETRWTIMEOUT);
}


//-----------------------------------------------------------------------------
// receiveRtcpMsgHeader
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpMsgHeader(
  const int socketFd, const int netReadWriteTimeout,
  MessageHeader &header) throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message header from RTCPD"
      << ": " << ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the messager header
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    Marshaller::unmarshallMessageHeader(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EBADMSG);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message header from RTCPD"
         ": " << ex.getMessage().str();

    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpFileRqstErrBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpFileRqstErrBody(
  const int socketFd, const int netReadWriteTimeout,
  const MessageHeader &header, RtcpFileRqstErrMsgBody &body)
  throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_FILEERR_REQ, header.reqType, __PRETTY_FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRtcpFileRqstErrMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpFileRqstBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpFileRqstBody(
  const int socketFd, const int netReadWriteTimeout,
  const MessageHeader &header, RtcpFileRqstMsgBody &body)
  throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_FILE_REQ, header.reqType, __PRETTY_FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRtcpFileRqstMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRqstErrBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpTapeRqstErrBody(
  const int socketFd, const int netReadWriteTimeout,
  const MessageHeader &header, RtcpTapeRqstErrMsgBody &body)
  throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_TAPEERR_REQ, header.reqType, __PRETTY_FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRtcpTapeRqstErrMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRqstBody
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpTapeRqstBody(
  const int socketFd, const int netReadWriteTimeout,
  const MessageHeader &header, RtcpTapeRqstMsgBody &body)
  throw(castor::exception::Exception) {

  checkMagic(RTCOPY_MAGIC, header.magic, __PRETTY_FUNCTION__);
  checkRtcopyReqType(RTCP_TAPE_REQ, header.reqType, __PRETTY_FUNCTION__);

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body
  try {
    Net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.len;
    Marshaller::unmarshallRtcpTapeRqstMsgBody(p, remainingLen, body);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// checkMagic
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::checkMagic(const uint32_t expected,
  const uint32_t actual, const char *function)
  throw(castor::exception::Exception) {

  if(expected != actual) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << function
      << ": Invalid magic number"
         ": Expected: 0x" << std::hex << expected
      << "(" << Utils::magicToStr(expected) << ")"
         ": Actual: 0x" << std::hex << actual
      << "(" << Utils::magicToStr(actual) << ")";

    throw ex;
  }
}


//-----------------------------------------------------------------------------
// checkRtcopyReqType
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::checkRtcopyReqType(
  const uint32_t expected, const uint32_t actual, const char *function)
  throw(castor::exception::Exception) {

  if(expected != actual) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << function
      << ": Invalid RTCOPY_MAGIC request type"
         ": Expected: 0x" << std::hex << expected
      << "(" << Utils::rtcopyReqTypeToStr(expected) << ")"
         ": Actual: 0x" << std::hex << actual
      << "(" << Utils::rtcopyReqTypeToStr(actual) << ")";

    throw ex;
  }
}


//-----------------------------------------------------------------------------
// tellGatewayToStartTransfer
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::tellGatewayToStartTransfer(
  const std::string gatewayhost, const unsigned short gatewayPort,
  const uint32_t volReqId, const char *const unit, char (&vid)[CA_MAXVIDLEN+1],
  uint32_t &mode, char (&label)[CA_MAXLBLTYPLEN+1],
  char (&density)[CA_MAXDENLEN+1], int &errorCode, std::string &errorMsg)
  throw(castor::exception::Exception) {

  tapegateway::StartTransferRequest request;

  request.setVdqmVolReqId(volReqId);
  request.setUnit(unit);

  castor::io::ClientSocket socket(gatewayPort, gatewayhost);

  castor::IObject *obj = socket.readObject();

  if(obj == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get StartTransferResponse from tape gateway"
         ": ClientSocket::readObject() returned null";

    throw ex;
  }

  std::auto_ptr<tapegateway::StartTransferResponse> response(
    dynamic_cast<tapegateway::StartTransferResponse*>(obj));

  if(response.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get StartTransferResponse from tape gateway"
         ": Failed to dynamic cast StartTransferResponse";

    throw ex;
  }

  Utils::copyString(vid, response->vid().c_str());
  mode = response->mode();
  Utils::copyString(vid, response->label().c_str());
  Utils::copyString(density, response->density().c_str());
  errorCode = response->errorCode();
  errorMsg = response->errorMessage();
}


//-----------------------------------------------------------------------------
// getFileToMigrateFromGateway
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::getFileToMigrateFromGateway(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t volReqId, int &errorCode, std::string &errorMsg)
  throw(castor::exception::Exception) {
/*
  tapegateway::FileToMigrateRequest request;

  request.setTransactionId(volReqId);

  castor::io::ClientSocket socket(gatewayPort, gatewayhost);

  castor::IObject *obj = socket.readObject();

  if(obj == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get FileToMigrateResponse from tape gateway"
         ": ClientSocket::readObject() returned null";

    throw ex;
  }

  std::auto_ptr<tapegateway::FileToMigrateResponse> response(
    dynamic_cast<tapegateway::FileToMigrateResponse*>(obj));

  if(response.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get FileToMigrateResponse from tape gateway"
         ": Failed to dynamic cast FileToMigrateResponse";

    throw ex;
  }

  if(response.transactonId() != volReqId) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get FileToMigrateResponse from tape gateway"
         ": Transaction ID does not match volume request ID"
         ": Transaction ID = " << response.transactonId()
      << ": Volume Request ID = " << volReqId;

    throw ex;
  }

  vid       = response->vid();
  mode      = response->mode();
  label     = response->label();
  density   = response->density();
  errorCode = response->errorCode();
  errorMsg  = response->errorMessage();
*/
}
