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
#include "h/common.h"
#include "h/rtcp.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <string.h>
#include <time.h>


//-----------------------------------------------------------------------------
// getVolumeRequestIdFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::
  getVolumeRequestIdFromRtcpd(const int socketFd,
  const int netReadWriteTimeout, RtcpTapeRequestMessage &reply)
  throw(castor::exception::Exception) {

  // Prepare logical request for volume request ID
  RtcpTapeRequestMessage request;
  Utils::setBytes(request, '\0');

  // Marshall the request
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRequestMessage(buf, request);
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
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  // If the magic number is invalid
  if(ackMsg.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid magic number from RTCPD"
      << ": Expected: 0x" << std::hex << RTCOPY_MAGIC
      << ": Received: " << ackMsg.magic;

    throw ex;
  }

  // If the request type is invalid
  if(ackMsg.reqtype != RTCP_TAPEERR_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_TAPEERR_REQ
      << ": Received: " << ackMsg.reqtype;

    throw ex;
  }

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
    receiveRtcpTapeRequest(socketFd, netReadWriteTimeout, reply);
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
// giveVolumeIdToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveVolumeIdToRtcpd(
  const int socketFd, const int netReadWriteTimeout,
  RtcpTapeRequestMessage &request, RtcpTapeRequestMessage &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRequestMessage(buf, request);
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
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  // If the magic number is invalid
  if(ackMsg.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid magic number from RTCPD"
      << ": Expected: 0x" << std::hex << RTCOPY_MAGIC
      << ": Received: " << ackMsg.magic;

    throw ex;
  }

  // If the request type is invalid
  if(ackMsg.reqtype != RTCP_TAPEERR_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_TAPEERR_REQ
      << ": Received: " << ackMsg.reqtype;

    throw ex;
  }

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
// receiveRtcpTapeRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpTapeRequest(
  const int socketFd, const int netReadWriteTimeout,
  RtcpTapeRequestMessage &request) throw(castor::exception::Exception) {

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
  MessageHeader header;
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

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid magic number from RTCPD"
       << ": Expected: 0x" << RTCOPY_MAGIC
       << ": Received: 0x" << header.magic;

     throw ex;
  }

  // If the request type is invalid
  if(header.reqtype != RTCP_TAPEERR_REQ) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << RTCP_TAPEERR_REQ
      << ": Received: 0x" << header.reqtype;

    throw ex;
  }

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
    Marshaller::unmarshallRtcpTapeRequestMessageBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// giveFileInfoToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::giveFileInfoToRtcpd(
  const int socketFd, const int netReadWriteTimeout,
  RtcpFileRequestMessage &request, RtcpFileRequestMessage &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpFileRequestMessage(buf, request);
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
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  // If the magic number is invalid
  if(ackMsg.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid magic number from RTCPD"
      << ": Expected: 0x" << std::hex << RTCOPY_MAGIC
      << ": Received: " << ackMsg.magic;

    throw ex;
  }

  // If the request type is invalid
  if(ackMsg.reqtype != RTCP_FILEERR_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_FILEERR_REQ
      << ": Received: " << ackMsg.reqtype;

    throw ex;
  }

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
// receiveRtcpFileRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpFileRequest(
  const int socketFd, const int netReadWriteTimeout,
  RtcpFileRequestMessage &request) throw(castor::exception::Exception) {

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
  MessageHeader header;
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

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid magic number from RTCPD"
       << ": Expected: 0x" << RTCOPY_MAGIC
       << ": Received: 0x" << header.magic;

     throw ex;
  }

  // If the request type is invalid
  if(header.reqtype != RTCP_FILEERR_REQ) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << RTCP_TAPEERR_REQ
      << ": Received: 0x" << header.reqtype;

    throw ex;
  }

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
    Marshaller::unmarshallRtcpFileRequestMessageBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRtcpAcknowledge(
  const int socketFd, const int netReadWriteTimeout,
  RtcpAcknowledgeMessage &message) throw(castor::exception::Exception) {

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
    Marshaller::unmarshallRtcpAcknowledgeMessageBody(p, remainingLen, message);
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
  const RtcpAcknowledgeMessage &message) throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  size_t totalLen = 0;

  try {
    totalLen = Marshaller::marshallRtcpAcknowledgeMessage(buf, message);
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
// signalNoMoreRequestsToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::signalNoMoreRequestsToRtcpd(
  const int socketFd, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpNoMoreRequestsMessage(buf);
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
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(socketFd, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  // If the magic number is invalid
  if(ackMsg.magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid magic number from RTCPD"
      << ": Expected: 0x" << std::hex << RTCOPY_MAGIC
      << ": Received: " << ackMsg.magic;

    throw ex;
  }

  // If the request type is invalid
  if(ackMsg.reqtype != RTCP_NOMORE_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_FILEERR_REQ
      << ": Received: " << ackMsg.reqtype;

    throw ex;
  }

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
// receiveRcpJobRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Transceiver::receiveRcpJobRequest(
  const int socketFd, const int netReadWriteTimeout,
  RcpJobRequestMessage &request) throw(castor::exception::Exception) {

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

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC_OLD0) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid magic number from RCP job submitter"
       << ": Expected: 0x" << RTCOPY_MAGIC
       << ": Received: 0x" << header.magic;

     throw ex;
  }

  // If the request type is invalid
  if(header.reqtype != VDQM_CLIENTINFO) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from RCP job submitter"
      << ": Expected: 0x" << RTCP_TAPEERR_REQ
      << ": Received: 0x" << header.reqtype;

    throw ex;
  }

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
    // The minimum size of a RcpJobRequestMessage is 4 uint32_t's and the
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
    Marshaller::unmarshallRcpJobRequestMessageBody(p, remainingLen, request);
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

  RtcpFileRequestMessage request;
  RtcpFileRequestMessage reply;

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
  const char *const filePath, const uint32_t umask, const bool requestMoreWork)
  throw(castor::exception::Exception) {

  RtcpFileRequestMessage request;
  RtcpFileRequestMessage reply;


  // Give file information to RTCPD
  {
    Utils::setBytes(request, '\0');
    Utils::copyString(request.filePath, filePath);
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
