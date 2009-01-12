/******************************************************************************
 *                castor/tape/aggregator/VdqmRequestHandlerThread.cpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/VdqmRequestHandlerThread.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "h/common.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandlerThread::VdqmRequestHandlerThread(
  const int rtcpdListenPort) throw () :
  m_rtcpdListenPort(rtcpdListenPort), m_jobQueue(1) {

  // Create the RTCOPY_MAGIC_OLD0 request handler map
  m_rtcopyMagicOld0Handlers[VDQM_CLIENTINFO] =
    &VdqmRequestHandlerThread::handleJobSubmission;

  // Create the magic number map
  m_magicToHandlers[RTCOPY_MAGIC_OLD0] = &m_rtcopyMagicOld0Handlers;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandlerThread::~VdqmRequestHandlerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::ServerSocket *socket = (castor::io::ServerSocket*)param;

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP

    // Get client IP info
    socket->getPeerIp(port, ip);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"  , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port", port)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, 2, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, 3, params);
  }

  try {

    dispatchRequest(cuuid, *socket);

  } catch(castor::exception::Exception &ex) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_HANDLE_VDQM_REQUEST_EXCEPT, 3, params);
  }

  // Close and de-allocate the socket
  socket->close();
  delete socket;
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// dispatchRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::dispatchRequest(
  Cuuid_t &cuuid, castor::io::ServerSocket &socket)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    SocketHelper::readBytes(socket, NETRWTIMEOUT, sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message header"
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
      << ": Failed to unmarshall message header"
         ": " << ex.getMessage().str();

    throw ex2;
  }

  // Find the map of request handlers for the magic number
  MagicToHandlersMap::iterator handlerMapItor = m_magicToHandlers.find(
    header.magic);
  if(handlerMapItor == m_magicToHandlers.end()) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Unknown magic number: 0x" << std::hex << header.magic;

    throw ex;
  }
  HandlerMap *handlers = handlerMapItor->second;

  // Find the request handler for the type of request
  HandlerMap::iterator handlerItor = handlers->find(header.reqtype);
  if(handlerItor == handlers->end()) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Unknown request type: 0x" << header.reqtype;

    throw ex;
  }
  Handler handler = handlerItor->second;

  // Length of message body buffer = Length of message buffer - length of header
  char bodyBuf[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is larger than the message body buffer
  if(header.len > sizeof(bodyBuf)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.len;

    throw ex;
  }

  // Read the message body from the socket
  try {
    SocketHelper::readBytes(socket, NETRWTIMEOUT, header.len, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body"
         ": " << ex.getMessage().str();

    throw ex2;
  }

  // Dispatch the request to the appropriate handler
  (this->*handler)(cuuid, header, bodyBuf, socket);
}


//-----------------------------------------------------------------------------
// handleJobSubmission
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::handleJobSubmission(
  Cuuid_t &cuuid, const MessageHeader &header, const char *bodyBuf,
  castor::io::ServerSocket &socket) throw() {

  // Check that the peer host is authorized
  {
    char peerHost[CA_MAXHOSTNAMELEN+1];


    const int rc = isadminhost(socket.socket(), peerHost);
    if(rc == -1 && serrno != SENOTADMIN) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function" , __PRETTY_FUNCTION__),
        castor::dlf::Param("Reason"   , "Failed to lookup connection"),
        castor::dlf::Param("Peer Host", peerHost)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_RECEIVED_RCP_JOB_FROM_UNAUTHORIZED_HOST, 3, params);
      return;
    }

    if(*peerHost == '\0' ) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function" , __PRETTY_FUNCTION__),
        castor::dlf::Param("Reason"   , "Peer host name is empty"),
        castor::dlf::Param("Peer Host", peerHost)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_RECEIVED_RCP_JOB_FROM_UNAUTHORIZED_HOST, 3, params);
      return;
    }

    if(rc != 0) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function" , __PRETTY_FUNCTION__),
        castor::dlf::Param("Reason"   , "Unauthorized host"),
        castor::dlf::Param("Peer Host", peerHost)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_RECEIVED_RCP_JOB_FROM_UNAUTHORIZED_HOST, 3, params);
      return;
    }
  }

  // If the message body is too small
  {
    const size_t minimumLen = 4 * sizeof(uint32_t) + 4;
    if(header.len < minimumLen) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function"      , __PRETTY_FUNCTION__),
        castor::dlf::Param("Minimum Length", minimumLen),
        castor::dlf::Param("Actual Length" , header.len)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_JOB_MESSAGE_BODY_LENGTH_TOO_SHORT, 3, params);
      return;
    }
  }

  // Unmarshall the message body
  RcpJobRequestMessage request;
  const char *p           = bodyBuf;
  size_t     remainingLen = header.len;
  try {
    Marshaller::unmarshallRcpJobRequestMessageBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_UNMARSHALL_MESSAGE_BODY, 3, params);
    return;
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID"  , request.tapeRequestID  ),
      castor::dlf::Param("clientPort"     , request.clientPort     ),
      castor::dlf::Param("clientEuid"     , request.clientEuid     ),
      castor::dlf::Param("clientEgid"     , request.clientEgid     ),
      castor::dlf::Param("clientHost"     , request.clientHost     ),
      castor::dlf::Param("deviceGroupName", request.deviceGroupName),
      castor::dlf::Param("driveName"      , request.driveName      ),
      castor::dlf::Param("clientUserName" , request.clientUserName )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_HANDLE_JOB_MESSAGE, 8, params);
  }

  // Pass a modified version of the request through to RTCPD, setting the
  // clientHost and clientPort parameters to identify the tape aggregator as
  // being a proxy for RTCPClientD
  RcpJobReplyMessage reply;
  try {
    RcpJobSubmitter::submit(
      "localhost",            // host
      RTCOPY_PORT,            // port
      NETRWTIMEOUT,           // netReadWriteTimeout
      "RTCPD",                // remoteCopyType
      request.tapeRequestID,
      request.clientUserName,
      "localhost",            // clientHost
      m_rtcpdListenPort,      // clientPort
      request.clientEuid,
      request.clientEgid,
      request.deviceGroupName,
      request.driveName,
      reply);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SUBMIT_JOB_TO_RTCPD, 3, params);
    return;
  }

  // Prepare a positive response which will be overwritten if RTCPD replied to
  // the tape aggregator with an error message
  uint32_t    errorStatusForVdqm  = VDQM_CLIENTINFO; // Strange status code
  std::string errorMessageForVdqm = "";

  // If RTCOPY or tape aggregator daemon returned an error message
  // Checking the size of the error message because the status maybe non-zero
  // even if there is no error
  if(strlen(reply.errorMessage) > 1) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , reply.errorMessage),
      castor::dlf::Param("Code"    , reply.status)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, 3, params);

    // Override positive response with the error message from RTCPD
    errorStatusForVdqm  = reply.status;
    errorMessageForVdqm = reply.errorMessage;
  }

  // Acknowledge the VDQM - maybe positive or negative depending on reply from
  // RTCPD
  char replyBuf[MSGBUFSIZ];
  size_t replyLen = 0;

  try {
    replyLen = Marshaller::marshallRcpJobReplyMessage(replyBuf, reply);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_MARSHALL_RCP_JOB_REPLY_MESSAGE, 3, params);

    return;
  }

  try {
    SocketHelper::writeBytes(socket, NETRWTIMEOUT, replyLen, replyBuf);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SEND_RCP_JOB_REPLY_TO_VDQM, 3, params);
  }
}
