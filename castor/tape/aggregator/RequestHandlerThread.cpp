/******************************************************************************
 *                castor/tape/aggregator/RequestHandlerThread.cpp
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
#include "castor/tape/aggregator/RequestHandlerThread.hpp"
#include "castor/tape/aggregator/RCPJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RequestHandlerThread::RequestHandlerThread()
  throw () : m_jobQueue(1) {

  m_rtcopyMagicOld0Handlers[VDQM_CLIENTINFO] =
    &RequestHandlerThread::handleJobSubmission;

  m_magicToHandlers[RTCOPY_MAGIC_OLD0] = &m_rtcopyMagicOld0Handlers;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RequestHandlerThread::~RequestHandlerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::ServerSocket *socket = (castor::io::ServerSocket*)param;

  try {

    dispatchRequest(cuuid, *socket);

  } catch(castor::exception::Exception &ex) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_HANDLE_REQUEST_EXCEPT, 2, params);
  }

  // Close and de-allocate the socket
  socket->close();
  delete socket;
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// dispatchRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::dispatchRequest(
  Cuuid_t &cuuid, castor::io::ServerSocket &socket)
  throw(castor::exception::Exception) {

  // Read and unmarshall the magic number of the request
  uint32_t magic = 0;
  try {
    magic = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_MAGIC, 2, params);

    return;
  }

  // Find the map of request handlers for the magic number
  MagicToHandlersMap::iterator handlerMapItor = m_magicToHandlers.find(magic);
  if(handlerMapItor == m_magicToHandlers.end()) {
    // Unknown magic number
    castor::dlf::Param params[] = {
      castor::dlf::Param("Magic Number", magic)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, AGGREGATOR_UNKNOWN_MAGIC, 1,
      params);

    return;
  }
  HandlerMap *handlers = handlerMapItor->second;

  // Read and unmarshall the type of the request
  uint32_t reqtype = 0;
  try {
    reqtype = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_REQUEST_TYPE, 2, params);

    return;
  }

  // Find the request handler for the type of request
  HandlerMap::iterator handlerItor = handlers->find(reqtype);
  if(handlerItor == handlers->end()) {
    // Unknown request type
    castor::dlf::Param params[] = {
      castor::dlf::Param("Magic Number", magic),
      castor::dlf::Param("Request Type", reqtype)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_UNKNOWN_REQUEST_TYPE, 2, params);

    return;
  }
  Handler handler = handlerItor->second;

  // Read and unmarshall the length of the message body of the request
  uint32_t len = 0;
  try {
    len = SocketHelper::readUint32(socket, NETRW_TIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_MESSAGE_BODY_LENGTH, 2, params);

    return;
  }

  // If the message body is larger than the message body buffer
  if(len > MSGBUFSIZ - 3 * sizeof(uint32_t)) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Maximum length", MSGBUFSIZ - 3 * sizeof(uint32_t)),
      castor::dlf::Param("Actual length", len)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_MESSAGE_BODY_LENGTH_TOO_LARGE, 2, params);

    return;
  }

  // Only need a buffer for the message body, the header has already been read
  // from the socket and unmarshalled
  char body[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // Read the message body from the socket
  try {
    SocketHelper::readBytes(socket, NETRW_TIMEOUT, len, body);
  } catch (castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_READ_MESSAGE_BODY, 2, params);

    return;
  }

  // Dispatch the request to the appropriate handler
  (this->*handler)(cuuid, magic, reqtype, len, body, socket);
}


//-----------------------------------------------------------------------------
// handleJobSubmission
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RequestHandlerThread::handleJobSubmission(
  Cuuid_t &cuuid, const uint32_t magic, const uint32_t reqtype,
  const uint32_t len, char *body, castor::io::ServerSocket &socket) throw() {

  // If the message body is too small
  if(len < 4 * sizeof(uint32_t) + 4) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Minimum Length", 4 * sizeof(uint32_t) + 4),
      castor::dlf::Param("Actual Length", len)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_JOB_MESSAGE_BODY_LENGTH_TOO_SHORT, 2, params);
  }

  uint32_t   ui32          = 0; // Used to for casts
  u_signed64 tapeRequestID = 0;
  uint32_t   clientPort    = 0;
  uint32_t   clientEuid    = 0;
  uint32_t   clientEgid    = 0;
  char       clientHost[CA_MAXHOSTNAMELEN+1];
  char       dgn[CA_MAXDGNLEN+1];
  char       driveName[CA_MAXUNMLEN+1];
  char       clientUsername[CA_MAXUSRNAMELEN+1];

  char   *p      = body;
  size_t bodyLen = len;

  Marshaller::unmarshallUint32(p, bodyLen, ui32);
  tapeRequestID = (u_signed64)ui32; // Cast from 32-bits to 64-bits
  Marshaller::unmarshallUint32(p, bodyLen, clientPort);
  Marshaller::unmarshallUint32(p, bodyLen, clientEuid);
  Marshaller::unmarshallUint32(p, bodyLen, clientEgid);
  Marshaller::unmarshallString(p, bodyLen, clientHost, sizeof(clientHost));
  Marshaller::unmarshallString(p, bodyLen, dgn, sizeof(dgn));
  Marshaller::unmarshallString(p, bodyLen, driveName, sizeof(driveName));
  Marshaller::unmarshallString(p, bodyLen, clientUsername,
    sizeof(clientUsername));

  castor::dlf::Param param[] = {
    castor::dlf::Param("tapeRequestID" , tapeRequestID ),
    castor::dlf::Param("clientPort"    , clientPort    ),
    castor::dlf::Param("clientEuid"    , clientEuid    ),
    castor::dlf::Param("clientEgid"    , clientEgid    ),
    castor::dlf::Param("clientHost"    , clientHost    ),
    castor::dlf::Param("dgn"           , dgn           ),
    castor::dlf::Param("driveName"     , driveName     ),
    castor::dlf::Param("clientUsername", clientUsername)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_HANDLE_JOB_MESSAGE, 8, param);

  // TEMPORARY
  // Acknowledge the VDQM
  char ackMsg[MSGBUFSIZ];

  size_t ackMsgLen = 0;

  try {
    ackMsgLen = marshallRTCPAckn(ackMsg, sizeof(ackMsg), 0, "");
//  ackMsgLen = marshallRTCPAckn(ackMsg, sizeof(ackMsg), 5, "Steve was here!!");
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_MARSHALL_RTCP_ACKN, 2, params);
  }

  const int rc = netwrite_timeout(socket.socket(), ackMsg, ackMsgLen,
    NETRW_TIMEOUT);

  try {
    if(rc == -1) {
      castor::exception::Exception ex(SECOMERR);
      ex.getMessage() << __PRETTY_FUNCTION__
        << ": netwrite(REQ) to VDQM: " << neterror();
      throw ex;
    } else if(rc == 0) {
      castor::exception::Exception ex(SECONNDROP);
      ex.getMessage() << __PRETTY_FUNCTION__
        << ": netwrite(REQ) to VDQM: Connection dropped";
      throw ex;
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SEND_RTCP_ACKN_TO_VDQM, 2, params);
  }


/*
  try {
    RCPJobSubmitter::submit(
    "localhost",   // host
    RTCOPY_PORT,   // port
    NETRW_TIMEOUT, // netReadWriteTimeout
    "RTCPD",       // remoteCopyType
    tapeRequestID,
    clientUsername,
    clientHost,
    clientPort,
    clientEuid,
    clientEgid,
    dgn,
    driveName);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SUBMIT_JOB_TO_RTCPD, 2, params);

    return;
  }
*/
}


//-----------------------------------------------------------------------------
// marshallRTCPAckn
//-----------------------------------------------------------------------------
size_t castor::tape::aggregator::RequestHandlerThread::marshallRTCPAckn(
  char *dst, const size_t dstLen, const uint32_t status, const char *errorMsg)
  throw (castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to destination buffer is NULL";
    throw ex;
  }

  // Minimum buffer length = magic number + request type + length + status code
  // + the string termination character '\0'
  if(dstLen < 4*sizeof(uint32_t) + 1) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Destination buffer length is too small: "
      "Expected minimum length: " << (4*sizeof(uint32_t) + 1)
       << ": Actual length: " << dstLen;
    throw ex;
  }

  if(errorMsg == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to error message is NULL";
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

  Marshaller::marshallUint32(RTCOPY_MAGIC_OLD0, dst); // Magic number
  Marshaller::marshallUint32(VDQM_CLIENTINFO  , dst); // Request type
  Marshaller::marshallUint32(len              , dst); // Length
  Marshaller::marshallUint32(status           , dst); // status code

  memcpy(dst, errorMsg, errorMsg2SendLen);
  *(dst+errorMsg2SendLen) = '\0';

  return 3*sizeof(uint32_t) + len; // Header + body
}
