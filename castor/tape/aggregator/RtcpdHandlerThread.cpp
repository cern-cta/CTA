/******************************************************************************
 *                castor/tape/aggregator/RtcpdHandlerThread.cpp
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
#include "castor/tape/aggregator/RtcpdHandlerThread.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "castor/tape/aggregator/exception/RTCPDErrorMessage.hpp"
#include "h/common.h"
#include "h/rtcp.h"
#include "h/rtcp_constants.h"

#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdHandlerThread::RtcpdHandlerThread() throw() {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdHandlerThread::~RtcpdHandlerThread()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RTCPD_HANDLER_SOCKET_IS_NULL, 1, params);
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
      AGGREGATOR_RTCPD_CONNECTION_WITH_INFO, 2, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RTCPD_CONNECTION_WITHOUT_INFO, 3, params);
  }

  // Get the tape arequest associated with the remote copy job from RTCPD
  RtcpTapeRequest request;
  try {
    getTapeRequestFromRtcpd(cuuid, *socket, NETRWTIMEOUT, request);

    castor::dlf::Param params[] = {
      castor::dlf::Param("vid"            , request.vid            ),
      castor::dlf::Param("vsn"            , request.vsn            ),
      castor::dlf::Param("label"          , request.label          ),
      castor::dlf::Param("devtype"        , request.devtype        ),
      castor::dlf::Param("density"        , request.density        ),
      castor::dlf::Param("unit"           , request.unit           ),
      castor::dlf::Param("VolReqID"       , request.VolReqID       ),
      castor::dlf::Param("jobID"          , request.jobID          ),
      castor::dlf::Param("mode"           , request.mode           ),
      castor::dlf::Param("start_file"     , request.start_file     ),
      castor::dlf::Param("end_file"       , request.end_file       ),
      castor::dlf::Param("side"           , request.side           ),
      castor::dlf::Param("tprc"           , request.tprc           ),
      castor::dlf::Param("TStartRequest"  , request.TStartRequest  ),
      castor::dlf::Param("TEndRequest"    , request.TEndRequest    ),
      castor::dlf::Param("TStartRtcpd"    , request.TStartRtcpd    ),
      castor::dlf::Param("TStartMount"    , request.TStartMount    ),
      castor::dlf::Param("TEndMount"      , request.TEndMount      ),
      castor::dlf::Param("TStartUnmount"  , request.TStartUnmount  ),
      castor::dlf::Param("TEndUnmount"    , request.TEndUnmount    ),
      castor::dlf::Param("rtcpReqId"      , request.rtcpReqId      ),
      castor::dlf::Param("err.errmsgtxt"  , request.err.errmsgtxt  ),
      castor::dlf::Param("err.severity"   , request.err.severity   ),
      castor::dlf::Param("err.errorcode"  , request.err.errorcode  ),
      castor::dlf::Param("err.max_tpretry", request.err.max_tpretry),
      castor::dlf::Param("err.max_cpretry", request.err.max_cpretry)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RECEIVED_TAPE_REQUEST, 26, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_RECEIVE_TAPE_REQUEST, 3, params);
  }

  // Close and de-allocate the socket
  socket->close();
  delete socket;
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// getTapeRequestFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::
  getTapeRequestFromRtcpd(const Cuuid_t &cuuid,
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  RtcpTapeRequest &request) throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];

  memset(&request, '\0', sizeof(request));

  // Marshall the request for VID message
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRequest(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall RTCP request message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the request for a VID to RTCPD
  try {
    SocketHelper::writeBytes(socket, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send the request for a VID to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpdAcknowledge ackMsg;
  try {
    receiveRtcpdAcknowledge(cuuid, socket, netReadWriteTimeout, ackMsg);
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

  // Receive tape request message from RTCPD
  memset(&request, '\0', sizeof(request));
  try {
    receiveRtcpTapeRequest(cuuid, socket, netReadWriteTimeout, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive tape request from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  // Send acknowledge to RTCP
/*
TO BE DONE!!!!!
  if(rtcp_SendAckn(&s, hdr.reqtype) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send acknowledge to RTCPD after receiving"
      " the VID: " << sstrerror(se);

    throw ex;
  }
*/
}


//-----------------------------------------------------------------------------
// receiveRtcpdAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpdAcknowledge(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, RtcpdAcknowledge &message)
  throw(castor::exception::Exception) {

  // Read and unmarshall the magic number
  try {
    message.magic = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall magic number: "
      << ex.getMessage().str();

    throw ie;
  }

  // Read and unmarshall the request type
  try {
    message.reqtype = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall request type: "
      << ex.getMessage().str();

    throw ie;
  }

  // Read and unmarshall the status
  try {
    message.status = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall status: "
      << ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// sendRtcpdAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::sendRtcpdAcknowledge(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, const RtcpdAcknowledge &message)
  throw(castor::exception::Exception) {

  castor::exception::Internal ie;

  ie.getMessage() << "Not implemented";

  throw ie;
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpTapeRequest(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, RtcpTapeRequest &request)
  throw (castor::exception::Exception) {

  // Read and unmarshall the magic number
  uint32_t magic = 0;
  try {
    magic = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read magic number from RTCPD: "
      << ex.getMessage().str();

    throw ex2;
  }

  // If the magic number is invalid
  if(magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EBADMSG);

     ex.getMessage() << __PRETTY_FUNCTION__
       << std::hex
       << ": Invalid magic number from RTCPD"
       << ": Expected: 0x" << RTCOPY_MAGIC
       << ": Received: 0x" << magic;

     throw ex;
  }

  // Read and unmarshall the request type
  uint32_t reqtype = 0;
  try {
    reqtype = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read request type from RTCPD: "
      << ex.getMessage().str();

    throw ex2;
  }

  // If the request type is invalid
  if(reqtype != RTCP_TAPEERR_REQ) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << RTCP_TAPEERR_REQ
      << ": Received: 0x" << reqtype;

    throw ex;
  }

  // Read and unmarshall the type of the request
  uint32_t len = 0;
  try {
    len = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body length from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Only need a buffer for the message body, the header has already been read
  // from the socket and unmarshalled
  char body[MSGBUFSIZ - 3 * sizeof(uint32_t)];

  // If the message body is too large
  if(len > sizeof(body)) {
    castor::exception::Exception ex(EMSGSIZE);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Message body from RTCPD is too large"
         ": Maximum: " << sizeof(body)
      << ": Received: " << len;

    throw ex;
  }

  // Read the message body
  try {
    SocketHelper::readBytes(socket, NETRWTIMEOUT, len, body);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EIO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ex2;
  }

  // Unmarshall the message body
  const char *p           = body;
  size_t     remainingLen = len;
  try {
    Marshaller::unmarshallRtcpTapeRequest(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}
