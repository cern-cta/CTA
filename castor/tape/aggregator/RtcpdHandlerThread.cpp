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

  std::string vid;

{
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , "Just before getVidFromRtcpd")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NULL, 2, params);
}

  try {
    vid = getVidFromRtcpd(*socket, NETRWTIMEOUT);

{
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , "Just after getVidFromRtcpd")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NULL, 2, params);
}

    castor::dlf::Param params[] = {
      castor::dlf::Param("VID"     , vid)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_VID_FROM_RTCPD, 1, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_GET_VID_FROM_RTCPD, 3, params);
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
// getVidFromRtcpd
//-----------------------------------------------------------------------------
std::string castor::tape::aggregator::RtcpdHandlerThread::getVidFromRtcpd(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  char buf[MSGBUFSIZ];
  RtcpTapeRequest request;

  memset(&request, '\0', sizeof(request));

  // Marshall the request for VID message
  size_t totalLen = 0;

  try {
    totalLen = Marshaller::marshallRtcpTapeRequest(&buf[0], sizeof(buf),
      request);
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

  // Receive acknowlege from RTCPD
  uint32_t status = 0;

  try {
    status = receiveRtcpdAcknowledge(socket, netReadWriteTimeout);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowlege from RTCPD: "
      << ex.getMessage().str();
    throw ex2;
  }

  if(status != 0) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Received negative acknowledge from RTCPD";
    throw ex;
  }

  return "NOTIMP";
/*
  SOCKET            s = socket.socket();
  rtcpHdr_t         hdr;
  rtcpTapeRequest_t tapereq;

  hdr.magic   = RTCOPY_MAGIC;
  hdr.reqtype = RTCP_TAPEERR_REQ;
  hdr.len     = -1;

  tapereq.vid[0] = '\0';

  if(rtcp_SendReq(&s, &hdr, NULL, &tapereq, NULL) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send request for VID to RTCPD: "
      << sstrerror(se);

    throw ex;
  }

  if(rtcp_RecvAckn(&s, hdr.reqtype) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive acknowldege from RTCPD after sending"
      " request for VID: " << sstrerror(se);

    throw ex;
  }

  if(rtcp_RecvReq(&s, &hdr, NULL, &tapereq, NULL) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive VID from RTCPD after receiving the"
      " acknowledge: " << sstrerror(se);

    throw ex;
  }

  if(rtcp_SendAckn(&s, hdr.reqtype) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send acknowledge to RTCPD after receiving"
      " the VID: " << sstrerror(se);

    throw ex;
  }

  if(tapereq.VolReqID <= 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get VID from RTCPD: No volume request ID";
    throw ex;
  }

  return tapereq.vid;
*/
}


//-----------------------------------------------------------------------------
// receiveRtcpdAcknowledge
//-----------------------------------------------------------------------------
uint32_t castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpdAcknowledge(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout)
  throw(castor::exception::Exception) {

  // Read and unmarshall the magic number
  uint32_t magic = 0;
  try {
    magic = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall magic number: "
      << ex.getMessage().str();

    throw ie;
  }

  // If the magic number is invalid
  if(magic != RTCOPY_MAGIC) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid magic number from RTCPD"
      << ": Expected: 0x" << std::hex << RTCOPY_MAGIC
      << ": Received: " << magic;

    throw ex;
  }

  // Read and unmarshall the request type
  uint32_t reqtype = 0;
  try {
    reqtype = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall request type: "
      << ex.getMessage().str();

    throw ie;
  }

  // If the request type is invalid
  if(reqtype != RTCP_TAPEERR_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_TAPEERR_REQ
      << ": Received: " << reqtype;

    throw ex;
  }

  // Read and unmarshall the status
  uint32_t status = 0;
  try {
    status = SocketHelper::readUint32(socket, NETRWTIMEOUT);
  } catch (castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to read and unmarshall status: "
      << ex.getMessage().str();

    throw ie;
  }

  return status;
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpTapeRequest(
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  const RtcpTapeRequest &request) throw (castor::exception::Exception) {

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
}
