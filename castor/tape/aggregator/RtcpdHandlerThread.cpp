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
#include "castor/tape/aggregator/Utils.hpp"
#include "h/common.h"
#include "h/rtcp.h"

#include <string.h>
#include <time.h>


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

  // Get the volume request ID associated with the remote copy job from RTCPD
  uint32_t volReqId = 0;
  {
    RtcpTapeRequestMessage reply;
    try {
      getVolumeRequestIdFromRtcpd(cuuid, *socket, NETRWTIMEOUT, reply);
      volReqId = reply.VolReqID;

      castor::dlf::Param params[] = {
        castor::dlf::Param("vid"            , reply.vid            ),
        castor::dlf::Param("vsn"            , reply.vsn            ),
        castor::dlf::Param("label"          , reply.label          ),
        castor::dlf::Param("devtype"        , reply.devtype        ),
        castor::dlf::Param("density"        , reply.density        ),
        castor::dlf::Param("unit"           , reply.unit           ),
        castor::dlf::Param("VolReqID"       , reply.VolReqID       ),
        castor::dlf::Param("jobID"          , reply.jobID          ),
        castor::dlf::Param("mode"           , reply.mode           ),
        castor::dlf::Param("start_file"     , reply.start_file     ),
        castor::dlf::Param("end_file"       , reply.end_file       ),
        castor::dlf::Param("side"           , reply.side           ),
        castor::dlf::Param("tprc"           , reply.tprc           ),
        castor::dlf::Param("TStartRequest"  , reply.TStartRequest  ),
        castor::dlf::Param("TEndRequest"    , reply.TEndRequest    ),
        castor::dlf::Param("TStartRtcpd"    , reply.TStartRtcpd    ),
        castor::dlf::Param("TStartMount"    , reply.TStartMount    ),
        castor::dlf::Param("TEndMount"      , reply.TEndMount      ),
        castor::dlf::Param("TStartUnmount"  , reply.TStartUnmount  ),
        castor::dlf::Param("TEndUnmount"    , reply.TEndUnmount    ),
        castor::dlf::Param("rtcpReqId"      , reply.rtcpReqId      ),
        castor::dlf::Param("err.errmsgtxt"  , reply.err.errmsgtxt  ),
        castor::dlf::Param("err.severity"   , reply.err.severity   ),
        castor::dlf::Param("err.errorcode"  , reply.err.errorcode  ),
        castor::dlf::Param("err.max_tpretry", reply.err.max_tpretry),
        castor::dlf::Param("err.max_cpretry", reply.err.max_cpretry)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_VOLREQID, 26, params);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_GET_VOLREQID, 3, params);
    }
  }

  // Send volume to RTCPD
  uint32_t tStartRequest = time(NULL); // CASTOR2/rtcopy/rtcpclientd.c:1494
  {
    RtcpTapeRequestMessage reply;
    try {
      sendVolumeToRtcpd(cuuid, *socket, NETRWTIMEOUT, reply, "I10547", "I10547",
        "aul", "700GC", volReqId, tStartRequest);

      castor::dlf::Param params[] = {
        castor::dlf::Param("vid"            , reply.vid            ),
        castor::dlf::Param("vsn"            , reply.vsn            ),
        castor::dlf::Param("label"          , reply.label          ),
        castor::dlf::Param("devtype"        , reply.devtype        ),
        castor::dlf::Param("density"        , reply.density        ),
        castor::dlf::Param("unit"           , reply.unit           ),
        castor::dlf::Param("VolReqID"       , reply.VolReqID       ),
        castor::dlf::Param("jobID"          , reply.jobID          ),
        castor::dlf::Param("mode"           , reply.mode           ),
        castor::dlf::Param("start_file"     , reply.start_file     ),
        castor::dlf::Param("end_file"       , reply.end_file       ),
        castor::dlf::Param("side"           , reply.side           ),
        castor::dlf::Param("tprc"           , reply.tprc           ),
        castor::dlf::Param("TStartRequest"  , reply.TStartRequest  ),
        castor::dlf::Param("TEndRequest"    , reply.TEndRequest    ),
        castor::dlf::Param("TStartRtcpd"    , reply.TStartRtcpd    ),
        castor::dlf::Param("TStartMount"    , reply.TStartMount    ),
        castor::dlf::Param("TEndMount"      , reply.TEndMount      ),
        castor::dlf::Param("TStartUnmount"  , reply.TStartUnmount  ),
        castor::dlf::Param("TEndUnmount"    , reply.TEndUnmount    ),
        castor::dlf::Param("rtcpReqId"      , reply.rtcpReqId      ),
        castor::dlf::Param("err.errmsgtxt"  , reply.err.errmsgtxt  ),
        castor::dlf::Param("err.severity"   , reply.err.severity   ),
        castor::dlf::Param("err.errorcode"  , reply.err.errorcode  ),
        castor::dlf::Param("err.max_tpretry", reply.err.max_tpretry),
        castor::dlf::Param("err.max_cpretry", reply.err.max_cpretry)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_SENT_VOLUME, 26, params);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_SEND_VOLUME, 3, params);
    }
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
// getVolumeRequestIdFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::
  getVolumeRequestIdFromRtcpd(const Cuuid_t &cuuid,
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  RtcpTapeRequestMessage &reply) throw(castor::exception::Exception) {

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
    SocketHelper::writeBytes(socket, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send request for volume request ID to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(cuuid, socket, netReadWriteTimeout, ackMsg);
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
    receiveRtcpTapeRequest(cuuid, socket, netReadWriteTimeout, reply);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive tape request from RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }

  // Send acknowledge to RTCPD
  try {
    sendRtcpAcknowledge(cuuid, socket, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send acknowledge to RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// sendVolumeToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::
  sendVolumeToRtcpd(const Cuuid_t &cuuid,
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  RtcpTapeRequestMessage &reply, const char *vid, const char *vsn,
  const char *label, const char *density, const uint32_t volReqId,
  const uint32_t tStartRequest) throw(castor::exception::Exception) {

  // Prepare logical volume message
  RtcpTapeRequestMessage volume;
  Utils::setBytes(volume, '\0');

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRequestMessage(buf, volume);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall volume message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the message
  try {
    SocketHelper::writeBytes(socket, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send volume message to RTCPD: "
      << ex.getMessage().str();
  }

  // Receive acknowledge from RTCPD
  RtcpAcknowledgeMessage ackMsg;
  try {
    receiveRtcpAcknowledge(cuuid, socket, netReadWriteTimeout, ackMsg);
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
    receiveRtcpTapeRequest(cuuid, socket, netReadWriteTimeout, reply);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to receive tape request from RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }

  // Send acknowledge to RTCPD
  try {
    sendRtcpAcknowledge(cuuid, socket, netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(EPROTO);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send acknowledge to RTCPD"
         ": " << ex.getMessage().str();
    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpAcknowledge
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpAcknowledge(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, RtcpAcknowledgeMessage &message)
  throw(castor::exception::Exception) {

  // Read in the RTCPD acknowledge message (there is no separate header and
  // body)
  char messageBuf[3 * sizeof(uint32_t)]; // magic + request type + status
  try {
    SocketHelper::readBytes(socket, NETRWTIMEOUT, sizeof(messageBuf),
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
    Marshaller::unmarshallRtcpAcknowledgeMessage(p, remainingLen, message);
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
void castor::tape::aggregator::RtcpdHandlerThread::sendRtcpAcknowledge(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, const RtcpAcknowledgeMessage &message)
  throw(castor::exception::Exception) {

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
    SocketHelper::writeBytes(socket, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send the RCP acknowledge message to RTCPD"
         ": " << ex.getMessage().str();

    throw ex2;
  }
}


//-----------------------------------------------------------------------------
// receiveRtcpTapeRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpTapeRequest(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, RtcpTapeRequestMessage &request)
  throw (castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    SocketHelper::readBytes(socket, NETRWTIMEOUT, sizeof(headerBuf), headerBuf);
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

  // 3 * sizeof(uint32_t) = length of the header
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
    SocketHelper::readBytes(socket, NETRWTIMEOUT, header.len, bodyBuf);
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
    Marshaller::unmarshallRtcpTapeRequestMessage(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}
