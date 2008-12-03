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
#include "castor/tape/aggregator/RCPJobSubmitter.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
#include "castor/tape/aggregator/exception/RTCPDErrorMessage.hpp"
#include "h/common.h"
#include "h/rtcp.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdHandlerThread::RtcpdHandlerThread() throw() {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdHandlerThread::~RtcpdHandlerThread()
  throw () {
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
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RTCPD_HANDLER_SOCKET_IS_NULL);
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
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RTCPD_CONNECTION_WITHOUT_INFO, 2, params);
  }

  std::string vid;

{
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Just before getVidFromRtcpd")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NULL, 1, params);
}

  try {
    vid = getVidFromRtcpd(*socket);

{
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Just after getVidFromRtcpd")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NULL, 1, params);
}

    castor::dlf::Param params[] = {
      castor::dlf::Param("VID", vid)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_VID_FROM_RTCPD, 1, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_GET_VID_FROM_RTCPD, 2, params);
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
  castor::io::AbstractTCPSocket &socket) throw(castor::exception::Exception) {

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

    ex.getMessage() << "Failed to send request for VID to RTCPD: "
      << sstrerror(se);

    throw ex;
  }

  if(rtcp_RecvAckn(&s, hdr.reqtype) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << "Failed to receive acknowldege from RTCPD after sending"
      " request for VID: " << sstrerror(se);

    throw ex;
  }

  if(rtcp_RecvReq(&s, &hdr, NULL, &tapereq, NULL) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << "Failed to receive VID from RTCPD after receiving the"
      " acknowledge: " << sstrerror(se);

    throw ex;
  }

  if(rtcp_SendAckn(&s, hdr.reqtype) == -1) {
    const int se = serrno;
    castor::exception::Exception ex(se);

    ex.getMessage() << "Failed to send acknowledge to RTCPD after receiving"
      " the VID: " << sstrerror(se);

    throw ex;
  }

  if(tapereq.VolReqID <= 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Failed to get VID from RTCPD: No volume request ID";
    throw ex;
  }

  return tapereq.vid;
*/
}
