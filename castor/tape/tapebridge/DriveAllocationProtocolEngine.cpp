/******************************************************************************
 *                      DriveAllocationProtocolEngine.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapeserver/DlfMessageConstants.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/tapeserver/ClientTxRx.hpp"
#include "castor/tape/tapeserver/RtcpJobSubmitter.hpp"
#include "castor/tape/tapeserver/RtcpTxRx.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include <string.h>

#include <iostream>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::DriveAllocationProtocolEngine::
  DriveAllocationProtocolEngine(Counter<uint64_t> &tapeserverTransactionCounter)
  throw() : m_tapeserverTransactionCounter(tapeserverTransactionCounter) {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
castor::tape::tapegateway::Volume
  *castor::tape::tapeserver::DriveAllocationProtocolEngine::run(
  const Cuuid_t                       &cuuid,
  const int                           rtcpdCallbackSockFd,
  const char                          *rtcpdCallbackHost,
  const unsigned short                rtcpdCallbackPort,
  utils::SmartFd                      &rtcpdInitialSockFd,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest)
  throw(castor::exception::Exception) {
  
  // Pass a modified version of the job request through to RTCPD, setting the
  // clientHost and clientPort parameters to identify the tape tapeserver as
  // being a proxy for RTCPClientD
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , jobRequest.volReqId       ),
      castor::dlf::Param("Port"           , rtcpdCallbackPort         ),
      castor::dlf::Param("HostName"       , rtcpdCallbackHost         ),
      castor::dlf::Param("clientHost"     , jobRequest.clientHost     ),
      castor::dlf::Param("clientPort"     , jobRequest.clientPort     ),
      castor::dlf::Param("clientUserName" , jobRequest.clientUserName ),
      castor::dlf::Param("clientEuid"     , jobRequest.clientEuid     ),
      castor::dlf::Param("clientEgid"     , jobRequest.clientEgid     ),
      castor::dlf::Param("deviceGroupName", jobRequest.deviceGroupName),
      castor::dlf::Param("driveUnit"      , jobRequest.driveUnit      )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPESERVER_SUBMITTING_RTCOPY_JOB_TO_RTCPD, params);
  }

  legacymsg::RtcpJobReplyMsgBody rtcpdReply;
  RtcpJobSubmitter::submit(
    "localhost",               // host
    RTCOPY_PORT,               // port
    RTCPDNETRWTIMEOUT,         // netReadWriteTimeout
    "RTCPD",                   // remoteCopyType
    jobRequest.volReqId,
    jobRequest.clientUserName,
    rtcpdCallbackHost,
    rtcpdCallbackPort,
    jobRequest.clientEuid,
    jobRequest.clientEgid,
    jobRequest.deviceGroupName,
    jobRequest.driveUnit,
    rtcpdReply);

  // If RTCPD returned an error message
  // Checking the size of the error message because the status maybe non-zero
  // even if there is no error
  if(strlen(rtcpdReply.errorMessage) > 0) {
    castor::exception::Exception ex(rtcpdReply.status);

    ex.getMessage() <<
      "Received and error message from RTCPD"
      ": " << rtcpdReply.errorMessage;

    throw(ex);
  }

  // Accept the initial incoming RTCPD callback connection.
  // Wrap the socket file descriptor in a smart file descriptor so that it is
  // guaranteed to be closed when it goes out of scope.
  rtcpdInitialSockFd.reset(net::acceptConnection(rtcpdCallbackSockFd,
    RTCPDCALLBACKTIMEOUT));

  // Log the connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(rtcpdInitialSockFd.get(), ip, port);
    net::getPeerHostName(rtcpdInitialSockFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.volReqId    ),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)  ),
      castor::dlf::Param("Port"    , port                        ),
      castor::dlf::Param("HostName", hostName                    ),
      castor::dlf::Param("socketFd", rtcpdInitialSockFd.get()  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPESERVER_INITIAL_RTCPD_CALLBACK, params);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << jobRequest.volReqId);
  }

  // Get the request informatiom and the drive unit from RTCPD
  legacymsg::RtcpTapeRqstErrMsgBody rtcpdRequestInfoReply;
  RtcpTxRx::getRequestInfoFromRtcpd(cuuid, jobRequest.volReqId,
    rtcpdInitialSockFd.get(), RTCPDNETRWTIMEOUT, rtcpdRequestInfoReply);

  // If the VDQM and RTCPD drive units do not match
  if(strcmp(jobRequest.driveUnit, rtcpdRequestInfoReply.unit) != 0) {

    TAPE_THROW_CODE(EBADMSG,
      ": VDQM and RTCPD drive units do not match"
      ": VDQM drive unit='" << jobRequest.driveUnit           << "'"
      " RTCPD drive unit='" << rtcpdRequestInfoReply.volReqId << "'");
  }

  // If the VDQM and RTCPD volume request IDs do not match
  if(jobRequest.volReqId != rtcpdRequestInfoReply.volReqId) {

    TAPE_THROW_CODE(EBADMSG,
      ": VDQM and RTCPD volume request Ids do not match"
      ": VDQM volume request ID=" << jobRequest.volReqId <<
      " RTCPD volume request ID=" << rtcpdRequestInfoReply.volReqId);
  }

  // Get the volume from the tape gateway
  return ClientTxRx::getVolume(cuuid, jobRequest.volReqId,
    m_tapeserverTransactionCounter.next(), jobRequest.clientHost,
    jobRequest.clientPort, jobRequest.driveUnit);
}
