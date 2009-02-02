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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMessage.hpp"
#include "castor/tape/aggregator/RtcpTapeRequestMessage.hpp"
#include "castor/tape/aggregator/RtcpFileRequestMessage.hpp"
#include "castor/tape/aggregator/Transceiver.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/aggregator/VdqmRequestHandlerThread.hpp"
#include "h/common.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <sys/select.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandlerThread::VdqmRequestHandlerThread(
  const int rtcpdListenPort) throw() :
  m_rtcpdListenPort(rtcpdListenPort), m_jobQueue(1) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandlerThread::~VdqmRequestHandlerThread()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::init() throw() {
/* TEST CODE
  Cuuid_t cuuid = nullCuuid;

  // Give a Cuuid to the test code
  Cuuid_create(&cuuid);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("TEST", "Start of test code")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, 1, params);
  }

  try {
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("TEST", "Creating listener socket")};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, 1,
        params);
    }

    // Create, bind and mark a listener socket
    const int listenSocketFd = Net::createListenerSocket(12345);

    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("TEST", "Waiting for a connection")};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, 1,
        params);
    }

    // Accept a connection with a timeout
    const int connectedSocketFd = Net::acceptConnection(listenSocketFd,
      10);

    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(connectedSocketFd, ip, port);
    Net::getPeerHostName(connectedSocketFd, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("TEST"    , "Accepted a connection"),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NULL, 4, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("TEST"    , "Test code through as exception"),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, AGGREGATOR_NULL, 4, params);
  }
*/
}


//-----------------------------------------------------------------------------
// processJobSubmissionRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processJobSubmissionRequest(const Cuuid_t &cuuid, const int vdqmSocketFd,
  RcpJobRequestMessage &jobRequest, const int rtcpdCallbackSocketFd)
  throw(castor::exception::Exception) {

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(vdqmSocketFd, ip, port);
    Net::getPeerHostName(vdqmSocketFd, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, 3, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, 3, params);
  }

  Utils::setBytes(jobRequest, 0);

  checkRcpJobSubmitterIsAuthorised(vdqmSocketFd);

  Transceiver::receiveRcpJobRequest(vdqmSocketFd, RTCPDNETRWTIMEOUT,
    jobRequest);
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , jobRequest.tapeRequestId  ),
      castor::dlf::Param("clientPort"     , jobRequest.clientPort     ),
      castor::dlf::Param("clientEuid"     , jobRequest.clientEuid     ),
      castor::dlf::Param("clientEgid"     , jobRequest.clientEgid     ),
      castor::dlf::Param("clientHost"     , jobRequest.clientHost     ),
      castor::dlf::Param("deviceGroupName", jobRequest.deviceGroupName),
      castor::dlf::Param("driveName"      , jobRequest.driveName      ),
      castor::dlf::Param("clientUserName" , jobRequest.clientUserName )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_HANDLE_JOB_MESSAGE, 8, params);
  }

  // Get the IP and port of the RTCPD callback socket
  unsigned long  rtcpdCallbackSocketIp   = 0;
  unsigned short rtcpdCallbackSocketPort = 0;
  Net::getSocketIpAndPort(rtcpdCallbackSocketFd, rtcpdCallbackSocketIp,
    rtcpdCallbackSocketPort);
  char rtcpdCallbackHostName[HOSTNAMEBUFLEN];
  Net::getSocketHostName(rtcpdCallbackSocketFd, rtcpdCallbackHostName);

  // Pass a modified version of the job request through to RTCPD, setting the
  // clientHost and clientPort parameters to identify the tape aggregator as
  // being a proxy for RTCPClientD
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.tapeRequestId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SUBMITTING_JOB_TO_RTCPD, 1, params);
  }
  RcpJobReplyMessage rtcpdReply;
  RcpJobSubmitter::submit(
    "localhost",               // host
    RTCOPY_PORT,               // port
    RTCPDNETRWTIMEOUT,         // netReadWriteTimeout
    "RTCPD",                   // remoteCopyType
    jobRequest.tapeRequestId,
    jobRequest.clientUserName,
    rtcpdCallbackHostName,
    rtcpdCallbackSocketPort,
    jobRequest.clientEuid,
    jobRequest.clientEgid,
    jobRequest.deviceGroupName,
    jobRequest.driveName,
    rtcpdReply);

  // Prepare a positive response for the VDQM which will be overwritten if
  // RTCPD replied to the tape aggregator with an error message
  uint32_t    errorStatusForVdqm  = VDQM_CLIENTINFO; // Strange status code
  std::string errorMessageForVdqm = "";

  // If RTCPD returned an error message
  // Checking the size of the error message because the status maybe non-zero
  // even if there is no error
  if(strlen(rtcpdReply.errorMessage) > 0) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.tapeRequestId),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__     ),
      castor::dlf::Param("Message" , rtcpdReply.errorMessage ),
      castor::dlf::Param("Code"    , rtcpdReply.status       )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, 4, params);

    // Override positive response with the error message from RTCPD
    errorStatusForVdqm  = rtcpdReply.status;
    errorMessageForVdqm = rtcpdReply.errorMessage;
  }

  // Acknowledge the VDQM - maybe positive or negative depending on reply
  // from RTCPD
  char vdqmReplyBuf[MSGBUFSIZ];
  size_t vdqmReplyLen = 0;

  vdqmReplyLen = Marshaller::marshallRcpJobReplyMessage(vdqmReplyBuf,
    rtcpdReply);

  Net::writeBytes(vdqmSocketFd, RTCPDNETRWTIMEOUT, vdqmReplyLen, vdqmReplyBuf);
}


//-----------------------------------------------------------------------------
// processErrorOnInitialRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processErrorOnInitialRtcpdConnection(const Cuuid_t &cuuid,
  const RcpJobRequestMessage &vdqmJobRequest, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_DATA_ON_INITIAL_RTCPD_CONNECTION, 1, params);
  }

  RtcpTapeRequestMessage request;

  Transceiver::receiveRtcpTapeRequest(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
    request);

  // TBD - Process incoming request?
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  acceptRtcpdConnection(const Cuuid_t &cuuid,
  const RcpJobRequestMessage &vdqmJobRequest, const int rtcpdCallbackSocketFd,
  std::list<int> &connectedSockets) throw(castor::exception::Exception) {

  const int connectedSocketFd = Net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDCALLBACKTIMEOUT);

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(connectedSocketFd, ip, port);
    Net::getPeerHostName(connectedSocketFd, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, 4, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, 1, params);
  }

  connectedSockets.push_back(connectedSocketFd);
}


//-----------------------------------------------------------------------------
// processTapeDiskIoConnections
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processTapeDiskIoConnections(const Cuuid_t &cuuid,
  const RcpJobRequestMessage &vdqmJobRequest, std::list<int> &connectedSockets)
  throw(castor::exception::Exception) {
/*
  // Give file information to RTCPD
  try {
    Transceiver::giveFileListToRtcpd(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
      vdqmJobRequest.tapeRequestId, "lxc2disk07:/dev/null", 18, false);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_GIVE_FILE_INFO, 3, params);
  }
*/
}


//-----------------------------------------------------------------------------
// processRtcpdSockets
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processRtcpdSockets(const Cuuid_t &cuuid,
  const RcpJobRequestMessage &vdqmJobRequest, const int rtcpdCallbackSocketFd,
  const int rtcpdInitialSocketFd) throw(castor::exception::Exception) {

  std::list<int> connectedSockets;
  int selectRc = 0;
  int selectErrno = 0;
  fd_set readFdSet;
  int maxFd = 0;
  timeval timeout;

  try {
    // Select loop
    bool selectAgain = true;
    while(selectAgain) {

      // Build the file descriptor set ready for the select call
      FD_ZERO(&readFdSet);
      FD_SET(rtcpdInitialSocketFd, &readFdSet);
      if(rtcpdCallbackSocketFd > rtcpdInitialSocketFd) {
        maxFd = rtcpdCallbackSocketFd;
      } else {
        maxFd = rtcpdInitialSocketFd;
      }
      for(std::list<int>::iterator itor = connectedSockets.begin();
        itor != connectedSockets.end(); itor++) {

        FD_SET(*itor, &readFdSet);

        if(*itor > maxFd) {
          maxFd = *itor;
        }
      }

      timeout.tv_sec  = RTCPDNETRWTIMEOUT;
      timeout.tv_usec = 0;

      selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
      selectErrno = errno;

      switch(selectRc) {
      case 0: // Select timed out
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
          AGGREGATOR_MAIN_SELECT_TIMEDOUT);
        break;
      case -1: // Select encountered an error
        // If select encountered an error other than an interruption
        if(selectErrno != EINTR) {
          char strerrorBuf[STRERRORBUFLEN];
          char *const errorStr = strerror_r(selectErrno, strerrorBuf,
            sizeof(strerrorBuf));

          castor::exception::Exception ex(selectErrno);

          ex.getMessage() << __PRETTY_FUNCTION__
            << ": Select encountered an error other than an interruption"
               ": " << errorStr;

          throw ex;
        }
        break;
      default: // One or more select file descriptors require attention
        // If there is an incoming message on the initial RTCPD connection
        if(FD_ISSET(rtcpdInitialSocketFd, &readFdSet)) {

          processErrorOnInitialRtcpdConnection(cuuid, vdqmJobRequest,
            rtcpdInitialSocketFd);

        // Else if there is a callback connection request from RTCPD
        } else if(FD_ISSET(rtcpdCallbackSocketFd, &readFdSet)) {

          acceptRtcpdConnection(cuuid, vdqmJobRequest, rtcpdCallbackSocketFd,
            connectedSockets);

        // Else there are one or messages from the tape and disk I/O threads
        } else {
          processTapeDiskIoConnections(cuuid, vdqmJobRequest, connectedSockets);
        }
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_MAIN_SELECT_FAILED, 4, params);
  }

  // Close all connected sockets
  for(
    std::list<int>::iterator itor = connectedSockets.begin();
    itor != connectedSockets.end();
    itor++) {
    close(*itor);
  }
}


//-----------------------------------------------------------------------------
// coordinateRemoteCopy
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::coordinateRemoteCopy(
  const Cuuid_t &cuuid, const RcpJobRequestMessage &vdqmJobRequest,
  const int rtcpdCallbackSocketFd) throw(castor::exception::Exception) {

  // Accept the initial incoming RTCPD callback connection
  int rtcpdInitialSocketFd = 0;
  try {
    rtcpdInitialSocketFd = Net::acceptConnection(rtcpdCallbackSocketFd,
      RTCPDCALLBACKTIMEOUT);

    try {
      unsigned short port = 0; // Client port
      unsigned long  ip   = 0; // Client IP
      char           hostName[HOSTNAMEBUFLEN];

      Net::getPeerIpAndPort(rtcpdInitialSocketFd, ip, port);
      Net::getPeerHostName(rtcpdInitialSocketFd, hostName);

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
        castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)  ),
        castor::dlf::Param("Port"    , port                        ),
        castor::dlf::Param("HostName", hostName                    )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITH_INFO, 4, params);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITHOUT_INFO);
    }

    try {
      uint32_t tStartRequest = time(NULL); // CASTOR2/rtcopy/rtcpclientd.c:1494
      RtcpTapeRequestMessage request;
      RtcpTapeRequestMessage reply;

      Utils::setBytes(request, '\0');
      Utils::copyString(request.vid    , "I10547");
      Utils::copyString(request.vsn    , "I10547");
      Utils::copyString(request.label  , "aul"   );
      Utils::copyString(request.density, "700GC" );
      request.volReqId       = vdqmJobRequest.tapeRequestId;
      request.tStartRequest  = tStartRequest;
      request.err.severity   = 1;
      request.err.maxTpRetry = -1;
      request.err.maxCpRetry = -1;

      Transceiver::giveVolumeIdToRtcpd(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
        request, reply);

      {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
          castor::dlf::Param("vid"     , request.vid                 ),
          castor::dlf::Param("vsn"     , request.vsn                 ),
          castor::dlf::Param("label"   , request.label               ),
          castor::dlf::Param("devtype" , request.devtype             ),
          castor::dlf::Param("density" , request.density             )};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_GAVE_VOLUME_INFO, 6, params);
      }

      Transceiver::giveRequestForMoreWorkToRtcpd(rtcpdInitialSocketFd,
        RTCPDNETRWTIMEOUT, vdqmJobRequest.tapeRequestId);

      {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", request.volReqId)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_GAVE_REQUEST_FOR_MORE_WORK, 1, params);
      }

    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
        castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
        castor::dlf::Param("Message" , ex.getMessage().str()       ),
        castor::dlf::Param("Code"    , ex.code()                   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_GIVE_VOLUME_INFO, 4, params);
    }

    try {
      processRtcpdSockets(cuuid, vdqmJobRequest, rtcpdCallbackSocketFd,
        rtcpdInitialSocketFd);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
        castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
        castor::dlf::Param("Message" , ex.getMessage().str()       ),
        castor::dlf::Param("Code"    , ex.code()                   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_PROCESS_RTCPD_CONNECTIONS_FAILED, 4, params);
    }

    close(rtcpdInitialSocketFd);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, 4, params);
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Give a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::AbstractTCPSocket *vdqmSocket =
    (castor::io::AbstractTCPSocket*)param;

  int rtcpdCallbackSocketFd = 0;
  bool processedJobSubmissionRequest = false;
  RcpJobRequestMessage vdqmJobRequest;

  try {
    // Create, bind and mark a listener socket for RTCPD callback connections
    rtcpdCallbackSocketFd = Net::createListenerSocket(0);

    try  {
      processJobSubmissionRequest(cuuid, vdqmSocket->socket(), vdqmJobRequest,
        rtcpdCallbackSocketFd);
      processedJobSubmissionRequest = true;
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_PROCESS_RCP_JOB_SUBMISSION, 3, params);
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_CREATE_RTCPD_CALLBACK_SOCKET, 3, params);
  }

  // Close and de-allocate the VDQM socket no matter if the processing of the
  // job submission request succeeded or not
  delete(vdqmSocket);

  // Close the RTCPD callback listener socket and return if the job submisison
  // request was not processed
  if(!processedJobSubmissionRequest) {
    close(rtcpdCallbackSocketFd);
    return;
  }

  // CoordinateRemoteCopy
  try {
    coordinateRemoteCopy(cuuid, vdqmJobRequest, rtcpdCallbackSocketFd);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_COORDINATE_REMOTE_COPY, 4, params);
  }

  // Close RTCPD callback listener socket
  close(rtcpdCallbackSocketFd);
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// checkRcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  checkRcpJobSubmitterIsAuthorised(const int socketFd)
  throw(castor::exception::Exception) {

  char peerHost[CA_MAXHOSTNAMELEN+1];

  // isadminhost fills in peerHost
  const int rc = isadminhost(socketFd, peerHost);

  if(rc == -1 && serrno != SENOTADMIN) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to lookup connection"
      << ": Peer Host: " << peerHost;

    throw ex;
  }

  if(*peerHost == '\0' ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Peer host name is an empty string";

    throw ex;
  }

  if(rc != 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << "Unauthorized host"
      << ": Peer Host: " << peerHost;

    throw ex;
  }
}
