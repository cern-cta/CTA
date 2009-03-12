/******************************************************************************
 *                castor/tape/aggregator/VdqmRequestHandler.cpp
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

#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMsg.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/SmartFd.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/aggregator/VdqmRequestHandler.hpp"
#include "castor/tape/tapegateway/ErrorReport.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <memory>
#include <sys/select.h>


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandler::~VdqmRequestHandler()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::init() throw() {
}


//-----------------------------------------------------------------------------
// processJobSubmissionRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::
  processJobSubmissionRequest(const Cuuid_t &cuuid, const int vdqmsocketFd,
  RcpJobRqstMsgBody &jobRequest, const int rtcpdCallbackSocketFd)
  throw(castor::exception::Exception) {

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(vdqmsocketFd, ip, port);
    Net::getPeerHostName(vdqmsocketFd, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, params);
  }

  Utils::setBytes(jobRequest, '\0');

  checkRcpJobSubmitterIsAuthorised(vdqmsocketFd);

  RtcpTxRx::receiveRcpJobRqst(vdqmsocketFd, RTCPDNETRWTIMEOUT,
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
      AGGREGATOR_HANDLE_JOB_MESSAGE, params);
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
      AGGREGATOR_SUBMITTING_JOB_TO_RTCPD, params);
  }
  RcpJobReplyMsgBody rtcpdReply;
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
      castor::dlf::Param("Message" , rtcpdReply.errorMessage ),
      castor::dlf::Param("Code"    , rtcpdReply.status       )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, params);

    // Override positive response with the error message from RTCPD
    errorStatusForVdqm  = rtcpdReply.status;
    errorMessageForVdqm = rtcpdReply.errorMessage;
  }

  // Acknowledge the VDQM - maybe positive or negative depending on reply
  // from RTCPD
  char vdqmReplyBuf[MSGBUFSIZ];
  size_t vdqmReplyLen = 0;

  vdqmReplyLen = Marshaller::marshallRcpJobReplyMsgBody(vdqmReplyBuf,
    rtcpdReply);

  Net::writeBytes(vdqmsocketFd, RTCPDNETRWTIMEOUT, vdqmReplyLen, vdqmReplyBuf);
}


//-----------------------------------------------------------------------------
// processErrorOnInitialRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::
  processErrorOnInitialRtcpdConnection(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int socketFd)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_DATA_ON_INITIAL_RTCPD_CONNECTION, params);
  }

  MessageHeader          header;
  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpMsgHeader(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header);
  RtcpTxRx::receiveRtcpTapeRqstErrBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  TAPE_THROW_CODE(body.err.errorCode,
    ": Received an error from RTCPD:" << body.err.errorMsg);
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
int castor::tape::aggregator::VdqmRequestHandler::
  acceptRtcpdConnection(const Cuuid_t &cuuid, const uint32_t volReqId,
  const int rtcpdCallbackSocketFd) throw(castor::exception::Exception) {

  SmartFd connectedsocketFd(Net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDCALLBACKTIMEOUT));

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(connectedsocketFd.get(), ip, port);
    Net::getPeerHostName(connectedsocketFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId                  ),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port                      ),
      castor::dlf::Param("HostName", hostName                  ),
      castor::dlf::Param("socketFd", connectedsocketFd.get()   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, params);
  }

  return connectedsocketFd.release();
}


//-----------------------------------------------------------------------------
// processRtcpdSockets
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::
  processRtcpdSockets(const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const int rtcpdCallbackSocketFd, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) {

  SmartFdList connectedsocketFds;
  int selectRc = 0;
  int selectErrno = 0;
  fd_set readFdSet;
  int maxFd = 0;
  timeval timeout;

  try {
    // Select loop
    bool continueMainSelectLoop = true;
    while(continueMainSelectLoop) {

      // Build the file descriptor set ready for the select call
      FD_ZERO(&readFdSet);
      FD_SET(rtcpdCallbackSocketFd, &readFdSet);
      FD_SET(rtcpdInitialSocketFd, &readFdSet);
      if(rtcpdCallbackSocketFd > rtcpdInitialSocketFd) {
        maxFd = rtcpdCallbackSocketFd;
      } else {
        maxFd = rtcpdInitialSocketFd;
      }
      for(std::list<int>::iterator itor = connectedsocketFds.begin();
        itor != connectedsocketFds.end(); itor++) {

        FD_SET(*itor, &readFdSet);

        if(*itor > maxFd) {
          maxFd = *itor;
        }
      }

      timeout.tv_sec  = RTCPDPINGTIMEOUT;
      timeout.tv_usec = 0;

      selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
      selectErrno = errno;

      switch(selectRc) {
      case 0: // Select timed out

        RtcpTxRx::pingRtcpd(cuuid, volReqId, rtcpdInitialSocketFd,
          RTCPDNETRWTIMEOUT);
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, AGGREGATOR_PINGED_RTCPD);
        break;

      case -1: // Select encountered an error

        // If select encountered an error other than an interruption
        if(selectErrno != EINTR) {
          char strerrorBuf[STRERRORBUFLEN];
          char *const errorStr = strerror_r(selectErrno, strerrorBuf,
            sizeof(strerrorBuf));

          TAPE_THROW_CODE(selectErrno,
            ": Select encountered an error other than an interruption"
            ": " << errorStr);
        }
        break;

      default: // One or more select file descriptors require attention

        // For each bit that has been set
        for(int i=0; i<selectRc; i++) {
          // If there is an incoming message on the initial RTCPD connection
          if(FD_ISSET(rtcpdInitialSocketFd, &readFdSet)) {

            processErrorOnInitialRtcpdConnection(cuuid, volReqId,
              rtcpdInitialSocketFd);

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else if there is a callback connection request from RTCPD
          } else if(FD_ISSET(rtcpdCallbackSocketFd, &readFdSet)) {

            connectedsocketFds.push_back(acceptRtcpdConnection(cuuid,
              volReqId, rtcpdCallbackSocketFd));

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else there are one or more messages from the tape/disk I/O threads
          } else {  
            for(std::list<int>::iterator itor=connectedsocketFds.begin(); 
                itor != connectedsocketFds.end(); itor++) {

              if(FD_ISSET(*itor, &readFdSet)) {

                continueMainSelectLoop = m_tapeDiskRqstHandler.processRequest(
                  cuuid, volReqId, gatewayHost, gatewayPort, mode, *itor);

                FD_CLR(*itor, &readFdSet);
              }
            }
          }
        } // For each bit that has been set
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId             ),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR, AGGREGATOR_MAIN_SELECT_FAILED,
      params);
  }
}


//-----------------------------------------------------------------------------
// coordinateRemoteCopy
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::coordinateRemoteCopy(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const int rtcpdCallbackSocketFd)
  throw(castor::exception::Exception) {

  // Accept the initial incoming RTCPD callback connection.
  // Wrap the socket file descriptor in a smart file descriptor so that it is
  // guaranteed to be closed when it goes out of scope.
  SmartFd rtcpdInitialSocketFd(Net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDCALLBACKTIMEOUT));

  // Log the connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(rtcpdInitialSocketFd.get(), ip, port);
    Net::getPeerHostName(rtcpdInitialSocketFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)  ),
      castor::dlf::Param("Port"    , port                        ),
      castor::dlf::Param("HostName", hostName                    ),
      castor::dlf::Param("socketFd", rtcpdInitialSocketFd.get()  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITHOUT_INFO);
  }

  // Get the request informatiom from RTCPD
  // The volume request ID is already known, but getting the drive unit is also
  // returned which is good for logging
  RtcpTapeRqstErrMsgBody rtcpdRequestInfoReply;
  RtcpTxRx::getRequestInfoFromRtcpd(cuuid, volReqId, rtcpdInitialSocketFd.get(),
    RTCPDNETRWTIMEOUT, rtcpdRequestInfoReply);

  // If the VDQM and RTCPD volume request IDs do not match
  if(rtcpdRequestInfoReply.volReqId != volReqId) {

    TAPE_THROW_CODE(EBADMSG,
         ": VDQM and RTCPD volume request Ids do not match"
         ": VDQM volume request ID: " << volReqId 
      << ": RTCPD volume request ID: " << rtcpdRequestInfoReply.volReqId);
  }

  // Get the volume from the tape gateway
  RtcpTapeRqstErrMsgBody rtcpVolume;
  Utils::setBytes(rtcpVolume, '\0');
  const bool thereIsAVolume = GatewayTxRx::getVolumeFromGateway(cuuid,
    volReqId, gatewayHost, gatewayPort, rtcpVolume.vid, rtcpVolume.mode,
    rtcpVolume.label, rtcpVolume.density);

  // If there is a volume
  if(thereIsAVolume) {

    // Complete the RTCPD volume message by setting the volume request ID
    rtcpVolume.volReqId = volReqId;

  // Else there is no volume
  } else {
    return;
  }

  RtcpFileRqstErrMsgBody rtcpFileReply;
  Utils::setBytes(rtcpFileReply, '\0');

  // If migrating
  if(rtcpVolume.mode == WRITE_ENABLE) {

    char     filePath[CA_MAXPATHLEN+1];
    char     nsHost[CA_MAXHOSTNAMELEN+1];
    uint64_t fileId;
    uint32_t tapeFseq;
    uint64_t fileSize;
    char     lastKnownFileName[CA_MAXPATHLEN+1];
    uint64_t lastModificationTime;

    // Get first file to migrate from the tape gateway
    const bool thereIsAFileToMigrate =
      GatewayTxRx::getFileToMigrateFromGateway(cuuid, volReqId, gatewayHost,
        gatewayPort, filePath, nsHost, fileId, tapeFseq, fileSize,
        lastKnownFileName, lastModificationTime);

    // Return if there is no file to migrate
    if(!thereIsAFileToMigrate) {
      return;
    } 

    // Give volume to RTCPD
    rtcpVolume.err.maxTpRetry = -1;
    rtcpVolume.err.maxCpRetry = -1;
    RtcpTxRx::giveVolumeToRtcpd(cuuid, volReqId, rtcpdInitialSocketFd.get(),
      RTCPDNETRWTIMEOUT, rtcpVolume); 
 
    // Give file to migrate to RTCPD
    char tapeFileId[CA_MAXPATHLEN+1];
    Utils::toHex(fileId, tapeFileId);
    RtcpTxRx::giveFileToRtcpd(cuuid, volReqId, rtcpdInitialSocketFd.get(),
      RTCPDNETRWTIMEOUT, WRITE_ENABLE, filePath, "", RECORDFORMAT, tapeFileId,
      MIGRATEUMASK);

    // Ask RTCPD to request more work
    RtcpTxRx::askRtcpdToRequestMoreWork(cuuid, volReqId,
      rtcpdInitialSocketFd.get(), RTCPDNETRWTIMEOUT, WRITE_ENABLE);

    // Tell RTCPD end of file list
    RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId,
      rtcpdInitialSocketFd.get(), RTCPDNETRWTIMEOUT);

  // Else recalling
  } else {
    // Give volume to RTCPD
    rtcpVolume.err.maxTpRetry = -1;
    rtcpVolume.err.maxCpRetry = -1;
    RtcpTxRx::giveVolumeToRtcpd(cuuid, volReqId, rtcpdInitialSocketFd.get(),
      RTCPDNETRWTIMEOUT, rtcpVolume);

    // Ask RTCPD to request more work
    RtcpTxRx::askRtcpdToRequestMoreWork(cuuid, volReqId,
      rtcpdInitialSocketFd.get(), RTCPDNETRWTIMEOUT, WRITE_DISABLE);

    // Tell RTCPD end of file list
    RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId,
      rtcpdInitialSocketFd.get(), RTCPDNETRWTIMEOUT);
  }

  processRtcpdSockets(cuuid, volReqId, gatewayHost, gatewayPort,
    rtcpVolume.mode, rtcpdCallbackSocketFd, rtcpdInitialSocketFd.get());
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Give a Cuuid to the request
  Cuuid_create(&cuuid);

  // Check the parameter to the run function has been set
  if(param == NULL) {
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  // Wrap the VDQM connection socket within an auto pointer.  When the auto
  // pointer goes out of scope it will delete the socket.  The destructor of
  // the socket will in turn close the connection.
  std::auto_ptr<castor::io::AbstractTCPSocket>
    vdqmSocket((castor::io::AbstractTCPSocket*)param);

  try {
    // Create, bind and mark a listen socket for RTCPD callback connections
    // Wrap the socket file descriptor in a smart file descriptor so that it is
    // guaranteed to be closed when it goes out of scope.
    SmartFd rtcpdCallbackSocketFd(Net::createListenerSocket(0));

    RcpJobRqstMsgBody vdqmJobRequest;

    processJobSubmissionRequest(cuuid, vdqmSocket->socket(), vdqmJobRequest,
      rtcpdCallbackSocketFd.get());

    // Close the connection to the VDQM
    //
    // The VDQM connection socket needs to be released from it's auto pointer
    // so that the auto pointer doesn't erroneously try to delete the socket
    // and close the connection a second time
    delete(vdqmSocket.release());

    coordinateRemoteCopy(cuuid, vdqmJobRequest.tapeRequestId,
      vdqmJobRequest.clientHost, vdqmJobRequest.clientPort,
      rtcpdCallbackSocketFd.get());

  } catch(castor::exception::Exception &ex) {

    char codeStr[1024];
    sstrerror_r(ex.code(), codeStr, sizeof(codeStr));

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message"   , ex.getMessage().str()),
      castor::dlf::Param("Code"      , ex.code()),
      castor::dlf::Param("CodeString", codeStr)};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_TRANSFER_FAILED, params);
  }
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// checkRcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::
  checkRcpJobSubmitterIsAuthorised(const int socketFd)
  throw(castor::exception::Exception) {

  char peerHost[CA_MAXHOSTNAMELEN+1];

  // isadminhost fills in peerHost
  const int rc = isadminhost(socketFd, peerHost);

  if(rc == -1 && serrno != SENOTADMIN) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to lookup connection"
      << ": Peer Host: " << peerHost);
  }

  if(*peerHost == '\0' ) {
    TAPE_THROW_CODE(EINVAL,
      ": Peer host name is an empty string");
  }

  if(rc != 0) {
    TAPE_THROW_CODE(SENOTADMIN,
         ": Unauthorized host"
      << ": Peer Host: " << peerHost);
  }
}
