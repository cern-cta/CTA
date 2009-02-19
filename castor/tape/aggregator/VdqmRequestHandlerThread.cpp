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
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/aggregator/VdqmRequestHandlerThread.hpp"
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
castor::tape::aggregator::VdqmRequestHandlerThread::~VdqmRequestHandlerThread()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::init() throw() {
}


//-----------------------------------------------------------------------------
// processJobSubmissionRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processJobSubmissionRequest(const Cuuid_t &cuuid, const int vdqmSocketFd,
  RcpJobRqstMsgBody &jobRequest, const int rtcpdCallbackSocketFd)
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
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, params);
  }

  Utils::setBytes(jobRequest, '\0');

  checkRcpJobSubmitterIsAuthorised(vdqmSocketFd);

  RtcpTxRx::receiveRcpJobRqst(vdqmSocketFd, RTCPDNETRWTIMEOUT,
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

  Net::writeBytes(vdqmSocketFd, RTCPDNETRWTIMEOUT, vdqmReplyLen, vdqmReplyBuf);
}


//-----------------------------------------------------------------------------
// processErrorOnInitialRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processErrorOnInitialRtcpdConnection(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_DATA_ON_INITIAL_RTCPD_CONNECTION, params);
  }

  MessageHeader          header;
  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpMsgHeader(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
    header);
  RtcpTxRx::receiveRtcpTapeRqstErrBody(rtcpdInitialSocketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  castor::exception::Exception ex(body.err.errorCode);

  ex.getMessage() << __PRETTY_FUNCTION__
    << ": Received an error from RTCPD:" << body.err.errorMsg;

  throw ex;
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  acceptRtcpdConnection(const Cuuid_t &cuuid, const uint32_t volReqId,
  const int rtcpdCallbackSocketFd, std::list<int> &connectedSocketFds)
  throw(castor::exception::Exception) {

  const int connectedSocketFd = Net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDCALLBACKTIMEOUT);

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(connectedSocketFd, ip, port);
    Net::getPeerHostName(connectedSocketFd, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, params);
  }

  connectedSocketFds.push_back(connectedSocketFd);
}


//-----------------------------------------------------------------------------
// processRtcpdSockets
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processRtcpdSockets(const Cuuid_t &cuuid, const uint32_t volReqId,
  const uint32_t mode, const int rtcpdCallbackSocketFd,
  const int rtcpdInitialSocketFd) throw(castor::exception::Exception) {

  std::list<int> connectedSocketFds;
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
      for(std::list<int>::iterator itor = connectedSocketFds.begin();
        itor != connectedSocketFds.end(); itor++) {

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

        RtcpTxRx::pingRtcpd(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT);
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, AGGREGATOR_PINGED_RTCPD);
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

        // For each bit that has been set
        for(int i=0; i<selectRc; i++) {
          // If there is an incoming message on the initial RTCPD connection
          if(FD_ISSET(rtcpdInitialSocketFd, &readFdSet)) {

            processErrorOnInitialRtcpdConnection(cuuid, volReqId,
              rtcpdInitialSocketFd);

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else if there is a callback connection request from RTCPD
          } else if(FD_ISSET(rtcpdCallbackSocketFd, &readFdSet)) {

            acceptRtcpdConnection(cuuid, volReqId, rtcpdCallbackSocketFd,
              connectedSocketFds);

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else there are one or more messages from the tape/disk I/O threads
          } else {  
            for(std::list<int>::iterator itor=connectedSocketFds.begin(); 
                itor != connectedSocketFds.end(); itor++) {

              if(FD_ISSET(*itor, &readFdSet)) {

                continueMainSelectLoop = m_tapeDiskRqstHandler.processRequest(
                  cuuid, volReqId, mode, *itor);

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

  // Close all connected sockets
  for(
    std::list<int>::iterator itor = connectedSocketFds.begin();
    itor != connectedSocketFds.end();
    itor++) {
    close(*itor);
  }
}


//-----------------------------------------------------------------------------
// coordinateRemoteCopy
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::coordinateRemoteCopy(
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
      castor::dlf::Param("HostName", hostName                    )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITHOUT_INFO);
  }

  // Get request informatiom from RTCPD
  RtcpTapeRqstErrMsgBody rtcpdRequestInfoReply;
  RtcpTxRx::getRequestInfoFromRtcpd(rtcpdInitialSocketFd.get(),
    RTCPDNETRWTIMEOUT, rtcpdRequestInfoReply);
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", rtcpdRequestInfoReply.volReqId),
      castor::dlf::Param("unit"    , rtcpdRequestInfoReply.unit)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_REQUEST_INFO_FROM_RTCPD, params);
  }

  // If the VDQM and RTCPD volume request IDs do not match
  if(rtcpdRequestInfoReply.volReqId != volReqId) {

    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": VDQM and RTCPD volume request Ids do not match"
         ": VDQM volume request ID: " << volReqId 
      << ": RTCPD volume request ID: " << rtcpdRequestInfoReply.volReqId;

    throw ex;
  }
 
  RtcpTapeRqstErrMsgBody rtcpVolume;
  Utils::setBytes(rtcpVolume, '\0');

  // If there is a volume
  if(GatewayTxRx::getVolumeFromGateway(gatewayHost, gatewayPort, volReqId, 
     rtcpVolume.vid, rtcpVolume.mode, rtcpVolume.label,
     rtcpVolume.density)) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId            ),
      castor::dlf::Param("gatewayHost", gatewayHost         ),
      castor::dlf::Param("gatewayPort", gatewayPort         ),
      castor::dlf::Param("vid"        , rtcpVolume.vid  ),
      castor::dlf::Param("mode"       , rtcpVolume.mode ),
      castor::dlf::Param("label"      , rtcpVolume.label),
      castor::dlf::Param("density"    , rtcpVolume.density)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VOLUME, params);

  // Else there is no volume
  } else {

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NO_VOLUME, params);

    return;
  }

  RtcpFileRqstErrMsgBody rtcpFileReply;
  Utils::setBytes(rtcpFileReply, '\0');

  // If it is a Migration
  if(rtcpVolume.mode == WRITE_ENABLE) {

    char     filePath[CA_MAXPATHLEN+1];
    char     nsHost[CA_MAXHOSTNAMELEN+1];
    uint64_t fileId;
    uint32_t tapeFseq;
    uint64_t fileSize;
    char     lastKnownFileName[CA_MAXPATHLEN+1];
    uint64_t lastModificationTime;

    // If there is a file to migrate
    if(GatewayTxRx::getFileToMigrateFromGateway(gatewayHost, gatewayPort,
      volReqId, filePath, nsHost, fileId, tapeFseq, fileSize,
      lastKnownFileName, lastModificationTime)) {

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"            , volReqId            ),
        castor::dlf::Param("gatewayHost"         , gatewayHost         ),
        castor::dlf::Param("gatewayPort"         , gatewayPort         ),
        castor::dlf::Param("filePath"            , filePath            ),
        castor::dlf::Param("nsHost"              , nsHost              ),
        castor::dlf::Param("fileId"              , fileId              ),
        castor::dlf::Param("tapeFseq"            , tapeFseq            ),
        castor::dlf::Param("fileSize"            , fileSize            ),
        castor::dlf::Param("lastKnownFileName"   , lastKnownFileName   ),
        castor::dlf::Param("lastModificationTime", lastModificationTime)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FIRST_FILE_TO_MIGRATE, params);

    // Else there is no file to migrate
    } else {

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"   , volReqId   ),
        castor::dlf::Param("gatewayHost", gatewayHost),
        castor::dlf::Param("gatewayPort", gatewayPort)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_NO_FIRST_FILE_TO_MIGRATE, params); 

      return;
    } 

    // Send the volume to RTCPD
    RtcpTxRx::giveVolumeToRtcpd(rtcpdCallbackSocketFd, RTCPDNETRWTIMEOUT, 
      rtcpVolume); 
 
    // Send file to migrate  to RTCPD
    char tapePath[CA_MAXPATHLEN+1];
    Utils::toHex(fileId, tapePath);
    RtcpTxRx::giveFileToRtcpd(rtcpdCallbackSocketFd, RTCPDNETRWTIMEOUT, 
      volReqId, filePath, tapePath, 022);

    // Send joker More work to RTCPD
    RtcpTxRx::giveRequestForMoreWorkToRtcpd(rtcpdCallbackSocketFd,
      RTCPDNETRWTIMEOUT, volReqId);

    // Send EndOfFileList
    RtcpTxRx::tellRtcpdEndOfFileList(rtcpdCallbackSocketFd, RTCPDNETRWTIMEOUT);

  } else { // It is a Recall
    // Send the volume to RTCPD
    RtcpTxRx::giveVolumeToRtcpd(rtcpdCallbackSocketFd, RTCPDNETRWTIMEOUT, 
      rtcpVolume); 

    // Send joker More work to RTCPD
    RtcpTxRx::giveRequestForMoreWorkToRtcpd(rtcpdCallbackSocketFd,
      RTCPDNETRWTIMEOUT, volReqId);

    // Send EndOfFileList
    RtcpTxRx::tellRtcpdEndOfFileList(rtcpdCallbackSocketFd, RTCPDNETRWTIMEOUT);
  }

  // Process the RTCPD sockets
  processRtcpdSockets(cuuid, volReqId, rtcpVolume.mode,
    rtcpdCallbackSocketFd, rtcpdInitialSocketFd.get());
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::run(void *param)
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
