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
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMsg.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
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

  Transceiver::receiveRcpJobRqst(vdqmSocketFd, RTCPDNETRWTIMEOUT,
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
  const RcpJobRqstMsgBody &vdqmJobRequest, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_DATA_ON_INITIAL_RTCPD_CONNECTION, params);
  }

  MessageHeader          header;
  RtcpTapeRqstErrMsgBody body;

  Transceiver::receiveRtcpMsgHeader(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
    header);
  Transceiver::receiveRtcpTapeRqstErrBody(rtcpdInitialSocketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  Transceiver::sendRtcpAcknowledge(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT,
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
  acceptRtcpdConnection(const Cuuid_t &cuuid,
  const RcpJobRqstMsgBody &vdqmJobRequest, const int rtcpdCallbackSocketFd,
  std::list<int> &connectedSocketFds) throw(castor::exception::Exception) {

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
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, params);
  }

  connectedSocketFds.push_back(connectedSocketFd);
}


//-----------------------------------------------------------------------------
// processTapeDiskIoConnection
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::VdqmRequestHandlerThread::
  processTapeDiskIoConnection(const Cuuid_t &cuuid,
  const RcpJobRqstMsgBody &vdqmJobRequest, const int socketFd)
  throw(castor::exception::Exception) {

  bool continueMainSelectLoop = true;
  bool firstRequestForMoreWork = true;
  MessageHeader header;

  Transceiver::receiveRtcpMsgHeader(socketFd, RTCPDNETRWTIMEOUT, header);

  switch(header.reqType) {
  case RTCP_FILE_REQ:
    {
      RtcpFileRqstMsgBody body;

      Transceiver::receiveRtcpFileRqstBody(socketFd, RTCPDNETRWTIMEOUT,
        header, body);

      switch(body.procStatus) {
      case RTCP_REQUEST_MORE_WORK:

        // THE FOLLOWING CODE IS A HACK TO TEST THE CONNECTION BETWEEN THE
        // TAPE AGGREGATOR AND THE TAPE GATEWAY.  THIS CODE WILL BE REPLACED
        // SOON.

        if(firstRequestForMoreWork) {
          firstRequestForMoreWork = false;

          // Start a worker in the tape gateway
          const char *unit = "PIPPO"; // For debugging only
          int startTransferErrorCode = 0;
          std::string startTransferErrorMsg;
          std::string vid;
          uint32_t mode = 0;
          try {
            Transceiver::tellGatewayToStartTransfer(vdqmJobRequest.clientHost,
              vdqmJobRequest.clientPort, vdqmJobRequest.tapeRequestId,
              unit, vid, mode, startTransferErrorCode, startTransferErrorMsg);

            castor::dlf::Param params[] = {
              castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
            castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
              AGGREGATOR_TOLD_GATEWAY_TO_START_TRANSFER, params);
          } catch(castor::exception::Exception &ex) {
            castor::dlf::Param params[] = {
              castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
            CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_SYSTEM,
              AGGRRGATOR_FAILED_TO_TELL_GATEWAY_TO_START_TRANSFER, params);
          }

          if(startTransferErrorCode != 0) {
            castor::dlf::Param params[] = {
              castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
              castor::dlf::Param("Message" , startTransferErrorMsg),
              castor::dlf::Param("Code"    , startTransferErrorCode)};
            CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_SYSTEM,
              AGGREGATOR_RECEIVED_START_TRANSFER_ERROR, params);
          }
        } // if(firstRequestForMoreWork)

        {
          // Give file information to RTCPD
          try {
            Transceiver::giveFileListToRtcpd(socketFd, RTCPDNETRWTIMEOUT,
              vdqmJobRequest.tapeRequestId,
              "lxc2disk07:/tmp/murrayc3/test_04_02_09", body.tapePath, 18,
              false);
            castor::dlf::Param params[] = {
              castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
              castor::dlf::Param("filePath","lxc2disk07:/dev/null"),
              castor::dlf::Param("tapePath", body.tapePath)};
            castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
              AGGREGATOR_GAVE_FILE_INFO, params);
          } catch(castor::exception::Exception &ex) {
            castor::dlf::Param params[] = {
              castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
              castor::dlf::Param("filePath","lxc2disk07:/dev/null"),
              castor::dlf::Param("tapePath", body.tapePath),
              castor::dlf::Param("Message" , ex.getMessage().str()),
              castor::dlf::Param("Code"    , ex.code())};
            CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
              AGGREGATOR_FAILED_TO_GIVE_FILE_INFO, params);
          }

          // Acknowledge request for more work from RTCPD
          RtcpAcknowledgeMsg ackMsg;
          ackMsg.magic   = RTCOPY_MAGIC;
          ackMsg.reqType = RTCP_FILE_REQ;
          ackMsg.status  = 0;
          Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
        } 
        break;
      case RTCP_POSITIONED:
        {
          castor::dlf::Param params[] = {
            castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
            castor::dlf::Param("filePath", "lxc2disk07:/dev/null"),
            castor::dlf::Param("tapePath", body.tapePath)};
          castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
            AGGREGATOR_TAPE_POSITIONED_FILE_REQ, params);

          RtcpAcknowledgeMsg ackMsg;
          ackMsg.magic   = RTCOPY_MAGIC;
          ackMsg.reqType = RTCP_FILE_REQ;
          ackMsg.status  = 0;
          Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
        }
        break;
      case RTCP_FINISHED:
        {
          castor::dlf::Param params[] = {
            castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
            castor::dlf::Param("filePath", "lxc2disk07:/dev/null"),
            castor::dlf::Param("tapePath", body.tapePath)};
          castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
            AGGREGATOR_FILE_TRANSFERED, params);

          RtcpAcknowledgeMsg ackMsg;
          ackMsg.magic   = RTCOPY_MAGIC;
          ackMsg.reqType = RTCP_FILE_REQ;
          ackMsg.status  = 0;
          Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
        }
        break;
      default:  
        {
          castor::exception::Exception ex(EBADMSG);

          ex.getMessage() << __PRETTY_FUNCTION__
            << ": Received unexpected file request process status 0x"
            << std::hex << body.procStatus
            << "(" << Utils::procStatusToStr(body.procStatus) << ")";

          throw ex;
        }
      }
    }
    break;

  case RTCP_FILEERR_REQ:
    {
      RtcpFileRqstErrMsgBody body;

      Transceiver::receiveRtcpFileRqstErrBody(socketFd, RTCPDNETRWTIMEOUT,
        header, body);

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILEERR_REQ;
      ackMsg.status  = 0;
      Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

      castor::exception::Exception ex(body.err.errorCode);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Received an error from RTCPD:" << body.err.errorMsg;

      throw ex;
    }
    break;

  case RTCP_TAPE_REQ:
    {
      RtcpTapeRqstMsgBody body;

      Transceiver::receiveRtcpTapeRqstBody(socketFd, RTCPDNETRWTIMEOUT,
        header, body);

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
        AGGREGATOR_TAPE_POSITIONED_TAPE_REQ, params);

      // Acknowledge tape request
      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_TAPE_REQ;
      ackMsg.status  = 0;
      Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;

  case RTCP_TAPEERR_REQ:
    {
      RtcpTapeRqstErrMsgBody body;

      Transceiver::receiveRtcpTapeRqstErrBody(socketFd, RTCPDNETRWTIMEOUT,
        header, body);

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_TAPEERR_REQ;
      ackMsg.status  = 0;
      Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

      castor::exception::Exception ex(body.err.errorCode);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Received an error from RTCPD:" << body.err.errorMsg;

      throw ex;
    }
    break;

  case RTCP_ENDOF_REQ:
    {
      // An RTCP_ENDOF_REQ message is bodiless
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_RECEIVED_RTCP_ENDOF_REQ);

      // Acknowledge RTCP_ENDOF_REQ message
      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_ENDOF_REQ;
      ackMsg.status  = 0;
      Transceiver::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

      continueMainSelectLoop = false;
      break;
    }
  default:
    {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << "Unexpected request type: 0x" << std::hex
        << header.reqType
        << "(" << Utils::rtcopyReqTypeToStr(header.reqType) << ")";

      throw ex;
    }
  }

  return continueMainSelectLoop;
}


//-----------------------------------------------------------------------------
// processRtcpdSockets
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processRtcpdSockets(const Cuuid_t &cuuid,
  const RcpJobRqstMsgBody &vdqmJobRequest, const int rtcpdCallbackSocketFd,
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

        Transceiver::pingRtcpd(rtcpdInitialSocketFd, RTCPDNETRWTIMEOUT);
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

            processErrorOnInitialRtcpdConnection(cuuid, vdqmJobRequest,
              rtcpdInitialSocketFd);

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else if there is a callback connection request from RTCPD
          } else if(FD_ISSET(rtcpdCallbackSocketFd, &readFdSet)) {

            acceptRtcpdConnection(cuuid, vdqmJobRequest, rtcpdCallbackSocketFd,
              connectedSocketFds);

            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

          // Else there are one or more messages from the tape/disk I/O threads
          } else {  
            for(std::list<int>::iterator itor=connectedSocketFds.begin(); 
                itor != connectedSocketFds.end(); itor++) {

              if(FD_ISSET(*itor, &readFdSet)) {

                continueMainSelectLoop = processTapeDiskIoConnection(cuuid,
                  vdqmJobRequest, *itor);

                FD_CLR(*itor, &readFdSet);
              }
            }
          }
        } // For each bit that has been set
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
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
  const Cuuid_t &cuuid, const RcpJobRqstMsgBody &vdqmJobRequest,
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
        AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITH_INFO, params);
    } catch(castor::exception::Exception &ex) {
      CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITHOUT_INFO);
    }

    try {
      uint32_t tStartRequest = time(NULL); // CASTOR2/rtcopy/rtcpclientd.c:1494
      RtcpTapeRqstErrMsgBody request;
      RtcpTapeRqstErrMsgBody reply;

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
          AGGREGATOR_GAVE_VOLUME_INFO, params);
      }

      Transceiver::giveRequestForMoreWorkToRtcpd(rtcpdInitialSocketFd,
        RTCPDNETRWTIMEOUT, vdqmJobRequest.tapeRequestId);

      {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", request.volReqId)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_GAVE_REQUEST_FOR_MORE_WORK, params);
      }

    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
        castor::dlf::Param("Message" , ex.getMessage().str()       ),
        castor::dlf::Param("Code"    , ex.code()                   )};
      CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_GIVE_VOLUME_INFO, params);
    }

    try {
      processRtcpdSockets(cuuid, vdqmJobRequest, rtcpdCallbackSocketFd,
        rtcpdInitialSocketFd);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
        castor::dlf::Param("Message" , ex.getMessage().str()       ),
        castor::dlf::Param("Code"    , ex.code()                   )};
      CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_PROCESS_RTCPD_CONNECTIONS_FAILED, params);
    }

    close(rtcpdInitialSocketFd);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", vdqmJobRequest.tapeRequestId),
      castor::dlf::Param("Function", __PRETTY_FUNCTION__         ),
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, params);
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
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::AbstractTCPSocket *vdqmSocket =
    (castor::io::AbstractTCPSocket*)param;

  int rtcpdCallbackSocketFd = 0;
  bool processedJobSubmissionRequest = false;
  RcpJobRqstMsgBody vdqmJobRequest;

  try {
    // Create, bind and mark a listener socket for RTCPD callback connections
    rtcpdCallbackSocketFd = Net::createListenerSocket(0);

    try  {
      processJobSubmissionRequest(cuuid, vdqmSocket->socket(), vdqmJobRequest,
        rtcpdCallbackSocketFd);
      processedJobSubmissionRequest = true;
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_PROCESS_RCP_JOB_SUBMISSION, params);
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_CREATE_RTCPD_CALLBACK_SOCKET, params);
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
      castor::dlf::Param("Message" , ex.getMessage().str()       ),
      castor::dlf::Param("Code"    , ex.code()                   )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_COORDINATE_REMOTE_COPY, params);
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
