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
void castor::tape::aggregator::VdqmRequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// processJobSubmissionRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  processJobSubmissionRequest(const Cuuid_t &cuuid, const int vdqmSocketFd,
  RcpJobRequestMessage &jobRequest, int &rtcpdCallbackSocketFd)
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

  Transceiver::receiveRcpJobRequest(vdqmSocketFd, NETRWTIMEOUT, jobRequest);
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID"  , jobRequest.tapeRequestID  ),
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

  // Create, bind and mark a listener socket for RTCPD callback connections
  rtcpdCallbackSocketFd = Net::createListenerSocket(0);

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
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_SUBMITTING_JOB_TO_RTCPD);
  RcpJobReplyMessage rtcpdReply;
  RcpJobSubmitter::submit(
    "localhost",               // host
    RTCOPY_PORT,               // port
    NETRWTIMEOUT,              // netReadWriteTimeout
    "RTCPD",                   // remoteCopyType
    jobRequest.tapeRequestID,
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
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , rtcpdReply.errorMessage),
      castor::dlf::Param("Code"    , rtcpdReply.status)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, 3, params);

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

  Net::writeBytes(vdqmSocketFd, NETRWTIMEOUT, vdqmReplyLen, vdqmReplyBuf);
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

  // Process the job submission request message from the VDQM using the
  // processJobSubmissionRequest function.
  //
  // If successfull then the function will have passed the request onto RTCPD
  // and will have given as output the contents of the job submission request
  // and the file descriptor of the listener socket which will be used to
  // accept callback connections from RTCPD.
  RcpJobRequestMessage vdqmJobRequest;
  int  rtcpdCallbackSocketFd         = 0;
  bool processedJobSubmissionRequest = false;
  try {
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

  // Close and de-allocate the VDQM socket no matter if the processing of the
  // job submission request succeeded or not
  delete(vdqmSocket);

  // Return if the processing of the job submission request failed
  if(!processedJobSubmissionRequest) {
    return;
  }

  // Accept the initial incoming RTCPD callback connection
  struct sockaddr_in acceptedAddress;
  unsigned int 	     len = sizeof(acceptedAddress);
  const int          rtcpdInitialSocketFd =
    accept(rtcpdCallbackSocketFd, (struct sockaddr *)  &acceptedAddress, &len);
  castor::io::AbstractTCPSocket rtcpdInitialSocket(rtcpdInitialSocketFd);

  if(rtcpdInitialSocketFd > 0) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , "Accepted initial RTCPD connection")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, 2, params);
  }

  // Give the volume ID from the VDQM job submission message to RTCPD
  {
    uint32_t tStartRequest = time(NULL); // CASTOR2/rtcopy/rtcpclientd.c:1494
    RtcpTapeRequestMessage request;
    RtcpTapeRequestMessage reply;

    Utils::setBytes(request, '\0');
    try {
      Utils::copyString(request.vid    , "I10547");
      Utils::copyString(request.vsn    , "I10547");
      Utils::copyString(request.label  , "aul"   );
      Utils::copyString(request.density, "700GC" );
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_NULL, 3, params);
      return;
    }
    request.volReqId      = vdqmJobRequest.tapeRequestID;
    request.tStartRequest = tStartRequest;

    try {

      Transceiver::giveVolumeIdToRtcpd(rtcpdInitialSocketFd, NETRWTIMEOUT,
        request, reply);

      castor::dlf::Param params[] = {
        castor::dlf::Param("vid"            , request.vid    ),
        castor::dlf::Param("vsn"            , request.vsn    ),
        castor::dlf::Param("label"          , request.label  ),
        castor::dlf::Param("devtype"        , request.devtype),
        castor::dlf::Param("density"        , request.density)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GAVE_VOLUME_INFO, 26, params);
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_GIVE_VOLUME_INFO, 3, params);
    }
  }

  // Give file information to RTCPD
  {
    RtcpFileRequestMessage request;

    Utils::setBytes(request, '\0');
    try {
      Utils::copyString(request.filePath, "lxc2disk07:/dev/null");
      Utils::copyString(request.recfm, "F");
    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_NULL, 3, params);
      return;
    }
    request.volReqId       = vdqmJobRequest.tapeRequestID;
    request.jobId          = -1;
    request.stageSubReqId  = -1;
    request.umask          = 18;
    request.tapeFseq	   = 1;
    request.diskFseq   	   = 1;
    request.blockSize	   = -1;
    request.recordLength   = -1;
    request.retention	   = -1;
    request.defAlloc	   = -1;
    request.rtcpErrAction  = -1;
    request.tpErrAction	   = -1;
    request.convert        = -1;
    request.checkFid       = -1;
    request.concat         = 1;
    request.procStatus     = RTCP_WAITING;
    request.err.severity   = 1;
    request.err.maxTpRetry = -1;
    request.err.maxCpRetry = -1;

    RtcpFileRequestMessage reply;

    try {

      Transceiver::giveFileInfoToRtcpd(rtcpdInitialSocketFd, NETRWTIMEOUT,
        request, reply);

      castor::dlf::Param params[] = {
        castor::dlf::Param("filePath", reply.filePath),
        castor::dlf::Param("tapePath", reply.tapePath),
        castor::dlf::Param("recfm", reply.recfm),
        castor::dlf::Param("fid", reply.fid),
        castor::dlf::Param("ifce", reply.ifce),
        castor::dlf::Param("stageId", reply.stageId),
       	castor::dlf::Param("volReqId", reply.volReqId),
       	castor::dlf::Param("jobId", reply.jobId),
	castor::dlf::Param("stageSubReqId", reply.stageSubReqId),
	castor::dlf::Param("umask", reply.umask),
	castor::dlf::Param("positionMethod", reply.positionMethod),
	castor::dlf::Param("tapeFseq", reply.tapeFseq),
        castor::dlf::Param("diskFseq", reply.diskFseq),
        castor::dlf::Param("blockSize", reply.blockSize),
        castor::dlf::Param("recordLength", reply.recordLength),
        castor::dlf::Param("retention", reply.retention),
        castor::dlf::Param("defAlloc", reply.defAlloc),
        castor::dlf::Param("rtcpErrAction", reply.rtcpErrAction),
        castor::dlf::Param("tpErrAction", reply.tpErrAction),
        castor::dlf::Param("convert", reply.convert),
        castor::dlf::Param("checkFid", reply.checkFid),
        castor::dlf::Param("concat", reply.concat),
        castor::dlf::Param("procStatus", reply.procStatus),
        castor::dlf::Param("cprc", reply.cprc),
        castor::dlf::Param("tStartPosition", reply.tStartPosition),
        castor::dlf::Param("tEndPosition", reply.tEndPosition),
        castor::dlf::Param("tStartTransferDisk", reply.tStartTransferDisk),
        castor::dlf::Param("tEndTransferDisk", reply.tEndTransferDisk),
        castor::dlf::Param("tStartTransferTape", reply.tStartTransferTape),
        castor::dlf::Param("tEndTransferTape", reply.tEndTransferTape),
        castor::dlf::Param("blockId[0]", reply.blockId[0]),
        castor::dlf::Param("blockId[1]", reply.blockId[1]),
        castor::dlf::Param("blockId[2]", reply.blockId[2]),
        castor::dlf::Param("blockId[3]", reply.blockId[3]),
        castor::dlf::Param("offset", reply.offset),
        castor::dlf::Param("bytesIn", reply.bytesIn),
        castor::dlf::Param("bytesOut", reply.bytesOut),
        castor::dlf::Param("hostBytes", reply.hostBytes),
        castor::dlf::Param("nbRecs", reply.nbRecs),
        castor::dlf::Param("maxNbRec", reply.maxNbRec),
        castor::dlf::Param("maxSize", reply.maxSize),
        castor::dlf::Param("startSize", reply.startSize),
        castor::dlf::Param("segAttr.nameServerHostName",
          reply.segAttr.nameServerHostName),
        castor::dlf::Param("segAttr.segmCksumAlgorithm",
          reply.segAttr.segmCksumAlgorithm),
        castor::dlf::Param("segAttr.segmCksum", reply.segAttr.segmCksum),
        castor::dlf::Param("segAttr.castorFileId", reply.segAttr.castorFileId),
        castor::dlf::Param("stgReqId.time_low", reply.stgReqId.time_low),
        castor::dlf::Param("stgReqId.time_mid", reply.stgReqId.time_mid),
        castor::dlf::Param("stgReqId.time_hi_and_version",
          reply.stgReqId.time_hi_and_version),
        castor::dlf::Param("stgReqId.clock_seq_hi_and_reserved",
          reply.stgReqId.clock_seq_hi_and_reserved),
        castor::dlf::Param("stgReqId.clock_seq_low",
          reply.stgReqId.clock_seq_low),
        castor::dlf::Param("stgReqId.node[0]", reply.stgReqId.node[0]),
        castor::dlf::Param("stgReqId.node[1]", reply.stgReqId.node[1]),
        castor::dlf::Param("stgReqId.node[2]", reply.stgReqId.node[2]),
        castor::dlf::Param("stgReqId.node[3]", reply.stgReqId.node[3]),
        castor::dlf::Param("stgReqId.node[4]", reply.stgReqId.node[4]),
        castor::dlf::Param("stgReqId.node[5]", reply.stgReqId.node[5]),
        castor::dlf::Param("err.errmsgtxt", reply.err.errmsgtxt),
        castor::dlf::Param("err.severity", reply.err.severity),
        castor::dlf::Param("err.errorcode", reply.err.errorcode),
        castor::dlf::Param("err.maxTpRetry", reply.err.maxTpRetry),
        castor::dlf::Param("err.maxCpRetry", reply.err.maxCpRetry)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GAVE_FILE_INFO, 62, params);

    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_GIVE_FILE_INFO, 3, params);
    }
  }

  // Signal the end of the file list to RTCPD
  try {
    Transceiver::signalNoMoreRequestsToRtcpd(rtcpdInitialSocketFd,
      NETRWTIMEOUT);
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SIGNALLED_NO_MORE_REQUESTS);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SIGNAL_NO_MORE_REQUESTS, 3, params);
  }

//==========================================

  {
    struct sockaddr_in acceptedAddress;
    unsigned int       len = sizeof(acceptedAddress);
    int                rtcpdInitialSocketFd;

    for(int i=1;;i++) {
      rtcpdInitialSocketFd = accept(rtcpdCallbackSocketFd, (struct sockaddr *)
        &acceptedAddress, &len);

      castor::dlf::Param params[] = {
        castor::dlf::Param("AcceptedConnection", rtcpdInitialSocketFd  ),
        castor::dlf::Param("LoopNb"          , i                     )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NULL, 2, params);
    }
  }

  // Select loop
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
