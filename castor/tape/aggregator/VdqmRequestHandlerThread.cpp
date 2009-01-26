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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMessage.hpp"
#include "castor/tape/aggregator/RtcpTapeRequestMessage.hpp"
#include "castor/tape/aggregator/RtcpFileRequestMessage.hpp"
#include "castor/tape/aggregator/SocketHelper.hpp"
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
  const int rtcpdListenPort) throw () :
  m_rtcpdListenPort(rtcpdListenPort), m_jobQueue(1) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandlerThread::~VdqmRequestHandlerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::init()
  throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Gives a Cuuid to the request
  Cuuid_create(&cuuid);

  if(param == NULL) {
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  castor::io::AbstractTCPSocket *vdqmSocket =
    (castor::io::AbstractTCPSocket*)param;

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP

    // Get client IP info
    vdqmSocket->getPeerIp(port, ip);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"  , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port", port)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, 2, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, 3, params);
  }

  RcpJobRequestMessage jobRequest;
  Utils::setBytes(jobRequest, 0);

  int            rtcpdListenSocket     = 0;
  unsigned long  rtcpdListenSocketIp   = 0;
  unsigned short rtcpdListenSocketPort = 0;

  try {
    checkRcpJobSubmitterIsAuthorised(*vdqmSocket);

    // Create, bind and mark as listener socket using any port number
    rtcpdListenSocket = Utils::createListenerSocket(0);

    Utils::getLocalIpAndPort(rtcpdListenSocket, rtcpdListenSocketIp,
      rtcpdListenSocketPort);

    Transceiver::receiveRcpJobRequest(cuuid, *vdqmSocket, NETRWTIMEOUT,
      jobRequest);
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

    // Pass a modified version of the job request through to RTCPD, setting the
    // clientHost and clientPort parameters to identify the tape aggregator as
    // being a proxy for RTCPClientD
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SUBMITTING_JOB_TO_RTCPD);
    RcpJobReplyMessage rtcpdReply;
    RcpJobSubmitter::submit(
      "localhost",            // host
      rtcpdListenSocketPort,  // port
      NETRWTIMEOUT,           // netReadWriteTimeout
      "RTCPD",                // remoteCopyType
      jobRequest.tapeRequestID,
      jobRequest.clientUserName,
      "localhost",            // clientHost
      m_rtcpdListenPort,      // clientPort
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
    if(strlen(rtcpdReply.errorMessage) > 1) {
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

    SocketHelper::writeBytes(*vdqmSocket, NETRWTIMEOUT, vdqmReplyLen,
      vdqmReplyBuf);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_PROCESS_RCP_JOB_SUBMISSION, 3, params);
  }

  // Close and de-allocate the VDQM socket
  vdqmSocket->close();
  delete(vdqmSocket);

  // Select loop
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::stop()
  throw() {
}


//-----------------------------------------------------------------------------
// handleJobSubmission
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::handleJobSubmission(
  Cuuid_t &cuuid, const MessageHeader &header, const char *bodyBuf,
  castor::io::ServerSocket &socket) throw() {

  // So that it all compiles
  RcpJobReplyMessage reply;

  // Prepare a positive response which will be overwritten if RTCPD replied to
  // the tape aggregator with an error message
  uint32_t    errorStatusForVdqm  = VDQM_CLIENTINFO; // Strange status code
  std::string errorMessageForVdqm = "";

  // If RTCOPY or tape aggregator daemon returned an error message
  // Checking the size of the error message because the status maybe non-zero
  // even if there is no error
  if(strlen(reply.errorMessage) > 1) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , reply.errorMessage),
      castor::dlf::Param("Code"    , reply.status)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, 3, params);

    // Override positive response with the error message from RTCPD
    errorStatusForVdqm  = reply.status;
    errorMessageForVdqm = reply.errorMessage;
  }

  // Acknowledge the VDQM - maybe positive or negative depending on reply from
  // RTCPD
  char replyBuf[MSGBUFSIZ];
  size_t replyLen = 0;

  try {
    replyLen = Marshaller::marshallRcpJobReplyMessage(replyBuf, reply);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_MARSHALL_RCP_JOB_REPLY_MESSAGE, 3, params);

    return;
  }

  try {
    SocketHelper::writeBytes(socket, NETRWTIMEOUT, replyLen, replyBuf);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_SEND_RCP_JOB_REPLY_TO_VDQM, 3, params);
  }
}


//-----------------------------------------------------------------------------
// recall
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::recall(Cuuid_t &cuuid,
  castor::io::ServerSocket &socket) throw() {

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP

    // Get client IP info
    socket.getPeerIp(port, ip);

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
      Transceiver::getVolumeRequestIdFromRtcpd(cuuid, socket, NETRWTIMEOUT,
        reply);
      volReqId = reply.volReqId;

      castor::dlf::Param params[] = {
        castor::dlf::Param("vid"            , reply.vid            ),
        castor::dlf::Param("vsn"            , reply.vsn            ),
        castor::dlf::Param("label"          , reply.label          ),
        castor::dlf::Param("devtype"        , reply.devtype        ),
        castor::dlf::Param("density"        , reply.density        ),
        castor::dlf::Param("unit"           , reply.unit           ),
        castor::dlf::Param("volReqId"       , reply.volReqId       ),
        castor::dlf::Param("jobId"          , reply.jobId          ),
        castor::dlf::Param("mode"           , reply.mode           ),
        castor::dlf::Param("start_file"     , reply.start_file     ),
        castor::dlf::Param("end_file"       , reply.end_file       ),
        castor::dlf::Param("side"           , reply.side           ),
        castor::dlf::Param("tprc"           , reply.tprc           ),
        castor::dlf::Param("tStartRequest"  , reply.tStartRequest  ),
        castor::dlf::Param("tEndRequest"    , reply.tEndRequest    ),
        castor::dlf::Param("tStartRtcpd"    , reply.tStartRtcpd    ),
        castor::dlf::Param("tStartMount"    , reply.tStartMount    ),
        castor::dlf::Param("tEndMount"      , reply.tEndMount      ),
        castor::dlf::Param("tStartUnmount"  , reply.tStartUnmount  ),
        castor::dlf::Param("tEndUnmount"    , reply.tEndUnmount    ),
        castor::dlf::Param("rtcpReqId"      , reply.rtcpReqId      ),
        castor::dlf::Param("err.errmsgtxt"  , reply.err.errmsgtxt  ),
        castor::dlf::Param("err.severity"   , reply.err.severity   ),
        castor::dlf::Param("err.errorcode"  , reply.err.errorcode  ),
        castor::dlf::Param("err.maxTpRetry", reply.err.maxTpRetry),
        castor::dlf::Param("err.maxCpRetry", reply.err.maxCpRetry)};
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

  // Give volume ID to RTCPD
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
    request.volReqId      = volReqId;
    request.tStartRequest = tStartRequest;
    
    try {

      Transceiver::giveVolumeIdToRtcpd(cuuid, socket, NETRWTIMEOUT, request,
        reply);

      castor::dlf::Param params[] = {
        castor::dlf::Param("vid"            , reply.vid            ),
        castor::dlf::Param("vsn"            , reply.vsn            ),
        castor::dlf::Param("label"          , reply.label          ),
        castor::dlf::Param("devtype"        , reply.devtype        ),
        castor::dlf::Param("density"        , reply.density        ),
        castor::dlf::Param("unit"           , reply.unit           ),
        castor::dlf::Param("volReqId"       , reply.volReqId       ),
        castor::dlf::Param("jobId"          , reply.jobId          ),
        castor::dlf::Param("mode"           , reply.mode           ),
        castor::dlf::Param("start_file"     , reply.start_file     ),
        castor::dlf::Param("end_file"       , reply.end_file       ),
        castor::dlf::Param("side"           , reply.side           ),
        castor::dlf::Param("tprc"           , reply.tprc           ),
        castor::dlf::Param("tStartRequest"  , reply.tStartRequest  ),
        castor::dlf::Param("tEndRequest"    , reply.tEndRequest    ),
        castor::dlf::Param("tStartRtcpd"    , reply.tStartRtcpd    ),
        castor::dlf::Param("tStartMount"    , reply.tStartMount    ),
        castor::dlf::Param("tEndMount"      , reply.tEndMount      ),
        castor::dlf::Param("tStartUnmount"  , reply.tStartUnmount  ),
        castor::dlf::Param("tEndUnmount"    , reply.tEndUnmount    ),
        castor::dlf::Param("rtcpReqId"      , reply.rtcpReqId      ),
        castor::dlf::Param("err.errmsgtxt"  , reply.err.errmsgtxt  ),
        castor::dlf::Param("err.severity"   , reply.err.severity   ),
        castor::dlf::Param("err.errorcode"  , reply.err.errorcode  ),
        castor::dlf::Param("err.maxTpRetry", reply.err.maxTpRetry),
        castor::dlf::Param("err.maxCpRetry", reply.err.maxCpRetry)};
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
    request.volReqId       = volReqId;
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

      Transceiver::giveFileInfoToRtcpd(cuuid, socket, NETRWTIMEOUT, request,
        reply);

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
    Transceiver::signalNoMoreRequestsToRtcpd(cuuid, socket, NETRWTIMEOUT);
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
}


//-----------------------------------------------------------------------------
// checkRcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandlerThread::
  checkRcpJobSubmitterIsAuthorised(castor::io::AbstractTCPSocket &socket)
  throw (castor::exception::Exception) {

  char peerHost[CA_MAXHOSTNAMELEN+1];

  // isadminhost fills in peerHost
  const int rc = isadminhost(socket.socket(), peerHost);

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
