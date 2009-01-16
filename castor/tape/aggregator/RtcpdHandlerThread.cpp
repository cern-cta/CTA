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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
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

  // Send volume to RTCPD
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

      sendVolumeToRtcpd(cuuid, *socket, NETRWTIMEOUT, request, reply);

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

  // Send file to RTCPD
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

      sendFileToRtcpd(cuuid, *socket, NETRWTIMEOUT, request, reply);

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
        AGGREGATOR_SENT_FILE, 62, params);

    } catch(castor::exception::Exception &ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        AGGREGATOR_FAILED_TO_SEND_FILE, 3, params);
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
  RtcpTapeRequestMessage &request, RtcpTapeRequestMessage &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpTapeRequestMessage(buf, request);
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

  // Length of body buffer = Length of message buffer - length of header
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
    Marshaller::unmarshallRtcpTapeRequestMessageBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
  }
}


//-----------------------------------------------------------------------------
// sendFileToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::
  sendFileToRtcpd(const Cuuid_t &cuuid,
  castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
  RtcpFileRequestMessage &request, RtcpFileRequestMessage &reply)
  throw(castor::exception::Exception) {

  // Marshall the message
  char buf[MSGBUFSIZ];
  size_t totalLen = 0;
  try {
    totalLen = Marshaller::marshallRtcpFileRequestMessage(buf, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to marshall file message: "
      << ex.getMessage().str();

    throw ie;
  }

  // Send the message
  try {
    SocketHelper::writeBytes(socket, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(SECOMERR);

    ex2.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to send file message to RTCPD: "
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
  if(ackMsg.reqtype != RTCP_FILEERR_REQ) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << std::hex << RTCP_FILEERR_REQ
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
}


//-----------------------------------------------------------------------------
// receiveRtcpFileRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::RtcpdHandlerThread::receiveRtcpFileRequest(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &socket,
  const int netReadWriteTimeout, RtcpFileRequestMessage &request)
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
  if(header.reqtype != RTCP_FILEERR_REQ) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << std::hex
      << ": Invalid request type from RTCPD"
      << ": Expected: 0x" << RTCP_TAPEERR_REQ
      << ": Received: 0x" << header.reqtype;

    throw ex;
  }

  // Length of body buffer = Length of message buffer - length of header
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
    Marshaller::unmarshallRtcpFileRequestMessageBody(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Internal ie;

    ie.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to unmarshall message body from RTCPD"
      << ": "<< ex.getMessage().str();

    throw ie;
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
    Marshaller::unmarshallRtcpAcknowledgeMessageBody(p, remainingLen, message);
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
