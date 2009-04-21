/******************************************************************************
 *                      RtcpdBridgeProtocolEngine.cpp
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

#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpdBridgeProtocolEngine.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RtcpdBridgeProtocolEngine(
  const Cuuid_t &cuuid,
  const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort,
  const uint32_t mode) throw():
  m_cuuid(cuuid),
  m_volReqId(volReqId),
  m_gatewayHost(gatewayHost),
  m_gatewayPort(gatewayPort),
  m_mode(mode) {

  // Build the map of message body handlers
  m_handlers[RTCP_FILE_REQ] = 
    &RtcpdBridgeProtocolEngine::rtcpFileReqCallback;
  m_handlers[RTCP_FILEERR_REQ] =
    &RtcpdBridgeProtocolEngine::rtcpFileErrReqCallback;
  m_handlers[RTCP_TAPE_REQ] =
    &RtcpdBridgeProtocolEngine::rtcpTapeReqCallback;
  m_handlers[RTCP_TAPEERR_REQ] =
    &RtcpdBridgeProtocolEngine::rtcpTapeErrReqCallback;
  m_handlers[RTCP_ENDOF_REQ] =
    &RtcpdBridgeProtocolEngine::rtcpEndOfReqCallback;
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult 
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::run(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  {
    char magicHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    magicHex[0] = '0';
    magicHex[1] = 'x';
    utils::toHex(header.magic, &(magicHex[2]), 17);

    const char *magicName = utils::magicToStr(header.magic);

    char reqTypeHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    reqTypeHex[0] = '0';
    reqTypeHex[1] = 'x';
    utils::toHex(header.reqType, &(reqTypeHex[2]), 17);

    const char *reqTypeName = utils::rtcopyReqTypeToStr(header.reqType);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , m_volReqId ),
      castor::dlf::Param("socketFd"   , socketFd   ),
      castor::dlf::Param("magic"      , magicHex   ),
      castor::dlf::Param("magicName"  , magicName  ),
      castor::dlf::Param("reqType"    , reqTypeHex ),
      castor::dlf::Param("reqTypeName", reqTypeName),
      castor::dlf::Param("len"        , header.len )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_PROCESSING_TAPE_DISK_RQST, params);
  }

  // Find the message type's corresponding handler
  MsgBodyCallbackMap::iterator itor = m_handlers.find(header.reqType);
  if(itor == m_handlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Unknown request type: 0x" << header.reqType);
  }
  const MsgBodyCallback handler = itor->second;

  // Invoke the handler
  return (this->*handler)(header, socketFd);
}


//-----------------------------------------------------------------------------
// rtcpFileReqCallback
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::rtcpFileReqCallback(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RqstResult result = REQUEST_PROCESSED;

  RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstBody(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  switch(body.procStatus) {
  case RTCP_REQUEST_MORE_WORK:
    // If migrating
    if(m_mode == WRITE_ENABLE) {

      char filePath[CA_MAXPATHLEN+1];
      utils::setBytes(filePath, '\0');
      char nsHost[CA_MAXHOSTNAMELEN+1];
      utils::setBytes(nsHost, '\0');
      char tapePath[CA_MAXPATHLEN+1];
      utils::setBytes(tapePath, '\0');
      uint64_t fileId = 0;
      uint32_t tapeFseq = 0;
      uint64_t fileSize = 0;
      char lastKnownFileName[CA_MAXPATHLEN+1];
      utils::setBytes(lastKnownFileName, '\0');
      uint64_t lastModificationTime = 0;
      int32_t  positionMethod = 0;
       unsigned char blockId[4];
      utils::setBytes(blockId, '\0');

      // If there is a file to migrate
      if(GatewayTxRx::getFileToMigrateFromGateway(m_cuuid, m_volReqId,
        m_gatewayHost, m_gatewayPort, filePath, nsHost, fileId, tapeFseq,
        fileSize, lastKnownFileName, lastModificationTime, positionMethod)) {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"            , m_volReqId          ),
          castor::dlf::Param("gatewayHost"         , m_gatewayHost       ),
          castor::dlf::Param("gatewayPort"         , m_gatewayPort       ),
          castor::dlf::Param("filePath"            , filePath            ),
          castor::dlf::Param("nsHost"              , nsHost              ),
          castor::dlf::Param("fileId"              , fileId              ),
          castor::dlf::Param("tapeFseq"            , tapeFseq            ),
          castor::dlf::Param("fileSize"            , fileSize            ),
          castor::dlf::Param("lastKnownFileName"   , lastKnownFileName   ),
          castor::dlf::Param("lastModificationTime", lastModificationTime),
          castor::dlf::Param("positionMethod"      , positionMethod      )};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_FILE_TO_MIGRATE, params);

        char tapeFileId[CA_MAXPATHLEN+1];
        utils::toHex(fileId, tapeFileId);
        RtcpTxRx::giveFileToRtcpd(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_ENABLE, filePath, "", RECORDFORMAT,
          tapeFileId, MIGRATEUMASK, tapeFseq, positionMethod, nsHost, fileId,
          blockId);

        RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_volReqId, tapePath,
          socketFd, RTCPDNETRWTIMEOUT, WRITE_ENABLE);

        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

      // Else there is no file to migrate
      } else {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"   , m_volReqId   ),
          castor::dlf::Param("gatewayHost", m_gatewayHost),
          castor::dlf::Param("gatewayPort", m_gatewayPort)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NO_MORE_FILES_TO_MIGRATE, params);

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT);
      }

    // Else recalling
    } else {

      char filePath[CA_MAXPATHLEN+1];
      utils::setBytes(filePath, '\0');
      char nsHost[CA_MAXHOSTNAMELEN+1];
      utils::setBytes(nsHost, '\0');
      uint64_t fileId = 0;
      uint32_t tapeFseq = 0;
      unsigned char blockId[4];
      utils::setBytes(blockId, '\0');
      int32_t  positionCommandCode = 0;

      // If there is a file to recall 
      if(GatewayTxRx::getFileToRecallFromGateway(m_cuuid, m_volReqId,
        m_gatewayHost, m_gatewayPort, filePath, nsHost, fileId, tapeFseq,
        blockId, positionCommandCode)) {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"           , m_volReqId         ),
          castor::dlf::Param("gatewayHost"        , m_gatewayHost      ),
          castor::dlf::Param("gatewayPort"        , m_gatewayPort      ),
          castor::dlf::Param("filePath"           , filePath           ),
          castor::dlf::Param("nsHost"             , nsHost             ),
          castor::dlf::Param("fileId"             , fileId             ),
          castor::dlf::Param("tapeFseq"           , tapeFseq           ),
          castor::dlf::Param("blockId[0]"         , blockId[0]         ),
          castor::dlf::Param("blockId[1]"         , blockId[1]         ),
          castor::dlf::Param("blockId[2]"         , blockId[2]         ),
          castor::dlf::Param("blockId[3]"         , blockId[3]         ),
          castor::dlf::Param("positionCommandCode", positionCommandCode)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_FILE_TO_RECALL, params);

        char tapeFileId[CA_MAXPATHLEN+1];
        utils::setBytes(tapeFileId, '\0');
        RtcpTxRx::giveFileToRtcpd(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_DISABLE, filePath, body.tapePath,
          RECORDFORMAT, tapeFileId, RECALLUMASK, positionCommandCode, tapeFseq,
          nsHost, fileId, blockId);

        RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_volReqId, body.tapePath, 
          socketFd, RTCPDNETRWTIMEOUT, WRITE_DISABLE);

	RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT);
      
        // Asynchronus Request More Work ACK 
        RtcpAcknowledgeMsg ackMsg;
        ackMsg.magic   = RTCOPY_MAGIC;
        ackMsg.reqType = RTCP_FILE_REQ;
        ackMsg.status  = 0;
        RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd, 
          RTCPDNETRWTIMEOUT,ackMsg);
        
      // Else there is no file to recall
      } else {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"   , m_volReqId   ),
          castor::dlf::Param("gatewayHost", m_gatewayHost),
          castor::dlf::Param("gatewayPort", m_gatewayPort)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NO_MORE_FILES_TO_RECALL, params);

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

        // Asynchronus Request More Work ACK 
        RtcpAcknowledgeMsg ackMsg;
        ackMsg.magic   = RTCOPY_MAGIC;
        ackMsg.reqType = RTCP_FILE_REQ;
        ackMsg.status  = 0;
        RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd, 
          RTCPDNETRWTIMEOUT,ackMsg);
      }
    } // Else recalling
    break;
  case RTCP_POSITIONED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_volReqId),
        castor::dlf::Param("filePath", body.filePath),
        castor::dlf::Param("tapePath", body.tapePath)};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_TAPE_POSITIONED_FILE_REQ, params);

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILE_REQ;
      ackMsg.status  = 0;
      RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
  case RTCP_FINISHED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_volReqId),
        castor::dlf::Param("filePath", body.filePath),
        castor::dlf::Param("tapePath", body.tapePath)};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FILE_TRANSFERED, params);

      // If migrating
      if(m_mode == WRITE_ENABLE) {
      // Else recalling
      } else {
      }

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILE_REQ;
      ackMsg.status  = 0;
      RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);

      // Connect to Gateway and tell "finish"
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
           ": Received unexpected file request process status 0x"
        << std::hex << body.procStatus
        << "(" << utils::procStatusToStr(body.procStatus) << ")");
    }
  }

  return result;
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqCallback
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult 
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::rtcpFileErrReqCallback(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstErrBody(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_FILEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);

  TAPE_THROW_CODE(body.err.errorCode,
    ": Received an error from RTCPD: " << body.err.errorMsg);
}


//-----------------------------------------------------------------------------
// rtcpTapeReqCallback
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult 
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::rtcpTapeReqCallback(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstBody(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", m_volReqId)};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_TAPE_POSITIONED_TAPE_REQ, params);

  // Acknowledge tape request
  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPE_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);

  // There is a possibility of more work
  return REQUEST_PROCESSED;
}


//-----------------------------------------------------------------------------
// rtcpTapeErrReqCallback
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult 
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::rtcpTapeErrReqCallback(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstErrBody(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  if(body.err.errorCode == 0) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", m_volReqId)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_TAPE_POSITIONED_TAPE_REQ, params);
  } else {
    char codeStr[STRERRORBUFLEN];
    sstrerror_r(body.err.errorCode, codeStr, sizeof(codeStr));

    TAPE_THROW_CODE(body.err.errorCode,
      ": Received an error from RTCPD"
      ": errorCode=" << body.err.errorCode <<
      ": errorMsg=" << body.err.errorMsg  <<
      ": sstrerror_r=" << codeStr);
  }

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);

  // There is a possibility of more work
  return REQUEST_PROCESSED;
}


//-----------------------------------------------------------------------------
// rtcpEndOfReqCallback
//-----------------------------------------------------------------------------
castor::tape::aggregator::RtcpdBridgeProtocolEngine::RqstResult 
  castor::tape::aggregator::RtcpdBridgeProtocolEngine::rtcpEndOfReqCallback(
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  // An RTCP_ENDOF_REQ message is bodiless
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_RECEIVED_RTCP_ENDOF_REQ);

  // Acknowledge RTCP_ENDOF_REQ message
  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_ENDOF_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(m_cuuid, m_volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);

  // There is no more work
  //return false;

  return RECEIVED_EOR;
}
