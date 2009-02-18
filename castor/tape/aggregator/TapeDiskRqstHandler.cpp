/******************************************************************************
 *                      TapeDiskRqstHandler.cpp
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
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/TapeDiskRqstHandler.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/rtcp_constants.h"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::TapeDiskRqstHandler::TapeDiskRqstHandler()
  throw() {

  // Build the map of message body handlers
  m_handlers[RTCP_FILE_REQ]    = &TapeDiskRqstHandler::rtcpFileReqHandler;
  m_handlers[RTCP_FILEERR_REQ] = &TapeDiskRqstHandler::rtcpFileErrReqHandler;
  m_handlers[RTCP_TAPE_REQ]    = &TapeDiskRqstHandler::rtcpTapeReqHandler;
  m_handlers[RTCP_TAPEERR_REQ] = &TapeDiskRqstHandler::rtcpTapeErrReqHandler;
  m_handlers[RTCP_ENDOF_REQ]   = &TapeDiskRqstHandler::rtcpEndOfReqHandler;
}


//-----------------------------------------------------------------------------
// processRequest
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::processRequest(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const int socketFd) throw(castor::exception::Exception) {

  MessageHeader header;

  RtcpTxRx::receiveRtcpMsgHeader(socketFd, RTCPDNETRWTIMEOUT, header);

  // Find the message type's corresponding handler
  MsgBodyHandlerMap::iterator itor = m_handlers.find(header.reqType);
  if(itor == m_handlers.end()) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Unknown request type: 0x" << header.reqType;

    throw ex;
  }
  const MsgBodyHandler handler = itor->second;

  // Invoke the handler
  return (this->*handler)(cuuid, volReqId, mode, header, socketFd);
}


//-----------------------------------------------------------------------------
// RTCP_FILE_REQ message body handler.
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpFileReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstBody(socketFd, RTCPDNETRWTIMEOUT, header, body);

  switch(body.procStatus) {
/*
  case RTCP_REQUEST_MORE_WORK:
    {
      RtcpFileRqstErrMsgBody rtcpFileInfoRequest;
      Utils::setBytes(rtcpFileInfoRequest, '\0');

      RtcpFileRqstErrMsgBody rtcpFileInfoReply;
      Utils::setBytes(rtcpFileInfoReply, '\0');

     // If migrating
     if(mode == WRITE_ENABLE) {

       char     nsHost[CA_MAXHOSTNAMELEN];
       uint64_t fileId;
       uint64_t fileSize;
       char     lastKnownFileName[CA_MAXPATHLEN+1];
       uint64_t lastModificationTime;

       // If there is NO file to migrate?
       if(!GatewayTxRx::getFileToMigrateFromGateway(volHost, volPort,
         volReqId, rtcpFileInfoRequest.filePath, rtcpFileInfoRequest.recfm,
         nsHost, fileId, rtcpFileInfoRequest.tapeFseq, fileSize,
         lastKnownFileName, lastModificationTime)) {
I AM HERE

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", volReqId     ),
        castor::dlf::Param("Port"    , volPort      ),
        castor::dlf::Param("HostName", volHost      )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_NO_MIGRATION_REQUEST_FOR_VOLUME, params);

      return;
    }
      // Give file information to RTCPD
      try {
        // Send: file to migrate  to RTCPD
        RtcpTxRx::giveFileInfoToRtcpd(socketFd, RTCPDNETRWTIMEOUT,
          volReqId,
          "lxc2disk07:/tmp/murrayc3/test_04_02_09", body.tapePath, 18);

        // Send joker More work to RTCPD
        RtcpTxRx::giveRequestForMoreWorkToRtcpd(socketFd, RTCPDNETRWTIMEOUT,
          volReqId);

        // Send EndOfFileList
        RtcpTxRx::signalNoMoreRequestsToRtcpd(socketFd, RTCPDNETRWTIMEOUT);

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", volReqId),
          castor::dlf::Param("filePath","lxc2disk07:/dev/null"),
          castor::dlf::Param("tapePath", body.tapePath)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_GAVE_FILE_INFO, params);
      } catch(castor::exception::Exception &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", volReqId),
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
      RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
*/
  case RTCP_POSITIONED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", volReqId),
        castor::dlf::Param("filePath", "lxc2disk07:/dev/null"),
        castor::dlf::Param("tapePath", body.tapePath)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_TAPE_POSITIONED_FILE_REQ, params);

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILE_REQ;
      ackMsg.status  = 0;
      RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
  case RTCP_FINISHED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", volReqId),
        castor::dlf::Param("filePath", "lxc2disk07:/dev/null"),
        castor::dlf::Param("tapePath", body.tapePath)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FILE_TRANSFERED, params);

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILE_REQ;
      ackMsg.status  = 0;
      RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);
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

  // There is a possibility of more work
  return true;
}


//-----------------------------------------------------------------------------
// RTCP_FILEERR_REQ message body handler.
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpFileErrReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstErrBody(socketFd, RTCPDNETRWTIMEOUT,
    header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_FILEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

  castor::exception::Exception ex(body.err.errorCode);

  ex.getMessage() << __PRETTY_FUNCTION__
    << ": Received an error from RTCPD:" << body.err.errorMsg;

  throw ex;
}


//-----------------------------------------------------------------------------
// RTCP_TAPEREQ message body handler.
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpTapeReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstBody(socketFd, RTCPDNETRWTIMEOUT,
    header, body);

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", volReqId)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
    AGGREGATOR_TAPE_POSITIONED_TAPE_REQ, params);

  // Acknowledge tape request
  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPE_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

  // There is a possibility of more work
  return true;
}


//-----------------------------------------------------------------------------
// RTCP_TAPEERR message body handler.
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpTapeErrReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstErrBody(socketFd, RTCPDNETRWTIMEOUT,
    header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

  castor::exception::Exception ex(body.err.errorCode);

  ex.getMessage() << __PRETTY_FUNCTION__
    << ": Received an error from RTCPD:" << body.err.errorMsg;

  throw ex;
}


//-----------------------------------------------------------------------------
// RTCP_ENDOF_REQ message body handler.
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpEndOfReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  // An RTCP_ENDOF_REQ message is bodiless
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_RECEIVED_RTCP_ENDOF_REQ);

  // Acknowledge RTCP_ENDOF_REQ message
  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_ENDOF_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(socketFd, RTCPDNETRWTIMEOUT, ackMsg);

  // There is no more work
  return false;
}
