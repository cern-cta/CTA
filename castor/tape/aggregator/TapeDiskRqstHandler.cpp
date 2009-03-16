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
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/TapeDiskRqstHandler.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/Ctape_constants.h"
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
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort,const uint32_t mode, const int socketFd)
  throw(castor::exception::Exception) {

/*
{                                                                         
  const int fd = socketFd;
  void *ptr = NULL;                                                       

  char buf[280];
  Utils::setBytes(buf, '\0');
  ptr = buf;                 
  read(fd, ptr, sizeof(buf));

  uint32_t magic   = 0;
  uint32_t reqType = 0;
  uint32_t len     = 0;

  memcpy(&magic,   buf                                  , sizeof(magic)  );
  memcpy(&reqType, buf + sizeof(magic)                  , sizeof(reqType));
  memcpy(&len    , buf + sizeof(magic) + sizeof(reqType), sizeof(len)    );

  char tmp[4];
  tmp[0] = *(((char*)&magic) + 3);
  tmp[1] = *(((char*)&magic) + 2);
  tmp[2] = *(((char*)&magic) + 1);
  tmp[3] = *(((char*)&magic) + 0);
  magic = *((uint32_t*)tmp);      

  tmp[0] = *(((char*)&reqType) + 3);
  tmp[1] = *(((char*)&reqType) + 2);
  tmp[2] = *(((char*)&reqType) + 1);
  tmp[3] = *(((char*)&reqType) + 0);
  reqType = *((uint32_t*)tmp);      

  tmp[0] = *(((char*)&len) + 3);
  tmp[1] = *(((char*)&len) + 2);
  tmp[2] = *(((char*)&len) + 1);
  tmp[3] = *(((char*)&len) + 0);
  len = *((uint32_t*)tmp);      

  std::cout << "HELLO" << std::endl;

  std::cout << "magic  =0x" << std::hex << magic   << std::endl;
  std::cout << "reqType=0x" << std::hex << reqType << std::endl;
  std::cout << "len    ="   << std::dec << len     << std::endl;

  for(size_t i=12; i<sizeof(buf); i++) {
    char c = buf[i];                    

    if((c >= '0' && c <= '9') || (c >= 'a' && c <='z') || (c >= 'A' && c <='Z'))
      {                                                                         
      std::cout << c;                                                           
    } else {                                                                    
//    std::cout << "(0x" << std::hex << (int)c << ")";                          
      std::cout << "#";                                                         
    }                                                                           
  }                                                                             
  std::cout << std::endl;                                                       

  castor::dlf::Param params[] = {
    castor::dlf::Param("msg"    , "Hardcoded reads"),
    castor::dlf::Param("fd"     , fd               ),
    castor::dlf::Param("magic"  , magic            ),
    castor::dlf::Param("reqType", reqType          ),
    castor::dlf::Param("len"    , len              )};
  CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, params);

  return true;
}
*/
  MessageHeader header;

  RtcpTxRx::receiveRtcpMsgHeader(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    header);

  {
    char magicHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    magicHex[0] = '0';
    magicHex[1] = 'x';
    Utils::toHex(header.magic, &(magicHex[2]), 17);

    const char *magicName = Utils::magicToStr(header.magic);

    char reqTypeHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    reqTypeHex[0] = '0';
    reqTypeHex[1] = 'x';
    Utils::toHex(header.reqType, &(reqTypeHex[2]), 17);

    const char *reqTypeName = Utils::rtcopyReqTypeToStr(header.reqType);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("socketFd"   , socketFd   ),
      castor::dlf::Param("magic"      , magicHex   ),
      castor::dlf::Param("magicName"  , magicName  ),
      castor::dlf::Param("reqType"    , reqTypeHex ),
      castor::dlf::Param("reqTypeName", reqTypeName),
      castor::dlf::Param("len"        , header.len )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_PROCESSING_TAPE_DISK_RQST, params);
  }

  // Find the message type's corresponding handler
  MsgBodyHandlerMap::iterator itor = m_handlers.find(header.reqType);
  if(itor == m_handlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Unknown request type: 0x" << header.reqType);
  }
  const MsgBodyHandler handler = itor->second;

  // Invoke the handler
  return (this->*handler)(cuuid, volReqId, gatewayHost, gatewayPort, mode,
    header, socketFd);
}


//-----------------------------------------------------------------------------
// rtcpFileReqHandler
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpFileReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  bool thereIsMoreWork = true;

  RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  switch(body.procStatus) {
  case RTCP_REQUEST_MORE_WORK:
    // If migrating
    if(mode == WRITE_ENABLE) {

      char filePath[CA_MAXPATHLEN+1];
      Utils::setBytes(filePath, '\0');
      char nsHost[CA_MAXHOSTNAMELEN+1];
      Utils::setBytes(nsHost, '\0');
      uint64_t fileId = 0;
      uint32_t tapeFseq = 0;
      uint64_t fileSize = 0;
      char lastKnownFileName[CA_MAXPATHLEN+1];
      Utils::setBytes(lastKnownFileName, '\0');
      uint64_t lastModificationTime = 0;
      int32_t  positionMethod = 0;

      // If there is a file to migrate
      if(GatewayTxRx::getFileToMigrateFromGateway(cuuid, volReqId, gatewayHost,
        gatewayPort, filePath, nsHost, fileId, tapeFseq, fileSize,
        lastKnownFileName, lastModificationTime, positionMethod)) {

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
          castor::dlf::Param("lastModificationTime", lastModificationTime),
          castor::dlf::Param("positionMethod"      , positionMethod      )};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_FILE_TO_MIGRATE, params);

        char tapeFileId[CA_MAXPATHLEN+1];
        Utils::toHex(fileId, tapeFileId);
        RtcpTxRx::giveFileToRtcpd(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
          WRITE_ENABLE, filePath, "", RECORDFORMAT, tapeFileId, MIGRATEUMASK,
          positionMethod, nsHost, fileId);

        RtcpTxRx::askRtcpdToRequestMoreWork(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_ENABLE);

        RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

      // Else there is no file to migrate
      } else {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"   , volReqId   ),
          castor::dlf::Param("gatewayHost", gatewayHost),
          castor::dlf::Param("gatewayPort", gatewayPort)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NO_MORE_FILES_TO_MIGRATE, params);

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

        thereIsMoreWork = false;
      }

    // Else recalling
    } else {

      char filePath[CA_MAXPATHLEN+1];
      Utils::setBytes(filePath, '\0');
      char nsHost[CA_MAXHOSTNAMELEN+1];
      Utils::setBytes(nsHost, '\0');
      uint64_t fileId = 0;
      uint32_t tapeFseq = 0;
      unsigned char blockId[4];
      Utils::setBytes(blockId, '\0');
      int32_t  positionCommandCode = 0;

      // If there is a file to recall 
      if(GatewayTxRx::getFileToRecallFromGateway(cuuid, volReqId, gatewayHost,
        gatewayPort, filePath, nsHost, fileId, tapeFseq, blockId,
        positionCommandCode)) {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"           , volReqId           ),
          castor::dlf::Param("gatewayHost"        , gatewayHost        ),
          castor::dlf::Param("gatewayPort"        , gatewayPort        ),
          castor::dlf::Param("filePath"           , filePath           ),
          castor::dlf::Param("nsHost"             , nsHost             ),
          castor::dlf::Param("fileId"             , fileId             ),
          castor::dlf::Param("tapeFseq"           , tapeFseq           ),
          castor::dlf::Param("blockId[0]"         , blockId[0]         ),
          castor::dlf::Param("blockId[1]"         , blockId[1]         ),
          castor::dlf::Param("blockId[2]"         , blockId[2]         ),
          castor::dlf::Param("blockId[3]"         , blockId[3]         ),
          castor::dlf::Param("positionCommandCode", positionCommandCode)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_FILE_TO_RECALL, params);

        char tapeFileId[CA_MAXPATHLEN+1];
        Utils::setBytes(tapeFileId, '\0');
        RtcpTxRx::giveFileToRtcpd(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
          WRITE_DISABLE, filePath, body.tapePath, RECORDFORMAT, tapeFileId,
          RECALLUMASK, positionCommandCode, nsHost, fileId);

        RtcpTxRx::askRtcpdToRequestMoreWork(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_DISABLE);

	RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

      // Else there is no file to recall
      } else {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"   , volReqId   ),
          castor::dlf::Param("gatewayHost", gatewayHost),
          castor::dlf::Param("gatewayPort", gatewayPort)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NO_MORE_FILES_TO_RECALL, params);

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId, socketFd,
          RTCPDNETRWTIMEOUT);

        thereIsMoreWork = false;
      }
    } // Else recalling
    break;
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
      RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);
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

      // If migrating
      if(mode == WRITE_ENABLE) {
      // Else recalling
      } else {
      }

      RtcpAcknowledgeMsg ackMsg;
      ackMsg.magic   = RTCOPY_MAGIC;
      ackMsg.reqType = RTCP_FILE_REQ;
      ackMsg.status  = 0;
      RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
           ": Received unexpected file request process status 0x"
        << std::hex << body.procStatus
        << "(" << Utils::procStatusToStr(body.procStatus) << ")");
    }
  }

  return thereIsMoreWork;
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqHandler
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpFileErrReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpFileRqstErrBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_FILEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  TAPE_THROW_CODE(body.err.errorCode,
    ": Received an error from RTCPD: " << body.err.errorMsg);
}


//-----------------------------------------------------------------------------
// rtcpTapeReqHandler
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpTapeReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId", volReqId)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_TAPE_POSITIONED_TAPE_REQ, params);

  // Acknowledge tape request
  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPE_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  // There is a possibility of more work
  return true;
}


//-----------------------------------------------------------------------------
// rtcpTapeErrReqHandler
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpTapeErrReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpTapeRqstErrBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  if(body.err.errorCode == 0) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
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
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);
/*
{                                                                         
  const int fd = socketFd;
  void *ptr = NULL;                                                       

  char buf[12];
  Utils::setBytes(buf, '\0');
  ptr = buf;                 
  read(fd, ptr, sizeof(buf));

  uint32_t magic   = 0;
  uint32_t reqType = 0;
  uint32_t len     = 0;

  memcpy(&magic,   buf                                  , sizeof(magic)  );
  memcpy(&reqType, buf + sizeof(magic)                  , sizeof(reqType));
  memcpy(&len    , buf + sizeof(magic) + sizeof(reqType), sizeof(len)    );

  char tmp[4];
  tmp[0] = *(((char*)&magic) + 3);
  tmp[1] = *(((char*)&magic) + 2);
  tmp[2] = *(((char*)&magic) + 1);
  tmp[3] = *(((char*)&magic) + 0);
  magic = *((uint32_t*)tmp);      

  tmp[0] = *(((char*)&reqType) + 3);
  tmp[1] = *(((char*)&reqType) + 2);
  tmp[2] = *(((char*)&reqType) + 1);
  tmp[3] = *(((char*)&reqType) + 0);
  reqType = *((uint32_t*)tmp);      

  tmp[0] = *(((char*)&len) + 3);
  tmp[1] = *(((char*)&len) + 2);
  tmp[2] = *(((char*)&len) + 1);
  tmp[3] = *(((char*)&len) + 0);
  len = *((uint32_t*)tmp);      

  std::cout << "HELLO" << std::endl;

  std::cout << "magic  =0x" << std::hex << magic   << std::endl;
  std::cout << "reqType=0x" << std::hex << reqType << std::endl;
  std::cout << "len    ="   << std::dec << len     << std::endl;

  for(size_t i=12; i<sizeof(buf); i++) {
    char c = buf[i];                    

    if((c >= '0' && c <= '9') || (c >= 'a' && c <='z') || (c >= 'A' && c <='Z'))
      {                                                                         
      std::cout << c;                                                           
    } else {                                                                    
//    std::cout << "(0x" << std::hex << (int)c << ")";                          
      std::cout << "#";                                                         
    }                                                                           
  }                                                                             
  std::cout << std::endl;                                                       

  castor::dlf::Param params[] = {
    castor::dlf::Param("msg"    , "Hardcoded reads"),
    castor::dlf::Param("fd"     , fd               ),
    castor::dlf::Param("magic"  , magic            ),
    castor::dlf::Param("reqType", reqType          ),
    castor::dlf::Param("len"    , len              )};
  CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_SYSTEM, AGGREGATOR_NULL, params);

  return true;
}
*/
  // There is a possibility of more work
  return true;
}


//-----------------------------------------------------------------------------
// rtcpEndOfReqHandler
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::TapeDiskRqstHandler::rtcpEndOfReqHandler(
  const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
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
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  // There is no more work
  return false;
}
