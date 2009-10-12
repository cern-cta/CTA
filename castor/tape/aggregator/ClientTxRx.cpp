/******************************************************************************
 *                castor/tape/aggregator/ClientTxRx.cpp
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

#include "castor/Constants.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/ClientTxRx.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/LogHelper.hpp"
#include "castor/tape/aggregator/SynchronizedCounter.hpp"
#include "castor/tape/tapegateway/DumpNotification.hpp"
#include "castor/tape/tapegateway/DumpParametersRequest.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/PingNotification.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/utils/utils.hpp"

#include <pthread.h>


//------------------------------------------------------------------------------
// getVolume
//------------------------------------------------------------------------------
castor::tape::tapegateway::Volume
  *castor::tape::aggregator::ClientTxRx::getVolume(const Cuuid_t &cuuid,
  const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort, const char (&unit)[CA_MAXUNMLEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort),
      castor::dlf::Param("unit"      , unit      )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_GET_VOLUME_FROM_CLIENT, params);
  }

  // Prepare the request
  tapegateway::VolumeRequest request;
  request.setMountTransactionId(volReqId);
  request.setUnit(unit);

  // Send the request and receive the reply
  time_t connectDuration  = 0;
  time_t sendRecvDuration = 0;
  std::auto_ptr<castor::IObject> obj(sendRequestAndReceiveReply(
    "VolumeRequest", clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
    connectDuration, sendRecvDuration));

  switch(obj->type()) {
  case OBJ_Volume:
    {
      // Down cast the reply to its specific class
      tapegateway::Volume *reply =
        dynamic_cast<tapegateway::Volume*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::Volume");
      }

      checkMountTransactionId(volReqId, reply->mountTransactionId());

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_VOLUME_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }

  case OBJ_NoMoreFiles:
    {
      // Down cast the reply to its specific class
      tapegateway::NoMoreFiles *reply =
        dynamic_cast<tapegateway::NoMoreFiles*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"  , volReqId  ),
        castor::dlf::Param("clientHost", clientHost),
        castor::dlf::Param("clientPort", clientPort),
        castor::dlf::Param("unit"      , unit      )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_NO_MORE_FILES_FROM_CLIENT, params);

      checkMountTransactionId(volReqId, reply->mountTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(volReqId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// getFileToMigrate
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FileToMigrate
  *castor::tape::aggregator::ClientTxRx::getFileToMigrate(const Cuuid_t &cuuid,
  const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_GET_FILE_TO_MIGRATE_FROM_CLIENT, params);
  }

  // Prepare the request
  tapegateway::FileToMigrateRequest request;
  request.setMountTransactionId(volReqId);

  // Send the request and receive the reply
  time_t connectDuration  = 0;
  time_t sendRecvDuration = 0;
  std::auto_ptr<castor::IObject> obj(sendRequestAndReceiveReply(
    "FileToMigrateRequest", clientHost, clientPort, CLIENTNETRWTIMEOUT,
    request, connectDuration, sendRecvDuration));

  switch(obj->type()) {
  case OBJ_FileToMigrate:
    {
      // Down cast the reply to its specific class
      tapegateway::FileToMigrate *reply =
        dynamic_cast<tapegateway::FileToMigrate*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::FileToMigrate");
      }

      checkMountTransactionId(volReqId, reply->mountTransactionId());

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_FILE_TO_MIGRATE_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }
    break;

  case OBJ_NoMoreFiles:
    {
      // Down cast the reply to its specific class
      tapegateway::NoMoreFiles *reply =
        dynamic_cast<tapegateway::NoMoreFiles*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"  , volReqId  ),
        castor::dlf::Param("clientHost", clientHost),
        castor::dlf::Param("clientPort", clientPort)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_NO_MORE_FILES_FROM_CLIENT, params);

      checkMountTransactionId(volReqId, reply->mountTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(volReqId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// getFileToRecall
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FileToRecall
  *castor::tape::aggregator::ClientTxRx::getFileToRecall(const Cuuid_t &cuuid,
  const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort) throw(castor::exception::Exception) {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"  , volReqId  ),
    castor::dlf::Param("clientHost", clientHost),
    castor::dlf::Param("clientPort", clientPort)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
    AGGREGATOR_GET_FILE_TO_RECALL_FROM_CLIENT, params);

  // Prepare the request
  tapegateway::FileToRecallRequest request;
  request.setMountTransactionId(volReqId);

  // Send the request and receive the reply
  time_t connectDuration  = 0;
  time_t sendRecvDuration = 0;
  std::auto_ptr<castor::IObject> obj(sendRequestAndReceiveReply(
    "FileToRecallRequest", clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
    connectDuration, sendRecvDuration));

  switch(obj->type()) {
  case OBJ_FileToRecall:
    {
      // Down cast the reply to its specific class
      tapegateway::FileToRecall *reply =
        dynamic_cast<tapegateway::FileToRecall*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::FileToRecall");
      }

      checkMountTransactionId(volReqId, reply->mountTransactionId());

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_FILE_TO_RECALL_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }
    break;

  case OBJ_NoMoreFiles:
    {
      // Down cast the reply to its specific class
      tapegateway::NoMoreFiles *reply =
        dynamic_cast<tapegateway::NoMoreFiles*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"  , volReqId  ),
        castor::dlf::Param("clientHost", clientHost),
        castor::dlf::Param("clientPort", clientPort)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_NO_MORE_FILES_FROM_CLIENT, params);

      checkMountTransactionId(volReqId, reply->mountTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(volReqId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// notifyFileMigrated
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::notifyFileMigrated(
  const Cuuid_t        &cuuid,
  const uint32_t       volReqId,
  const char           *clientHost,
  const unsigned short clientPort,
  const uint64_t       fileTransactionId,
  const char           (&nsHost)[CA_MAXHOSTNAMELEN+1],
  const uint64_t       fileId,
  const int32_t        tapeFileSeq,
  const unsigned char  (&blockId)[4],
  const int32_t        positionCommandCode,
  const char           (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1],
  const uint32_t       checksum,
  const uint64_t       fileSize,
  const uint64_t       compressedFileSize)
  throw(castor::exception::Exception) {

  {
    // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
    char checksumHex[11];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    utils::toHex(checksum, &checksumHex[2], 9);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("clientHost"         , clientHost         ),
      castor::dlf::Param("clientPort"         , clientPort         ),
      castor::dlf::Param("fileTransactionId"  , fileTransactionId  ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("blockId[0]"         , blockId[0]         ),
      castor::dlf::Param("blockId[1]"         , blockId[1]         ),
      castor::dlf::Param("blockId[2]"         , blockId[2]         ),
      castor::dlf::Param("blockId[3]"         , blockId[3]         ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksumHex        ),
      castor::dlf::Param("fileSize"           , fileSize           ),
      castor::dlf::Param("compressedFileSize" , compressedFileSize )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFY_CLIENT_FILE_MIGRATED, params);
  }

  // Prepare the request
  tapegateway::FileMigratedNotification request;

  request.setMountTransactionId(volReqId);
  request.setFileTransactionId(fileTransactionId);
  request.setNshost(nsHost);
  request.setFileid(fileId);
  request.setBlockId0(blockId[0]); // block ID=4 integers (little indian order)
  request.setBlockId1(blockId[1]);
  request.setBlockId2(blockId[2]);
  request.setBlockId3(blockId[3]);
  request.setFseq(tapeFileSeq);
  request.setPositionCommandCode(
    (tapegateway::PositionCommandCode)positionCommandCode);
  request.setChecksumName(checksumAlgorithm);
  request.setChecksum(checksum);
  request.setFileSize(fileSize);
  request.setCompressedFileSize( compressedFileSize);

  const char *requestTypeName = "FileMigratedNotification";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
    char checksumHex[11];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    utils::toHex(checksum, &checksumHex[2], 9);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("clientHost"         , clientHost         ),
      castor::dlf::Param("clientPort"         , clientPort         ),
      castor::dlf::Param("fileTransactionId"  , fileTransactionId  ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("blockId[0]"         , blockId[0]         ),
      castor::dlf::Param("blockId[1]"         , blockId[1]         ),
      castor::dlf::Param("blockId[2]"         , blockId[2]         ),
      castor::dlf::Param("blockId[3]"         , blockId[3]         ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksumHex        ),
      castor::dlf::Param("fileSize"           , fileSize           ),
      castor::dlf::Param("compressedFileSize" , compressedFileSize )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_CLIENT_FILE_MIGRATED, params);
  }
}




//-----------------------------------------------------------------------------
// notifyFileRecalled
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::notifyFileRecalled(
  const Cuuid_t        &cuuid,
  const uint32_t       volReqId,
  const char           *clientHost,
  const unsigned short clientPort,
  const uint64_t       fileTransactionId,
  const char           (&nsHost)[CA_MAXHOSTNAMELEN+1],
  const uint64_t       fileId,
  const int32_t        tapeFileSeq,
  const char           (&filePath)[CA_MAXPATHLEN+1],
  const int32_t        positionCommandCode,
  const char           (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1],
  const uint32_t       checksum,
  const uint64_t       fileSize)
  throw(castor::exception::Exception) {

  {
    // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
    char checksumHex[11];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    utils::toHex(checksum, &checksumHex[2], 9);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("clientHost"         , clientHost         ),
      castor::dlf::Param("clientPort"         , clientPort         ),
      castor::dlf::Param("fileTransactionId"  , fileTransactionId  ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksumHex        ),
      castor::dlf::Param("filePath"           , filePath           ),
      castor::dlf::Param("fileSize"           , fileSize           )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFY_CLIENT_FILE_RECALLED, params);
  }

  // Prepare the request
  tapegateway::FileRecalledNotification request;
  request.setMountTransactionId(volReqId);
  request.setFileTransactionId(fileTransactionId);
  request.setNshost(nsHost);
  request.setFileid(fileId);
  request.setFseq(tapeFileSeq);
  request.setPath(filePath);
  request.setPositionCommandCode(
    (tapegateway::PositionCommandCode)positionCommandCode);
  request.setChecksumName(checksumAlgorithm);
  request.setChecksum(checksum);
  request.setFileSize(fileSize);

  const char *requestTypeName = "FileRecalledNotification";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    // 32-bits = 1 x '0' + 1 x 'x' + 8 x hex + 1 x '/0' = 11 byte string
    char checksumHex[11];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    utils::toHex(checksum, &checksumHex[2], 9);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("clientHost"         , clientHost         ),
      castor::dlf::Param("clientPort"         , clientPort         ),
      castor::dlf::Param("fileTransactionId"  , fileTransactionId  ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksumHex        ),
      castor::dlf::Param("fileSize"           , fileSize           )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_CLIENT_FILE_RECALLED, params);
  }
}



//-----------------------------------------------------------------------------
// notifyEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::notifyEndOfSession(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFY_CLIENT_END_OF_SESSION, params);
  }

  // Prepare the request
  tapegateway::EndNotification request;
  request.setMountTransactionId(volReqId);

  const char *requestTypeName = "EndNotification";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_CLIENT_END_OF_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// getDumpParameters
//-----------------------------------------------------------------------------
castor::tape::tapegateway::DumpParameters
  *castor::tape::aggregator::ClientTxRx::getDumpParameters(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_GET_DUMP_PARAMETERS_FROM_CLIENT, params);
  }

  // Prepare the request
  tapegateway::DumpParametersRequest request;
  request.setMountTransactionId(volReqId);

  // Send the request and receive the reply
  time_t connectDuration  = 0;
  time_t sendRecvDuration = 0;
  std::auto_ptr<castor::IObject> obj(sendRequestAndReceiveReply(
    "DumpParametersRequest", clientHost, clientPort, CLIENTNETRWTIMEOUT,
    request, connectDuration, sendRecvDuration));

  switch(obj->type()) {
  case OBJ_DumpParameters:
    {
      // Down cast the reply to its specific class
      tapegateway::DumpParameters *reply =
        dynamic_cast<tapegateway::DumpParameters*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::DumpParameters");
      }

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_GOT_DUMP_PARAMETERS_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      checkMountTransactionId(volReqId, reply->mountTransactionId());

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(volReqId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  return NULL;
}


//-----------------------------------------------------------------------------
// notifyDumpMessage
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::notifyDumpMessage(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort, const char (&message)[CA_MAXLINELEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort),
      castor::dlf::Param("message"   , message   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFY_CLIENT_DUMP_MESSAGE, params);
  }

  // Prepare the request
  tapegateway::DumpNotification request;
  request.setMountTransactionId(volReqId);
  request.setMessage(message);

  const char *requestTypeName = "DumpNotification";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort),
      castor::dlf::Param("message"    , message  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFIED_CLIENT_DUMP_MESSAGE, params);
  }
}


//-----------------------------------------------------------------------------
// notifyEndOfFailedSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::notifyEndOfFailedSession(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort, castor::exception::Exception &ex)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
  }

  // Prepare the request
  tapegateway::EndNotificationErrorReport request;
  request.setMountTransactionId(volReqId);
  request.setErrorCode(ex.code());
  request.setErrorMessage(ex.getMessage().str());

  const char *requestTypeName = "EndNotificationErrorReport";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"          , volReqId             ),
      castor::dlf::Param("clientHost"        , clientHost           ),
      castor::dlf::Param("clientPort"        , clientPort           ),
      castor::dlf::Param("errorCode"         , ex.code()            ),
      castor::dlf::Param("errorrMessage"     , ex.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_CLIENT_END_OF_FAILED_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// ping
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::ping(const Cuuid_t &cuuid,
  const uint32_t volReqId, const char *clientHost,
  const unsigned short clientPort) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      AGGREGATOR_PING_CLIENT, params);
  }

  // Prepare the request
  tapegateway::PingNotification request;
  request.setMountTransactionId(volReqId);

  const char *requestTypeName = "PingNotification";
  notifyClient(cuuid, volReqId, requestTypeName, clientHost, clientPort,
    CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"  , volReqId  ),
      castor::dlf::Param("clientHost", clientHost),
      castor::dlf::Param("clientPort", clientPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_PINGED_CLIENT, params);
  }
}


//-----------------------------------------------------------------------------
// sendRequestAndReceiveReply
//-----------------------------------------------------------------------------
castor::IObject
  *castor::tape::aggregator::ClientTxRx::sendRequestAndReceiveReply(
  const char           *requestTypeName,
  const char           *clientHost,
  const unsigned short clientPort,
  const int            clientNetRWTimeout,
  IObject              &request,
  time_t               &connectDuration,
  time_t               &sendRecvDuration)
  throw(castor::exception::Exception) {

  // Connect to the client
  castor::io::ClientSocket sock(clientPort, clientHost);
  sock.setTimeout(clientNetRWTimeout);
  try {
    const size_t connectStartTime = time(NULL);
    sock.connect();
    connectDuration = time(NULL) - connectStartTime;
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to client to send " << requestTypeName <<
      ": " << ex.getMessage().str());
  }

  // Send the request
  const size_t sendAndReceiveStartTime = time(NULL);
  try {
    sock.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send "  << requestTypeName << " to client"
      ": " << ex.getMessage().str());
  }

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope
  std::auto_ptr<castor::IObject> obj;
  try {
    obj.reset(sock.readObject());

    if(obj.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
    sendRecvDuration = time(NULL) - sendAndReceiveStartTime;
  } catch(castor::exception::Exception &ex) {

    std::stringstream oss;

    oss << ": Failed to read " <<  requestTypeName << " reply from client: "
      << ex.getMessage().str();

    if(ex.code() == SETIMEDOUT) {
      oss << ": Timed out after " << sock.timeout() << " seconds";
    }

    TAPE_THROW_CODE(ex.code(), oss.str());
  }

  // Close the connection to the client
  sock.close();

  return(obj.release());
}


//-----------------------------------------------------------------------------
// notifyClient
//-----------------------------------------------------------------------------
void  castor::tape::aggregator::ClientTxRx::notifyClient(
  const Cuuid_t        &cuuid,
  const uint32_t       volReqId,
  const char           *requestTypeName,
  const char           *clientHost,
  const unsigned short clientPort,
  const int            clientNetRWTimeout,
  IObject              &request)
  throw(castor::exception::Exception) {

  // Send the request and receive the reply
  time_t connectDuration  = 0;
  time_t sendRecvDuration = 0;
  std::auto_ptr<castor::IObject> obj(sendRequestAndReceiveReply(
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT,
    request, connectDuration, sendRecvDuration));

  switch(obj->type()) {
  case OBJ_NotificationAcknowledge:
    {
      // Down cast the reply to its specific class
      tapegateway::NotificationAcknowledge *reply =
        dynamic_cast<tapegateway::NotificationAcknowledge*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to "
          "tapegateway::NotificationAcknowledge");
      }

      checkMountTransactionId(volReqId, reply->mountTransactionId());
    }
    break;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(volReqId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())
}


//-----------------------------------------------------------------------------
// throwEndNotificationErrorReport
//-----------------------------------------------------------------------------
void  castor::tape::aggregator::ClientTxRx::throwEndNotificationErrorReport(
  const uint32_t volReqId, IObject *const obj)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  tapegateway::EndNotificationErrorReport *reply =
    dynamic_cast<tapegateway::EndNotificationErrorReport*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to "
      "tapegateway::EndNotificationErrorReport");
  }

  checkMountTransactionId(volReqId, reply->mountTransactionId());

  // Translate the reception of the error report into a C++ exception
  TAPE_THROW_CODE(reply->errorCode(),
     ": Client error report "
     ": " << reply->errorMessage());
}


//-----------------------------------------------------------------------------
// checkMountTransactionId
//-----------------------------------------------------------------------------
void castor::tape::aggregator::ClientTxRx::checkMountTransactionId(
  const uint32_t expectedId, const uint32_t actualId)
  throw(castor::exception::Exception) {

  // Throw an exception if there is a transaction ID mismatch
  if(expectedId != actualId) {
    TAPE_THROW_CODE(EBADMSG,
      ": Transaction ID mismatch"
      ": Expected = " <<  expectedId
      << ": Actual = " << actualId);
  }
}
