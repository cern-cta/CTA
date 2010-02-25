/******************************************************************************
 *                castor/tape/tapebridge/ClientTxRx.cpp
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
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/ClientTxRx.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/LogHelper.hpp"
#include "castor/tape/tapebridge/SynchronizedCounter.hpp"
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
#include <memory>


//------------------------------------------------------------------------------
// getVolume
//------------------------------------------------------------------------------
castor::tape::tapegateway::Volume
  *castor::tape::tapebridge::ClientTxRx::getVolume(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  const char (&unit)[CA_MAXUNMLEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("unit"              , unit                   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_GET_VOLUME_FROM_CLIENT, params);
  }

  // Prepare the request
  tapegateway::VolumeRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
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

      checkTransactionIds("Volume",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_GOT_VOLUME_FROM_CLIENT, *reply, connectDuration,
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
        castor::dlf::Param("mountTransactionId", mountTransactionId     ),
        castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
        castor::dlf::Param("clientHost"        , clientHost             ),
        castor::dlf::Param("clientPort"        , clientPort             ),
        castor::dlf::Param("unit"              , unit                   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_GOT_NO_MORE_FILES_FROM_CLIENT, params);

      checkTransactionIds("NoMoreFiles",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
// sendFileToMigrateRequest
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFileToMigrateRequest(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  time_t               &connectDuration)
  throw(castor::exception::Exception) {

  // Prepare the request
  tapegateway::FileToMigrateRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

  // Send the request and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
      connectDuration);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "Failed to send FileToMigrateRequest"
      ": " << ex.getMessage().str();

    throw(ce);
  }
}


//-----------------------------------------------------------------------------
// receiveFileToMigrateReplyAndClose
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FileToMigrate
  *castor::tape::tapebridge::ClientTxRx::receiveFileToMigrateReplyAndClose(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  const int      clientSock)
  throw(castor::exception::Exception) {

  std::auto_ptr<castor::IObject> obj(receiveReplyAndClose(clientSock,
    CLIENTNETRWTIMEOUT));

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

      checkTransactionIds("FileToMigrate",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());

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

      checkTransactionIds("NoMoreFiles",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
// sendFileToRecallRequest
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFileToRecallRequest(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  time_t               &connectDuration)
  throw(castor::exception::Exception) {

  // Prepare the request
  tapegateway::FileToRecallRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

  // Send the request and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
      connectDuration);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "Failed to send FileToRecallRequest"
      ": " << ex.getMessage().str();

    throw(ce);
  }
}


//-----------------------------------------------------------------------------
// receiveFileToRecallReplyAndClose
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FileToRecall
  *castor::tape::tapebridge::ClientTxRx::receiveFileToRecallReplyAndClose(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  const int      clientSock)
  throw(castor::exception::Exception) {

  std::auto_ptr<castor::IObject> obj(receiveReplyAndClose(clientSock,
    CLIENTNETRWTIMEOUT));

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

      checkTransactionIds("FileToRecall",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());

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

      checkTransactionIds("NoMoreFiles",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
// sendFileMigratedNotification
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFileMigratedNotification(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  time_t               &connectDuration,
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

  // Prepare the notification
  tapegateway::FileMigratedNotification notification;
  notification.setMountTransactionId(mountTransactionId);
  notification.setAggregatorTransactionId(aggregatorTransactionId);
  notification.setFileTransactionId(fileTransactionId);
  notification.setNshost(nsHost);
  notification.setFileid(fileId);
  notification.setBlockId0(blockId[0]); // block ID=4 integers
                                        // (little indian order)
  notification.setBlockId1(blockId[1]);
  notification.setBlockId2(blockId[2]);
  notification.setBlockId3(blockId[3]);
  notification.setFseq(tapeFileSeq);
  notification.setPositionCommandCode(
    (tapegateway::PositionCommandCode)positionCommandCode);
  notification.setChecksumName(checksumAlgorithm);
  notification.setChecksum(checksum);
  notification.setFileSize(fileSize);
  notification.setCompressedFileSize( compressedFileSize);

  // Send the notification and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT,
      notification, connectDuration);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "Failed to send FileMigratedNotification"
      ": " << ex.getMessage().str();

    throw(ce);
  }
}


//-----------------------------------------------------------------------------
// sendFileRecalledNotification
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFileRecalledNotification(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  time_t               &connectDuration,
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

  // Prepare the notification
  tapegateway::FileRecalledNotification notification;
  notification.setMountTransactionId(mountTransactionId);
  notification.setAggregatorTransactionId(aggregatorTransactionId);
  notification.setFileTransactionId(fileTransactionId);
  notification.setNshost(nsHost);
  notification.setFileid(fileId);
  notification.setFseq(tapeFileSeq);
  notification.setPath(filePath);
  notification.setPositionCommandCode(
    (tapegateway::PositionCommandCode)positionCommandCode);
  notification.setChecksumName(checksumAlgorithm);
  notification.setChecksum(checksum);
  notification.setFileSize(fileSize);

  // Send the notification and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT,
      notification, connectDuration);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "Failed to send FileRecalledNotification"
      ": " << ex.getMessage().str();

    throw(ce);
  }
}


//-----------------------------------------------------------------------------
// notifyEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::notifyEndOfSession(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFY_CLIENT_END_OF_SESSION, params);
  }

  // Prepare the request
  tapegateway::EndNotification request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

  const char *requestTypeName = "EndNotification";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_NOTIFIED_CLIENT_END_OF_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// getDumpParameters
//-----------------------------------------------------------------------------
castor::tape::tapegateway::DumpParameters
  *castor::tape::tapebridge::ClientTxRx::getDumpParameters(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_GET_DUMP_PARAMETERS_FROM_CLIENT, params);
  }

  // Prepare the request
  tapegateway::DumpParametersRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

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
        TAPEBRIDGE_GOT_DUMP_PARAMETERS_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      checkTransactionIds("DumpParameters",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());

      // Release the reply message from its smart pointer and return it
      obj.release();
      return reply;
    }

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
void castor::tape::tapebridge::ClientTxRx::notifyDumpMessage(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort,
  const char           (&message)[CA_MAXLINELEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("message"           , message                )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFY_CLIENT_DUMP_MESSAGE, params);
  }

  // Prepare the request
  tapegateway::DumpNotification request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
  request.setMessage(message);

  const char *requestTypeName = "DumpNotification";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("message"           , message                )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFIED_CLIENT_DUMP_MESSAGE, params);
  }
}


//-----------------------------------------------------------------------------
// notifyEndOfFailedSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::notifyEndOfFailedSession(
  const Cuuid_t                &cuuid,
  const uint32_t               mountTransactionId,
  const uint64_t               aggregatorTransactionId,
  const char                   *clientHost,
  const unsigned short         clientPort,
  castor::exception::Exception &ex)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
  }

  // Prepare the request
  tapegateway::EndNotificationErrorReport request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
  request.setErrorCode(ex.code());
  request.setErrorMessage(ex.getMessage().str());

  const char *requestTypeName = "EndNotificationErrorReport";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorrMessage"     , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_NOTIFIED_CLIENT_END_OF_FAILED_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// ping
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::ping(const Cuuid_t &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char           *clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_PING_CLIENT, params);
  }

  // Prepare the request
  tapegateway::PingNotification request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

  const char *requestTypeName = "PingNotification";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_PINGED_CLIENT, params);
  }
}


//-----------------------------------------------------------------------------
// sendMessage
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendMessage(
  const char           *clientHost,
  const unsigned short clientPort,
  const int            clientNetRWTimeout,
  IObject              &message,
  time_t               &connectDuration)
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
      ": Failed to connect to client"
      ": clientHost=" << clientHost <<
      ": clientPort=" << clientPort <<
      ": clientNetRWTimeout=" << clientNetRWTimeout <<
      ": " << ex.getMessage().str());
  }

  // Send the message
  try {
    sock.sendObject(message);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send request to client"
      ": clientHost=" << clientHost <<
      ": clientPort=" << clientPort <<
      ": clientNetRWTimeout=" << clientNetRWTimeout <<
      ": " << ex.getMessage().str());
  }

  // Release the socket-descriptor from the castor::io::ClientSocket object so
  // that it is not closed by its destructor
  const int socketDescriptor = sock.socket();
  sock.resetSocket();

  // Return the socket-descriptor
  return(socketDescriptor);
}


//-----------------------------------------------------------------------------
// receiveReplyAndClose
//-----------------------------------------------------------------------------
castor::IObject *castor::tape::tapebridge::ClientTxRx::receiveReplyAndClose(
  const int clientSock, const int clientNetRWTimeout)
  throw(castor::exception::Exception) {

  // Receive the reply object
  castor::io::AbstractTCPSocket sock(clientSock);
  sock.setTimeout(clientNetRWTimeout);
  std::auto_ptr<castor::IObject> obj;
  try {
    obj.reset(sock.readObject());

    if(obj.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {

    std::stringstream oss;

    oss << ": Failed to read reply from client: " << ex.getMessage().str();

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
// sendRequestAndReceiveReply
//-----------------------------------------------------------------------------
castor::IObject
  *castor::tape::tapebridge::ClientTxRx::sendRequestAndReceiveReply(
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
// receiveNotificationReplyAndClose
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::receiveNotificationReplyAndClose(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  const int      clientSock)
  throw(castor::exception::Exception) {

  std::auto_ptr<castor::IObject> obj(receiveReplyAndClose(clientSock,
    CLIENTNETRWTIMEOUT));

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

      checkTransactionIds("NotificationAcknowledge",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());
    }
    break;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
// notifyClient
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::notifyClient(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
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

      checkTransactionIds("NotificationAcknowledge",
        mountTransactionId     , reply->mountTransactionId(),
        aggregatorTransactionId, reply->aggregatorTransactionId());
    }
    break;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
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
void  castor::tape::tapebridge::ClientTxRx::throwEndNotificationErrorReport(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  IObject *const obj)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  tapegateway::EndNotificationErrorReport *reply =
    dynamic_cast<tapegateway::EndNotificationErrorReport*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to "
      "tapegateway::EndNotificationErrorReport");
  }

  // Translate the reception of the error report into a C++ exception
  TAPE_THROW_CODE(reply->errorCode(),
     ": Client error report "
     ": " << reply->errorMessage());
}


//-----------------------------------------------------------------------------
// checkTransactionIds
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::checkTransactionIds(
  const char *const messageTypeName,
  const uint32_t    expectedMountTransactionId,
  const uint32_t    actualMountTransactionId,
  const uint64_t    expectedTapeBridgeTransactionId,
  const uint64_t    actualTapeBridgeTransactionId)
  throw(castor::exception::Exception) {

  int nbErrors = 0;
  std::stringstream errorMessage;

  if(expectedMountTransactionId != actualMountTransactionId) {
    nbErrors++;
    errorMessage <<
      ": " << messageTypeName <<
      " message contains a mount transaction ID mismatch"
      ": expectedMountTransactionId=" << expectedMountTransactionId <<
      ": actualMountTransactionId=" << actualMountTransactionId;
  }

  if(expectedTapeBridgeTransactionId != actualTapeBridgeTransactionId) {
    nbErrors++;
    errorMessage <<
      ": " << messageTypeName <<
      " message contains an tapebridge transaction ID mismatch"
      ": expectedTapeBridgeTransactionId=" << expectedTapeBridgeTransactionId <<
      ": actualTapeBridgeTransactionId=" << actualTapeBridgeTransactionId;
  }
  if(nbErrors > 0) {
    TAPE_THROW_CODE(EBADMSG,
      errorMessage.str());
  }
}
