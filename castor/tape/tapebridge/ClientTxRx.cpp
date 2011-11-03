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
#include "castor/tape/tapegateway/DumpNotification.hpp"
#include "castor/tape/tapegateway/DumpParametersRequest.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
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
  const char *const    clientHost,
  const unsigned short clientPort,
  const char (&unit)[CA_MAXUNMLEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
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
  timeval connectDuration  = {0, 0};
  timeval sendRecvDuration = {0, 0};
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
          ": Failed to get volume"
          ": Failed to down cast reply object to tapegateway::Volume");
      }

      try {
        checkTransactionIds("Volume",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to get volume"
          ": " << ex.getMessage().str());
      }

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
          ": Failed to get volume"
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      castor::dlf::Param params[] = {
        castor::dlf::Param("mountTransactionId", mountTransactionId     ),
        castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
        castor::dlf::Param("clientHost"        , clientHost             ),
        castor::dlf::Param("clientPort"        , clientPort             ),
        castor::dlf::Param("unit"              , unit                   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_GOT_NO_MORE_FILES_FROM_CLIENT, params);

      try {
        checkTransactionIds("NoMoreFiles",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to get volume"
          ": " << ex.getMessage().str());
      }
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Failed to get volume"
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// sendFilesToMigrateListRequest
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFilesToMigrateListRequest(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char *const    clientHost,
  const unsigned short clientPort,
  const uint64_t       maxFiles,
  const uint64_t       maxBytes,
  timeval              &connectDuration)
  throw(castor::exception::Exception) {

  // Check method arguments
  if(NULL == clientHost) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": clientHost is NULL");
  }
  if(maxFiles == 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": maxFiles is 0");
  }
  if(maxBytes == 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": maxBytes is 0");
  }

  // Prepare the request
  tapegateway::FilesToMigrateListRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
  request.setMaxFiles(maxFiles);
  request.setMaxBytes(maxBytes);

  // Send the request and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
      connectDuration);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FilesToMigrateListRequest"
      ": maxFiles=" << maxFiles << " maxBytes=" << maxBytes <<
      ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveFilesToMigrateListRequestReplyAndClose
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FilesToMigrateList
  *castor::tape::tapebridge::ClientTxRx::
  receiveFilesToMigrateListRequestReplyAndClose(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  const int      clientSock)
  throw(castor::exception::Exception) {

  std::auto_ptr<castor::IObject> obj(receiveReplyAndClose(clientSock,
    CLIENTNETRWTIMEOUT));

  const char *const task = "receive reply to FilesToMigrateListRequest and"
    " close connection";

  switch(obj->type()) {
  case OBJ_FilesToMigrateList:
    {
      // Down cast the reply to its specific class
      tapegateway::FilesToMigrateList *reply =
        dynamic_cast<tapegateway::FilesToMigrateList*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to " << task <<
          ": Failed to down cast reply object to"
          " tapegateway::FilesToMigrateList");
      }

      try {
        checkTransactionIds("FilesToMigrateList",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to " << task <<
          ": " << ex.getMessage().str());
      }

      // Check list contains at least 1 file
      if(0 == reply->filesToMigrate().size()) {
        TAPE_THROW_CODE(EBADMSG,
          ": Failed to " << task <<
          ": FilesToMigrateList message contains 0 files");
      }

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
          ": Failed to " << task <<
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      try {
        checkTransactionIds("NoMoreFiles",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to " << task <<
          ": " << ex.getMessage().str());
      }
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Failed to " << task <<
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// sendFilesToRecallListRequest
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientTxRx::sendFilesToRecallListRequest(
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char *const    clientHost,
  const unsigned short clientPort,
  const uint64_t       maxFiles,
  const uint64_t       maxBytes,
  timeval              &connectDuration)
  throw(castor::exception::Exception) {

  // Check method arguments
  if(NULL == clientHost) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": clientHost is NULL");
  }
  if(maxFiles == 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": maxFiles is 0");
  }
  if(maxBytes == 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": maxBytes is 0");
  }

  // Prepare the request
  tapegateway::FilesToRecallListRequest request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
  request.setMaxFiles(maxFiles);
  request.setMaxBytes(maxBytes);

  // Send the request and return the socket-descriptor of the connection
  try {
    return sendMessage(clientHost, clientPort, CLIENTNETRWTIMEOUT, request,
      connectDuration);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FilesToRecallListRequest"
      ": maxFiles=" << maxFiles << "maxBytes=" << maxBytes <<
      ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveFilesToRecallListRequestReplyAndClose
//-----------------------------------------------------------------------------
castor::tape::tapegateway::FilesToRecallList
  *castor::tape::tapebridge::ClientTxRx::
  receiveFilesToRecallListRequestReplyAndClose(
  const uint32_t mountTransactionId,
  const uint64_t aggregatorTransactionId,
  const int      clientSock)
  throw(castor::exception::Exception) {

  std::auto_ptr<castor::IObject> obj(receiveReplyAndClose(clientSock,
    CLIENTNETRWTIMEOUT));

  const char *const task = "receive reply to FilesToRecallListRequest and"
    " close connection";

  switch(obj->type()) {
  case OBJ_FilesToRecallList:
    {
      // Down cast the reply to its specific class
      tapegateway::FilesToRecallList *reply =
        dynamic_cast<tapegateway::FilesToRecallList*>(obj.get());

      if(reply == NULL) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to " << task <<
          ": Failed to down cast reply object to"
          " tapegateway::FilesToRecallList");
      }

      try {
        checkTransactionIds("FilesToRecallList",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to " << task <<
          ": " << ex.getMessage().str());
      }

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
          ": Failed to " << task <<
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      try {
        checkTransactionIds("NoMoreFiles",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to " << task <<
          ": " << ex.getMessage().str());
      }
    }
    return NULL;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Failed to " << task <<
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())

  // This line should never be reached
  return NULL;
}


//-----------------------------------------------------------------------------
// getDumpParameters
//-----------------------------------------------------------------------------
castor::tape::tapegateway::DumpParameters
  *castor::tape::tapebridge::ClientTxRx::getDumpParameters(
  const Cuuid_t        &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char *const    clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
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
  timeval connectDuration  = {0, 0};
  timeval sendRecvDuration = {0, 0};
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
          ": Failed to get dump parameters"
          ": Failed to down cast reply object to tapegateway::DumpParameters");
      }

      LogHelper::logMsg(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_GOT_DUMP_PARAMETERS_FROM_CLIENT, *reply, connectDuration,
        sendRecvDuration);

      try {
        checkTransactionIds("DumpParameters",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to get dump parameters"
          ": " << ex.getMessage().str());
      }

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
        ": Failed to get dump parameters"
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
  const char *const    clientHost,
  const unsigned short clientPort,
  const char           (&message)[CA_MAXLINELEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
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

  const char *const requestTypeName = "DumpNotification";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("message"           , message                )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFIED_CLIENT_DUMP_MESSAGE, params);
  }
}


//-----------------------------------------------------------------------------
// ping
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::ping(const Cuuid_t &cuuid,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char *const    clientHost,
  const unsigned short clientPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_PING_CLIENT, params);
  }

  // Prepare the request
  tapegateway::PingNotification request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);

  const char *const requestTypeName = "PingNotification";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
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
  const char *const    clientHost,
  const unsigned short clientPort,
  const int            clientNetRWTimeout,
  IObject              &message,
  timeval              &connectDuration)
  throw(castor::exception::Exception) {

  // Connect to the client
  castor::io::ClientSocket sock(clientPort, clientHost);
  sock.setTimeout(clientNetRWTimeout);
  try {
    timeval connectStartTime = {0, 0};
    timeval connectEndTime   = {0, 0};
    utils::getTimeOfDay(&connectStartTime, NULL);
    sock.connect();
    utils::getTimeOfDay(&connectEndTime, NULL);
    connectDuration = utils::timevalAbsDiff(connectStartTime, connectEndTime);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send message"
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
      ": Failed to send message"
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
        ": Failed to receive reply and close connection"
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {

    std::stringstream oss;

    oss <<
      ": Failed to receive reply and close connection"
      ": Failed to read reply from client"
      ": " << ex.getMessage().str();

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
  const char *const    requestTypeName,
  const char *const    clientHost,
  const unsigned short clientPort,
  const int            clientNetRWTimeout,
  IObject              &request,
  timeval              &connectDuration,
  timeval              &sendRecvDuration)
  throw(castor::exception::Exception) {

  // Connect to the client
  castor::io::ClientSocket sock(clientPort, clientHost);
  sock.setTimeout(clientNetRWTimeout);
  try {
    timeval connectStartTime = {0, 0};
    timeval connectEndTime   = {0, 0};
    utils::getTimeOfDay(&connectStartTime, NULL);
    sock.connect();
    utils::getTimeOfDay(&connectEndTime, NULL);
    connectDuration = utils::timevalAbsDiff(connectStartTime, connectEndTime);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send request and receive reply"
      ": Failed to connect to client to send " << requestTypeName <<
      ": " << ex.getMessage().str());
  }

  // Send the request
  timeval sendAndReceiveStartTime = {0, 0};
  timeval sendAndReceiveEndTime   = {0, 0};
  utils::getTimeOfDay(&sendAndReceiveStartTime, NULL);
  try {
    sock.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send request and receive reply"
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
        ": Failed to send request and receive reply"
        ": ClientSocket::readObject() returned null");
    }
    utils::getTimeOfDay(&sendAndReceiveEndTime, NULL);
    sendRecvDuration = utils::timevalAbsDiff(sendAndReceiveStartTime,
      sendAndReceiveEndTime);
  } catch(castor::exception::Exception &ex) {

    std::stringstream oss;

    oss <<
      ": Failed to send request and receive reply"
      ": Failed to read " <<  requestTypeName << " reply from client"
      ": " << ex.getMessage().str();

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
          ": Failed to receive notification reply and close connection"
          ": Failed to down cast reply object to "
          "tapegateway::NotificationAcknowledge");
      }

      try {
        checkTransactionIds("NotificationAcknowledge",
          mountTransactionId     , reply->mountTransactionId(),
          aggregatorTransactionId, reply->aggregatorTransactionId());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to receive notification reply and close connection"
          ": " << ex.getMessage().str());
      }
    }
    break;

  case OBJ_EndNotificationErrorReport:
    throwEndNotificationErrorReport(mountTransactionId,
      aggregatorTransactionId, obj.get());
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Failed to receive notification reply and close connection"
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())
}


//-----------------------------------------------------------------------------
// notifyClient
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientTxRx::notifyClient(
  const                Cuuid_t&,
  const uint32_t       mountTransactionId,
  const uint64_t       aggregatorTransactionId,
  const char *const    requestTypeName,
  const char *const    clientHost,
  const unsigned short clientPort,
  const int,
  IObject& request)
  throw(castor::exception::Exception) {

  // Send the request and receive the reply
  timeval connectDuration  = {0, 0};
  timeval sendRecvDuration = {0, 0};
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
          ": Failed to notify client"
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
        ": Failed to notify client"
        ": Unknown reply type "
        ": Reply type = " << obj->type());
    }
  } // switch(obj->type())
}


//-----------------------------------------------------------------------------
// throwEndNotificationErrorReport
//-----------------------------------------------------------------------------
void  castor::tape::tapebridge::ClientTxRx::throwEndNotificationErrorReport(
  const uint32_t, const uint64_t, IObject *const obj)
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
      ": Mount transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedMountTransactionId=" << expectedMountTransactionId <<
      " actualMountTransactionId=" << actualMountTransactionId;
  }

  if(expectedTapeBridgeTransactionId != actualTapeBridgeTransactionId) {
    nbErrors++;
    errorMessage <<
      ": Tape-bridge transaction ID mismatch"
      ": messageTypeName=" << messageTypeName <<
      " expectedTapeBridgeTransactionId=" << expectedTapeBridgeTransactionId <<
      " actualTapeBridgeTransactionId=" << actualTapeBridgeTransactionId;
  }
  if(nbErrors > 0) {
    TAPE_THROW_CODE(EBADMSG,
      errorMessage.str());
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
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
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
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_NOTIFIED_CLIENT_END_OF_SESSION, params);
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
  const int                    errorCode,
  const std::string            &errorMessage)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
  }

  // Prepare the request
  tapegateway::EndNotificationErrorReport request;
  request.setMountTransactionId(mountTransactionId);
  request.setAggregatorTransactionId(aggregatorTransactionId);
  request.setErrorCode(errorCode);
  request.setErrorMessage(errorMessage);

  const char *requestTypeName = "EndNotificationErrorReport";
  notifyClient(cuuid, mountTransactionId, aggregatorTransactionId,
    requestTypeName, clientHost, clientPort, CLIENTNETRWTIMEOUT, request);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", mountTransactionId     ),
      castor::dlf::Param("tapebridgeTransId" , aggregatorTransactionId),
      castor::dlf::Param("clientHost"        , clientHost             ),
      castor::dlf::Param("clientPort"        , clientPort             ),
      castor::dlf::Param("errorCode"         , errorCode              ),
      castor::dlf::Param("errorMessage"      , errorMessage           )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_NOTIFIED_CLIENT_END_OF_FAILED_SESSION, params);
  }
}
