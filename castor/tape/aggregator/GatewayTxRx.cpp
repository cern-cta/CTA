/******************************************************************************
 *                castor/tape/aggregator/GatewayTxRx.cpp
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
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/SynchronizedCounter.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/ErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/utils/utils.hpp"

#include <pthread.h>


/**
 * An emulated recall counter used for debugging purposes only.
 */
static castor::tape::aggregator::SynchronizedCounter emulatedRecallCounter(0);


//-----------------------------------------------------------------------------
// getVolumeFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getVolumeFromGateway(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort,
  char (&vid)[CA_MAXVIDLEN+1], uint32_t &mode,
  char (&label)[CA_MAXLBLTYPLEN+1], char (&density)[CA_MAXDENLEN+1])
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GET_VOLUME_FROM_GATEWAY, params);
  }

  bool thereIsAVolumeToMount = false;

  // Prepare the request
  tapegateway::VolumeRequest request;
  request.setVdqmVolReqId(volReqId);

  //===================================================
  // Hardcoded volume INFO
  #ifdef EMULATE_GATEWAY

    //volReqIdFromGateway = volReqId;
    utils::copyString(vid,     "I02011");
    //mode = 0;  // tpread
    mode = 1;  // tpwrite
    utils::copyString(label,   "aul");
    utils::copyString(density, "1000GC");
    return(true);

  #endif
  //===================================================

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send VolumeRequest"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send VolumeRequest to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read VolumeRequest reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_Volume:
    // Copy the reply information
    try {
      tapegateway::Volume &volume = dynamic_cast<tapegateway::Volume&>(*reply);
      volReqIdFromGateway = volume.transactionId();
      utils::copyString(vid, volume.vid().c_str());
      mode = volume.mode();
      utils::copyString(label, volume.label().c_str());
      utils::copyString(density, volume.density().c_str());
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::Volume");
    }

    thereIsAVolumeToMount = true;
    break;

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      volReqIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::NoMoreFiles");
    }
    break;

  case OBJ_EndNotificationErrorReport:
    {
      int errorCode;
      std::string errorMessage;

      // Copy the reply information
      try {
        tapegateway::EndNotificationErrorReport &errorReport =
          dynamic_cast<tapegateway::EndNotificationErrorReport&>(*reply);
        volReqIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();
        errorMessage = errorReport.errorMessage();
      } catch(std::bad_cast &bc) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to "
          "tapegateway::EndNotificationErrorReport");
      }

      // Translate the reception of the error report into a C++ exception
      TAPE_THROW_CODE(errorCode,
         ": Tape gateway error report "
         ": " << errorMessage);
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  if(thereIsAVolumeToMount) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("volReqIdFromGateway", volReqIdFromGateway),
      castor::dlf::Param("gatewayHost"        , gatewayHost        ),
      castor::dlf::Param("gatewayPort"        , gatewayPort        ),
      castor::dlf::Param("vid"                , vid                ),
      castor::dlf::Param("mode"               , mode               ),
      castor::dlf::Param("label"              , label              ),
      castor::dlf::Param("density"            , density            )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_VOLUME_FROM_GATEWAY, params);
  } else {

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_NO_VOLUME_FROM_GATEWAY, params);
  }

  // If there is a transaction ID mismatch
  if(volReqIdFromGateway != volReqId) {
    TAPE_THROW_CODE(EINVAL,
         ": Transaction ID mismatch"
         ": Expected = " << volReqId
      << ": Actual = " << volReqIdFromGateway);
  }

  return thereIsAVolumeToMount;
}


//-----------------------------------------------------------------------------
// getFileToMigrateFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getFileToMigrateFromGateway(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort, char (&filePath)[CA_MAXPATHLEN+1], 
  char (&nsHost)[CA_MAXHOSTNAMELEN+1], uint64_t &fileId, int32_t &tapeFileSeq,
  uint64_t &fileSize, char (&lastKnownFileName)[CA_MAXPATHLEN+1],
  uint64_t &lastModificationTime, int32_t &positionCommandCode)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort),
      castor::dlf::Param("positionCommandCode", positionCommandCode)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GET_FILE_TO_MIGRATE_FROM_GATEWAY, params);
  }
  //===================================================
  // Hardcoded volume INFO
  #ifdef EMULATE_GATEWAY
  
   switch(emulatedRecallCounter.next()) {
   case 1:
     {
       utils::copyString(filePath,
         "lxc2disk15.cern.ch:/tmp/nbessone/file1.txt");
       utils::copyString(nsHost, "castorns");
       fileId               = 111111111;
       tapeFileSeq          = 1;
       fileSize             = 1451640;
       utils::copyString(lastKnownFileName, "file1.txt");
       lastModificationTime = 1234567890;
       positionCommandCode  = 0;// TPPOSIT_FSEQ: position by file sequence number
       {
         castor::dlf::Param params[] = {
           castor::dlf::Param("volReqId"            , volReqId            ),
           castor::dlf::Param("gatewayHost"         , gatewayHost         ),
           castor::dlf::Param("gatewayPort"         , gatewayPort         ),
           castor::dlf::Param("filePath"            , filePath            ),
           castor::dlf::Param("nsHost"              , nsHost              ),
           castor::dlf::Param("fileId"              , fileId              ),
           castor::dlf::Param("tapeFileSeq"         , tapeFileSeq         ),
           castor::dlf::Param("fileSize"            , fileSize            ),
           castor::dlf::Param("lastKnownFileName"   , lastKnownFileName   ),
           castor::dlf::Param("lastModificationTime", lastModificationTime)};
         castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
           AGGREGATOR_GOT_FILE_TO_MIGRATE_FROM_GATEWAY, params);
       }  
       return(true);
     }
     break;

   case 2:
     {
       utils::copyString(filePath,
         "lxc2disk15.cern.ch:/tmp/nbessone/file1.txt");
       utils::copyString(nsHost, "castorns");
       fileId               = 222222222;
       tapeFileSeq          = 2;
       fileSize             = 1451640;
       utils::copyString(lastKnownFileName, "file2.txt");
       lastModificationTime = 9876543210;
       positionCommandCode  = 0;// TPPOSIT_FSEQ: position by file sequence number
       {
         castor::dlf::Param params[] = {
           castor::dlf::Param("volReqId"            , volReqId            ),
           castor::dlf::Param("gatewayHost"         , gatewayHost         ),
           castor::dlf::Param("gatewayPort"         , gatewayPort         ),
           castor::dlf::Param("filePath"            , filePath            ),
           castor::dlf::Param("nsHost"              , nsHost              ),
           castor::dlf::Param("fileId"              , fileId              ),
           castor::dlf::Param("tapeFileSeq"         , tapeFileSeq         ),
           castor::dlf::Param("fileSize"            , fileSize            ),
           castor::dlf::Param("lastKnownFileName"   , lastKnownFileName   ),
           castor::dlf::Param("lastModificationTime", lastModificationTime)};
         castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
           AGGREGATOR_GOT_FILE_TO_MIGRATE_FROM_GATEWAY, params);
       }
       return(true);
     }
     break;

   default:     
     { // No more files to migrate
       castor::dlf::Param params[] = {
         castor::dlf::Param("volReqId"   , volReqId   ),
         castor::dlf::Param("gatewayHost", gatewayHost),
         castor::dlf::Param("gatewayPort", gatewayPort)};
       castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
         AGGREGATOR_NO_MORE_FILES_TO_MIGRATE, params);

       return(false);
     }
   }// switch
 
  #endif
  //===================================================

  bool thereIsAFileToMigrate = false;

  // Prepare the request
  tapegateway::FileToMigrateRequest request;
  request.setTransactionId(volReqId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send FileToMigrateRequest"
      ": " << ex.getMessage().str());
  }

  // Send the request
  try {
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FileToMigrateRequest to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read FileToMigrateRequest reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_FileToMigrate:
    // Copy the reply information
    try {
      tapegateway::FileToMigrate &file =
        dynamic_cast<tapegateway::FileToMigrate&>(*reply);
      volReqIdFromGateway = file.transactionId();
      utils::copyString(filePath, file.path().c_str());
      utils::copyString(nsHost, file.nshost().c_str());
      fileId = file.fileid();
      tapeFileSeq = file.fseq();
      fileSize = file.fileSize();
      utils::copyString(lastKnownFileName, file.lastKnownFileName().c_str());
      lastModificationTime = file.lastModificationTime();
      positionCommandCode = file.positionCommandCode();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::FileToMigrate");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EBADMSG,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }

    thereIsAFileToMigrate = true;
    break;

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      volReqIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::NoMoreFiles");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EINVAL,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }
    break;

  case OBJ_ErrorReport:
    {
       int errorCode;
       std::string errorMessage;

      // Copy the reply information
      try {
        tapegateway::ErrorReport &errorReport =
          dynamic_cast<tapegateway::ErrorReport&>(*reply);
        volReqIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();
        errorMessage = errorReport.errorMessage();
      } catch(std::bad_cast &bc) {
        TAPE_THROW_EX(castor::exception::Internal,
          "Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      // If there is a transaction ID mismatch
      if(volReqIdFromGateway != volReqId) {
        TAPE_THROW_CODE(EBADMSG,
             ": Transaction ID mismatch"
             ": Expected = " << volReqId
          << ": Actual = " << volReqIdFromGateway);
      }

      // Translate the reception of the error report into a C++ exception
      TAPE_THROW_CODE(errorCode,
        ": Tape gateway error report "
        ": " << errorMessage);
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

 if(thereIsAFileToMigrate) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"            , volReqId            ),
      castor::dlf::Param("gatewayHost"         , gatewayHost         ),
      castor::dlf::Param("gatewayPort"         , gatewayPort         ),
      castor::dlf::Param("filePath"            , filePath            ),
      castor::dlf::Param("nsHost"              , nsHost              ),
      castor::dlf::Param("fileId"              , fileId              ),
      castor::dlf::Param("tapeFileSeq"         , tapeFileSeq         ),
      castor::dlf::Param("fileSize"            , fileSize            ),
      castor::dlf::Param("lastKnownFileName"   , lastKnownFileName   ),
      castor::dlf::Param("lastModificationTime", lastModificationTime)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_FILE_TO_MIGRATE_FROM_GATEWAY, params);

  } else {

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NO_MORE_FILES_TO_MIGRATE, params);

  }

  return thereIsAFileToMigrate;
}


//-----------------------------------------------------------------------------
// getFileToRecallFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getFileToRecallFromGateway(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort, char (&filePath)[CA_MAXPATHLEN+1],
  char (&nsHost)[CA_MAXHOSTNAMELEN+1], uint64_t &fileId, int32_t &tapeFileSeq,
  unsigned char (&blockId)[4], int32_t &positionCommandCode)
  throw(castor::exception::Exception) {

  castor::dlf::Param params[] = {
    castor::dlf::Param("volReqId"   , volReqId   ),
    castor::dlf::Param("gatewayHost", gatewayHost),
    castor::dlf::Param("gatewayPort", gatewayPort),
    castor::dlf::Param("positionCommandCode", positionCommandCode)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    AGGREGATOR_GET_FILE_TO_RECALL_FROM_GATEWAY, params);

  //===================================================
  // Hardcoded volume INFO
  #ifdef EMULATE_GATEWAY

    if(emulatedRecallCounter.next() <= 1){
      //volReqIdFromGateway = volReqId;
      utils::copyString(filePath,
        "lxc2disk15.cern.ch:/tmp/volume_I02000_file_n5");
      utils::copyString(nsHost, "castorns");
      fileId      = 320723286;
      tapeFileSeq = 5;
      blockId[0]  = 0;
      blockId[1]  = 0;
      blockId[2]  = 0;
      blockId[3]  = 41;
      positionCommandCode = 3; // TPPOSIT_BLKID: position by block id (locate)
      return(true);
    }
    else {
      return(false);
    } 

  #endif
  //===================================================

  bool thereIsAFileToRecall = false;

  // Prepare the request
  tapegateway::FileToRecallRequest request;
  request.setTransactionId(volReqId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send FileToRecallRequest"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FileToRecallRequest to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read FileToRecallRequest reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_FileToRecall:
    // Copy the reply information
    try {
      tapegateway::FileToRecall &file =
        dynamic_cast<tapegateway::FileToRecall&>(*reply);
      volReqIdFromGateway = file.transactionId();
      utils::copyString(filePath, file.path().c_str());
      utils::copyString(nsHost, file.nshost().c_str());
      fileId = file.fileid();
      tapeFileSeq = file.fseq();
      blockId[0] = file.blockId0();
      blockId[1] = file.blockId1();
      blockId[2] = file.blockId2();
      blockId[3] = file.blockId3();
      positionCommandCode = file.positionCommandCode();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::FileToRecall");
    }

    thereIsAFileToRecall = true;
    break;

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      volReqIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to tapegateway::NoMoreFiles");
    }

    break;

  case OBJ_ErrorReport:
    {
       int errorCode;
       std::string errorMessage;

      // Copy the reply information
      try {
        tapegateway::ErrorReport &errorReport =
          dynamic_cast<tapegateway::ErrorReport&>(*reply);
        volReqIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();
        errorMessage = errorReport.errorMessage();
      } catch(std::bad_cast &bc) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to down cast reply object to tapegateway::NoMoreFiles");
      }

      // Translate the reception of the error report into a C++ exception
      TAPE_THROW_CODE(errorCode,
        ": Tape gateway error report "
        ": " << errorMessage);
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  // If there is a transaction ID mismatch
  if(volReqIdFromGateway != volReqId) {
    TAPE_THROW_CODE(EBADMSG,
         ": Transaction ID mismatch"
         ": Expected = " << volReqId
      << ": Actual = " << volReqIdFromGateway);
  }

  if(thereIsAFileToRecall) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"            , volReqId   ),
      castor::dlf::Param("gatewayHost"         , gatewayHost),
      castor::dlf::Param("gatewayPort"         , gatewayPort),
      castor::dlf::Param("filePath"            , filePath   ),
      castor::dlf::Param("nsHost"              , nsHost     ),
      castor::dlf::Param("fileId"              , fileId     ),
      castor::dlf::Param("tapeFileSeq"         , tapeFileSeq),
      castor::dlf::Param("blockId[0]"          , blockId[0] ),
      castor::dlf::Param("blockId[1]"          , blockId[1] ),
      castor::dlf::Param("blockId[2]"          , blockId[2] ),
      castor::dlf::Param("blockId[3]"          , blockId[3] )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_GOT_FILE_TO_RECALL_FROM_GATEWAY, params);
  } else {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NO_MORE_FILES_TO_RECALL, params);
  }

  return thereIsAFileToRecall;
}


//-----------------------------------------------------------------------------
// notifyGatewayFileMigrated
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayFileMigrated(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort,
  char (&nsHost)[CA_MAXHOSTNAMELEN+1], uint64_t &fileId, int32_t &tapeFileSeq,
  unsigned char (&blockId)[4], int32_t positionCommandCode,
  char (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1], uint32_t checksum,
  uint32_t fileSize, uint32_t compressedFileSize )
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("gatewayHost"        , gatewayHost        ),
      castor::dlf::Param("gatewayPort"        , gatewayPort        ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("blockId[0]"         , blockId[0]         ),
      castor::dlf::Param("blockId[1]"         , blockId[1]         ),
      castor::dlf::Param("blockId[2]"         , blockId[2]         ),
      castor::dlf::Param("blockId[3]"         , blockId[3]         ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksum           ),
      castor::dlf::Param("fileSize"           , fileSize           ),
      castor::dlf::Param("compressedFileSize" , compressedFileSize )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFY_GATEWAY_FILE_MIGRATED, params);
  }

  //===================================================
  // Hardcoded Gateway reply
  #ifdef EMULATE_GATEWAY

    return;

  #endif
  //===================================================

  // Prepare the request
  tapegateway::FileMigratedNotification request;

  request.setTransactionId(volReqId);
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

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send FileMigratedNotification"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FileMigratedNotification to gateway"
      ": " << ex.getMessage().str());
  }

   std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read FileMigratedNotification reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_NotificationAcknowledge:
    // Copy the reply information
    try {
      tapegateway::NotificationAcknowledge &notificationAcknowledge =
        dynamic_cast<tapegateway::NotificationAcknowledge&>(*reply);
      volReqIdFromGateway = notificationAcknowledge.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to "
        "tapegateway::NotificationAcknowledge");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EBADMSG,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("gatewayHost"        , gatewayHost        ),
      castor::dlf::Param("gatewayPort"        , gatewayPort        ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("blockId[0]"         , blockId[0]         ),
      castor::dlf::Param("blockId[1]"         , blockId[1]         ),
      castor::dlf::Param("blockId[2]"         , blockId[2]         ),
      castor::dlf::Param("blockId[3]"         , blockId[3]         ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksum           ),
      castor::dlf::Param("fileSize"           , fileSize           ),
      castor::dlf::Param("compressedFileSize" , compressedFileSize )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_GATEWAY_FILE_MIGRATED, params);
  }
}




//-----------------------------------------------------------------------------
// notifyGatewayFileRecalled
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayFileRecalled(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort, char (&nsHost)[CA_MAXHOSTNAMELEN+1], 
  uint64_t &fileId, int32_t &tapeFileSeq,char (&filePath)[CA_MAXPATHLEN+1],
  int32_t positionCommandCode, char (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1],
  uint32_t checksum ) throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("gatewayHost"        , gatewayHost        ),
      castor::dlf::Param("gatewayPort"        , gatewayPort        ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksum           ),
      castor::dlf::Param("filePath"           , filePath           )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFY_GATEWAY_FILE_RECALLED, params);
  }

  //===================================================
  // Hardcoded Gateway reply
  #ifdef EMULATE_GATEWAY

    return;

  #endif
  //===================================================

  // Prepare the request
  tapegateway::FileRecalledNotification request;
  request.setTransactionId(volReqId);
  request.setNshost(nsHost);
  request.setFileid(fileId);
  request.setFseq(tapeFileSeq);
  request.setPath(filePath);
  request.setPositionCommandCode(
    (tapegateway::PositionCommandCode)positionCommandCode);
  request.setChecksumName(checksumAlgorithm);
  request.setChecksum(checksum);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send FileRecalledNotification"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send FileRecalledNotification to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;

  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read FileRecalledNotification reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_NotificationAcknowledge:
    // Copy the reply information
    try {
      tapegateway::NotificationAcknowledge &notificationAcknowledge =
        dynamic_cast<tapegateway::NotificationAcknowledge&>(*reply);
      volReqIdFromGateway = notificationAcknowledge.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to "
        "tapegateway::NotificationAcknowledge");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EBADMSG,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"           , volReqId           ),
      castor::dlf::Param("gatewayHost"        , gatewayHost        ),
      castor::dlf::Param("gatewayPort"        , gatewayPort        ),
      castor::dlf::Param("nsHost"             , nsHost             ),
      castor::dlf::Param("fileId"             , fileId             ),
      castor::dlf::Param("tapeFileSeq"        , tapeFileSeq        ),
      castor::dlf::Param("positionCommandCode", positionCommandCode),
      castor::dlf::Param("checksumAlgorithm"  , checksumAlgorithm  ),
      castor::dlf::Param("checksum"           , checksum           )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_GATEWAY_FILE_RECALLED, params);
  }
}



//-----------------------------------------------------------------------------
// notifyGatewayEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayEndOfSession(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFY_GATEWAY_END_OF_SESSION, params);
  }


  //===================================================
  // Hardcoded Gateway reply
  #ifdef EMULATE_GATEWAY

    return;

  #endif
  //===================================================

  // Prepare the request
  tapegateway::EndNotification request;
  request.setTransactionId(volReqId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send EndNotification"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send EndNotification to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read EndNotification reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_NotificationAcknowledge:
    // Copy the reply information
    try {
      tapegateway::NotificationAcknowledge &notificationAcknowledge =
        dynamic_cast<tapegateway::NotificationAcknowledge&>(*reply);
      volReqIdFromGateway = notificationAcknowledge.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to "
        "tapegateway::NotificationAcknowledge");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EBADMSG,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_GATEWAY_END_OF_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// notifyGatewayEndOfFailedSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayEndOfFailedSession(
  const Cuuid_t &cuuid, const uint32_t volReqId, const char *gatewayHost,
  const unsigned short gatewayPort, castor::exception::Exception &ex)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFY_GATEWAY_END_OF_FAILED_SESSION, params);
  }


  //===================================================
  // Hardcoded Gateway reply
  #ifdef EMULATE_GATEWAY

    return;

  #endif
  //===================================================

  // Prepare the request
  tapegateway::EndNotificationErrorReport request;
  request.setTransactionId(volReqId);
  request.setErrorCode(ex.code());
  request.setErrorMessage(ex.getMessage().str());

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  try {
    gatewaySocket.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to gateway to send EndNotificationErrorReport"
      ": " << ex.getMessage().str());
  }

  try {
    // Send the request
    gatewaySocket.sendObject(request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send EndNotificationErrorReport to gateway"
      ": " << ex.getMessage().str());
  }

  std::auto_ptr<castor::IObject> reply;
  try {
    // Receive the reply object wrapping it in an auto pointer so that it is
    // automatically deallocated when it goes out of scope
    reply.reset(gatewaySocket.readObject());

    if(reply.get() == NULL) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": ClientSocket::readObject() returned null");
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to read EndNotificationErrorReport reply from gateway"
      ": " << ex.getMessage().str());
  }

  // Close the connection to the tape gateway
  gatewaySocket.close();

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t volReqIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_NotificationAcknowledge:
    // Copy the reply information
    try {
      tapegateway::NotificationAcknowledge &notificationAcknowledge =
        dynamic_cast<tapegateway::NotificationAcknowledge&>(*reply);
      volReqIdFromGateway = notificationAcknowledge.transactionId();
    } catch(std::bad_cast &bc) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to down cast reply object to "
        "tapegateway::NotificationAcknowledge");
    }

    // If there is a transaction ID mismatch
    if(volReqIdFromGateway != volReqId) {
      TAPE_THROW_CODE(EBADMSG,
           ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << volReqIdFromGateway);
    }
    break;

  default:
    {
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown reply type "
        ": Reply type = " << reply->type());
    }
  } // switch(reply->type())

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"   , volReqId   ),
      castor::dlf::Param("gatewayHost", gatewayHost),
      castor::dlf::Param("gatewayPort", gatewayPort)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_NOTIFIED_GATEWAY_END_OF_FAILED_SESSION, params);
  }
}
