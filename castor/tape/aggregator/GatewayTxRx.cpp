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
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/ErrorReport.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"


//-----------------------------------------------------------------------------
// getVolumeFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getVolumeFromGateway(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t volReqId, char (&vid)[CA_MAXVIDLEN+1], uint32_t &mode,
  char (&label)[CA_MAXLBLTYPLEN+1], char (&density)[CA_MAXDENLEN+1])
  throw(castor::exception::Exception) {

  bool thereIsAVolumeToMount = false;

  // Prepare the request
  tapegateway::VolumeRequest request;
  request.setVdqmVolReqId(volReqId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  gatewaySocket.connect();

  // Send the request
  gatewaySocket.sendObject(request);

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope
  std::auto_ptr<castor::IObject> reply(gatewaySocket.readObject());

  // Close the connection to the tape gateway
  gatewaySocket.close();

  if(reply.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get reply object from the tape gateway"
         ": ClientSocket::readObject() returned null";

    throw ex;
  }

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t transactionIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_Volume:
    // Copy the reply information
    try {
      tapegateway::Volume &volume = dynamic_cast<tapegateway::Volume&>(*reply);
      transactionIdFromGateway = volume.transactionId();
      Utils::copyString(vid, volume.vid());
      mode = volume.mode();
      Utils::copyString(label, volume.label());
      Utils::copyString(density, volume.density());
    } catch(std::bad_cast &bc) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::Volume";

      throw ex;
    }

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != volReqId) {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
    }

    thereIsAVolumeToMount = true;
    break;

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      transactionIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::NoMoreFiles";

      throw ex;
    }

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != volReqId) {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"
           ": Expected = " << volReqId
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
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
        transactionIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();
        errorMessage = errorReport.errorMessage();
      } catch(std::bad_cast &bc) {
        castor::exception::Internal ex;

        ex.getMessage() << __PRETTY_FUNCTION__
          << "Failed to down cast reply object to tapegateway::NoMoreFiles";

        throw ex;
      }

      // If there is a transaction ID mismatch
      if(transactionIdFromGateway != volReqId) {
        castor::exception::Exception ex(EINVAL);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Transaction ID mismatch"
             ": Expected = " << volReqId
          << ": Actual = " << transactionIdFromGateway;

        throw ex;
      }

      // Translate the reception of the error report into a C++ exception
      castor::exception::Exception ex(errorCode);

      ex.getMessage() << errorMessage;

      throw ex;
    }
    break;
  default:
    {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Unknown reply type "
           ": Reply type = " << reply->type();

      throw ex;
    }
  }

  return thereIsAVolumeToMount;
}


//-----------------------------------------------------------------------------
// getFileToMigrateFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getFileToMigrateFromGateway(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t transactionId, char (&filePath)[CA_MAXPATHLEN+1],
  char (&recordFormat)[CA_MAXRECFMLEN+1], char (&nsHost)[CA_MAXHOSTNAMELEN],
  uint64_t &fileId, uint32_t &tapeFileSeq, uint64_t &fileSize,
  char (&lastKnownFileName)[CA_MAXPATHLEN+1], uint64_t &lastModificationTime)
  throw(castor::exception::Exception) {

  bool thereIsAFileToMigrate = false;

  // Prepare the request
  tapegateway::FileToMigrateRequest request;
  request.setTransactionId(transactionId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  gatewaySocket.connect();

  // Send the request
  gatewaySocket.sendObject(request);

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope
  std::auto_ptr<castor::IObject> reply(gatewaySocket.readObject());

  // Close the connection to the tape gateway
  gatewaySocket.close();

  if(reply.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get reply object from the tape gateway"
         ": ClientSocket::readObject() returned null";

    throw ex;
  }

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t transactionIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_FileToMigrate:
    // Copy the reply information
    try {
      tapegateway::FileToMigrate &file =
        dynamic_cast<tapegateway::FileToMigrate&>(*reply);
      transactionIdFromGateway = file.transactionId();
      Utils::copyString(filePath, file.path());
      Utils::copyString(recordFormat, file.recordFormat());
      Utils::copyString(nsHost, file.nshost());
      fileId = file.fileid();
      tapeFileSeq = file.fseq();
      fileSize = file.fileSize();
      Utils::copyString(lastKnownFileName, file.lastKnownFileName());
      lastModificationTime = file.lastModificationTime();
    } catch(std::bad_cast &bc) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::FileToMigrate";

      throw ex;
    }

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != transactionId) {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"
           ": Expected = " << transactionId
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
    }

    thereIsAFileToMigrate = true;
    break;

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      transactionIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::NoMoreFiles";

      throw ex;
    }

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != transactionId) {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"
           ": Expected = " << transactionId
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
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
        transactionIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();
        errorMessage = errorReport.errorMessage();
      } catch(std::bad_cast &bc) {
        castor::exception::Internal ex;

        ex.getMessage() << __PRETTY_FUNCTION__
          << "Failed to down cast reply object to tapegateway::NoMoreFiles";

        throw ex;
      }

      // If there is a transaction ID mismatch
      if(transactionIdFromGateway != transactionId) {
        castor::exception::Exception ex(EINVAL);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Transaction ID mismatch"
             ": Expected = " << transactionId
          << ": Actual = " << transactionIdFromGateway;

        throw ex;
      }

      // Translate the reception of the error report into a C++ exception
      castor::exception::Exception ex(errorCode);

      ex.getMessage() << errorMessage;

      throw ex;
    }
    break;
  default:
    {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Unknown reply type "
           ": Reply type = " << reply->type();

      throw ex;
    }
  }

  return thereIsAFileToMigrate;
}


//-----------------------------------------------------------------------------
// getFileToRecallFromGateway
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::GatewayTxRx::getFileToRecallFromGateway(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t transactionId, char (&filePath)[CA_MAXPATHLEN+1],
  char (&recordFormat)[CA_MAXRECFMLEN+1], char (&nsHost)[CA_MAXHOSTNAMELEN],
  uint64_t &fileId, uint32_t &tapeFileSeq) throw(castor::exception::Exception) {

  bool thereIsAFileToRecall = false;

  // Prepare the request
  tapegateway::FileToRecallRequest request;
  request.setTransactionId(transactionId);  

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  gatewaySocket.connect();                                         

  // Send the request
  gatewaySocket.sendObject(request);

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope                  
  std::auto_ptr<castor::IObject> reply(gatewaySocket.readObject());       

  // Close the connection to the tape gateway
  gatewaySocket.close();

  if(reply.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get reply object from the tape gateway"
         ": ClientSocket::readObject() returned null";       

    throw ex;
  }          

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t transactionIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_FileToRecall:
    // Copy the reply information
    try {                        
      tapegateway::FileToRecall &file =
        dynamic_cast<tapegateway::FileToRecall&>(*reply);
      transactionIdFromGateway = file.transactionId();    
      Utils::copyString(filePath, file.path());           
      Utils::copyString(recordFormat, file.recordFormat());
      Utils::copyString(nsHost, file.nshost());            
      fileId = file.fileid();                              
      tapeFileSeq = file.fseq();                           
    } catch(std::bad_cast &bc) {                                     
      castor::exception::Internal ex;                                

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::FileToRecall";

      throw ex;
    }          

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != transactionId) {
      castor::exception::Exception ex(EINVAL);     

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"      
           ": Expected = " << transactionId 
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
    }          

    thereIsAFileToRecall = true;
    break;                       

  case OBJ_NoMoreFiles:
    // Copy the reply information
    try {                        
      tapegateway::NoMoreFiles &noMoreFiles =
        dynamic_cast<tapegateway::NoMoreFiles&>(*reply);
      transactionIdFromGateway = noMoreFiles.transactionId();
    } catch(std::bad_cast &bc) {                             
      castor::exception::Internal ex;                        

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to tapegateway::NoMoreFiles";

      throw ex;
    }          

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != transactionId) {
      castor::exception::Exception ex(EINVAL);     

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"      
           ": Expected = " << transactionId 
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
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
        transactionIdFromGateway = errorReport.transactionId();
        errorCode = errorReport.errorCode();                   
        errorMessage = errorReport.errorMessage();             
      } catch(std::bad_cast &bc) {                             
        castor::exception::Internal ex;                        

        ex.getMessage() << __PRETTY_FUNCTION__
          << "Failed to down cast reply object to tapegateway::NoMoreFiles";

        throw ex;
      }          

      // If there is a transaction ID mismatch
      if(transactionIdFromGateway != transactionId) {
        castor::exception::Exception ex(EINVAL);     

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Transaction ID mismatch"      
             ": Expected = " << transactionId 
          << ": Actual = " << transactionIdFromGateway;

        throw ex;
      }          

      // Translate the reception of the error report into a C++ exception
      castor::exception::Exception ex(errorCode);                        

      ex.getMessage() << errorMessage;

      throw ex;
    }          
    break;     
  default:     
    {          
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Unknown reply type "          
           ": Reply type = " << reply->type();

      throw ex;
    }          
  }

  return thereIsAFileToRecall;
}


//-----------------------------------------------------------------------------
// notifyGatewayOfFileMigrated
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayOfFileMigrated(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t transactionId) throw(castor::exception::Exception) {
}


//-----------------------------------------------------------------------------
// notifyGatewayOfFileRecalled
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayOfFileRecalled(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t transactionId) throw(castor::exception::Exception) {
}


//-----------------------------------------------------------------------------
// notifyGatewayOfEnd
//-----------------------------------------------------------------------------
void castor::tape::aggregator::GatewayTxRx::notifyGatewayOfEnd(
  const std::string gatewayHost, const unsigned short gatewayPort,
  const uint32_t transactionId) throw(castor::exception::Exception) {

  // Prepare the request
  tapegateway::EndNotification request;
  request.setTransactionId(transactionId);

  // Connect to the tape gateway
  castor::io::ClientSocket gatewaySocket(gatewayPort, gatewayHost);
  gatewaySocket.connect();

  // Send the request
  gatewaySocket.sendObject(request);

  // Receive the reply object wrapping it in an auto pointer so that it is
  // automatically deallocated when it goes out of scope
  std::auto_ptr<castor::IObject> reply(gatewaySocket.readObject());

  // Close the connection to the tape gateway
  gatewaySocket.close();

  if(reply.get() == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Failed to get reply object from the tape gateway"
         ": ClientSocket::readObject() returned null";

    throw ex;
  }

  // Temporary variable used to check for a transaction ID mistatch
  uint64_t transactionIdFromGateway = 0;

  switch(reply->type()) {
  case OBJ_NotificationAcknowledge:
    // Copy the reply information
    try {
      tapegateway::NotificationAcknowledge &notificationAcknowledge =
        dynamic_cast<tapegateway::NotificationAcknowledge&>(*reply);
      transactionIdFromGateway = notificationAcknowledge.transactionId();
    } catch(std::bad_cast &bc) {
      castor::exception::Internal ex;

      ex.getMessage() << __PRETTY_FUNCTION__
        << "Failed to down cast reply object to "
           "tapegateway::NotificationAcknowledge";

      throw ex;
    }

    // If there is a transaction ID mismatch
    if(transactionIdFromGateway != transactionId) {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Transaction ID mismatch"
           ": Expected = " << transactionId
        << ": Actual = " << transactionIdFromGateway;

      throw ex;
    }
    break;

  default:
    {
      castor::exception::Exception ex(EINVAL);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Unknown reply type "
           ": Reply type = " << reply->type();

      throw ex;
    }
  }
}
