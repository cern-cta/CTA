/******************************************************************************
 *                      WorkerThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: WorkerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/tape/tapegateway/WorkerThread.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"

#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"

#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"

#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"

#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/ErrorReport.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"

#include <getconfent.h>
#include <u64subr.h>
#include <sstream>
#include <errno.h>

#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"



//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

castor::tape::tapegateway::WorkerThread::WorkerThread(castor::tape::tapegateway::ITapeGatewaySvc* dbSvc):BaseDbThread(){ 
  m_dbSvc=dbSvc; 
  // populate the map with the different handlers
  m_handlerMap[OBJ_VolumeRequest] = &WorkerThread::handleStartWorker;
  m_handlerMap[OBJ_FileRecalledNotification] = &WorkerThread::handleRecallUpdate;
  m_handlerMap[OBJ_FileMigratedNotification] = &WorkerThread::handleMigrationUpdate;
  m_handlerMap[OBJ_FileToMigrateRequest] = &WorkerThread::handleRecallMoreWork;
  m_handlerMap[OBJ_FileToRecallRequest] = &WorkerThread::handleMigrationMoreWork;
  m_handlerMap[OBJ_EndNotification] = &WorkerThread::handleEndWorker;

}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::WorkerThread::run(void* arg)
{

  // to comunicate with the tape aggragator

  castor::IObject* obj = NULL;
  castor::IObject* response = NULL;

  try{
    unsigned short port=0;
    unsigned long ip=0;
    
    // create a socket
    castor::io::ServerSocket* sock=(castor::io::ServerSocket*)arg; 

    // Retrieve info on the client for logging 

    try {
      sock->getPeerIp(port, ip);
    } catch(castor::exception::Exception e) {
      // "Exception caught : ignored" message
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 34, 2, params);
      
    }
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("Client IP", ip), 
       castor::dlf::Param("Client port",port)
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 34, 2, params);
    
    // get the incoming request

    try {
      
      obj = sock->readObject();

    } catch (castor::exception::Exception e) {
     
      // "Unable to read Request from socket" message
	  
      castor::dlf::Param params[] =
	{castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 40, 2, params);
      return;
    }
    
    if (obj == NULL){

      //object not valid
      if (obj) delete obj;
      obj=NULL;
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 41, 0, NULL);
      return;

    }

    //let's  call the proper handler 

    HandlerMap::iterator handlerItor = m_handlerMap.find(obj->type());
	
    if(handlerItor == m_handlerMap.end()) {
	  
      //object not valid
      if (obj) delete obj;
      obj=NULL;
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 41, 0, NULL);
      return;
    
    }
	
    Handler handler = handlerItor->second;
    
    // Dispatch the request to the appropriate handler
  
    response = (this->*handler)(sock,obj,m_dbSvc);

    // send back the proper response

    try {
      sock->sendObject(*response);
    }catch (castor::exception::Exception e){
      castor::dlf::Param params[] =
	{castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 47, 2, params);
    }
    
  } catch(...) {
    //castor one are trapped before 

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 68, 0, NULL);
    
  }

  if (obj) delete obj;
  if (response) delete response;  

  // Deliberately slow down processing times to force thread pool expansion.
  sleep(1);

}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleStartWorker(castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){

  // I received a start worker request

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 35, 0, NULL);
 
  Volume* response = NULL;
  VolumeRequest* startRequest = dynamic_cast<VolumeRequest*>(obj);
  if (0 == startRequest) {
	// "Invalid Request" message
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, NULL);
	ErrorReport* errorReport=new ErrorReport();
	errorReport->setErrorCode(EINVAL);
	errorReport->setErrorMessage("invalid object");
	return errorReport;
  }
  try {
    
    response = m_dbSvc->updateDbStartTape(startRequest); 
    
  } catch (castor::exception::Exception e){
    if (response) delete response;

    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("TapeGateway server not available because of database problems");
    return errorReport;
     
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 38, 2, params);
    
  }
  return response;
}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleRecallUpdate(castor::io::ServerSocket* sock,  castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){
      // I received a FileRecalledResponse
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 43, 0, NULL);
      
      FileRecalledNotification* fileRecalled = dynamic_cast<FileRecalledNotification*>(obj);
      if (0 == fileRecalled) {
	// "Invalid Request" message
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 41, 0, NULL);

	ErrorReport* errorReport=new ErrorReport();
	errorReport->setErrorCode(EINVAL);
	errorReport->setErrorMessage("invalid object");
	return errorReport;

      }

      NotificationAcknowledge* response = new NotificationAcknowledge();
      response->setTransactionId(fileRecalled->transactionId());

      if ( !fileRecalled->errorCode()){
	try {
	  // check the nameserver if no error was faced already happened
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.checkRecalledFile(fileRecalled);
 

	} catch (castor::exception::Exception e){
	  // nameserver error
	  fileRecalled->setErrorCode(e.code());
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 71, 0, NULL); 
	}
      }

      try {

	m_dbSvc->fileRecallUpdate(fileRecalled);
	
      } catch (castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
       
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 48, 2, params);
       
      }
      
      return response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleMigrationUpdate( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){
  
  // I received a FileMigratedResponse

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 53, 0, NULL);
  
  VmgrTapeGatewayHelper vmgrHelper;
  NsTapeGatewayHelper nsHelper;

  
  FileMigratedNotification* fileMigrated = dynamic_cast<FileMigratedNotification*>(obj);
  if (0 == fileMigrated) {

    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 51, 0, NULL);

    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;

  }
  
  NotificationAcknowledge* response = new NotificationAcknowledge();
  response->setTransactionId(fileMigrated->transactionId());

  if (!fileMigrated->errorCode()) {

    // update vmgr
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 60, 0, NULL);
    try {  
      
      vmgrHelper.updateTapeInVmgr(fileMigrated);

      // check if it is repack case
      std::string repackVid;
      try {
	
	repackVid=m_dbSvc->getRepackVid(fileMigrated);
      
	try {
	  if (repackVid.empty()) {
      
	    // update the name server (standard migration)
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 63, 0, NULL);
	    nsHelper.updateMigratedFile(fileMigrated);
	  } else {
	    
	    // update the name server (repacked file)
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 65, 0, NULL);
	    nsHelper.updateRepackedFile(fileMigrated,repackVid);
	  }

	} catch (castor::exception::Exception e) {
	  fileMigrated->setErrorCode(e.code());
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 64, 0, NULL);
	}
      
      } catch (castor::exception::Exception e){
	castor::dlf::Param params[] =
	  {castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
      
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 62, 2, params);
      
	fileMigrated->setErrorCode(e.code());
	
      }
    

    } catch (castor::exception::Exception e){
      // vmgr exception
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 61, 0, NULL);
      fileMigrated->setErrorCode(e.code());
    }

  }
  // update the db
  
  try {
    m_dbSvc->fileMigrationUpdate(fileMigrated);
  }catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 58, 2, params);
    
  }

  return response;
    
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork( castor::io::ServerSocket* sock, castor::IObject* obj,castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){

  // I received FileToRecallRequest
     
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 44, 0, NULL);

  FileToRecall* response=NULL;

  FileToRecallRequest* fileToRecall = dynamic_cast<FileToRecallRequest*>(obj);
  if (0 == fileToRecall) {
    // "Invalid Request" message

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 41, 0, NULL);

    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }


  try {
    response = m_dbSvc->fileToRecall(fileToRecall);
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
	};
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 49, 2, params);
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(e.code());
    errorReport->setErrorMessage(e.getMessage().str());
    errorReport->setTransactionId(fileToRecall->transactionId());
    return errorReport;
     
  }
    
  if (response  == NULL ) {
    // I don't have anything to recall I send a NoMoreFiles
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 45, 0, NULL);
    
    NoMoreFiles* noMore= new NoMoreFiles();
    noMore->setTransactionId(fileToRecall->transactionId());
    return noMore;
  }
  return response;
}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc) throw(){
       
  // I received FileToMigrateRequest
  // I get a file from the db
  // send a new file

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 54, 0, NULL);

  FileToMigrate* response=NULL;

  
  FileToMigrateRequest* fileToMigrate = dynamic_cast<FileToMigrateRequest*>(obj);
  if (0 == fileToMigrate) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 51, 0, NULL);
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }

 
  try {
      
    response=m_dbSvc->fileToMigrate(fileToMigrate);

  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 59, 2, params);
    
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(e.code());
    errorReport->setErrorMessage(e.getMessage().str());
    return errorReport;


  }
      
  if ( response == NULL ) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 55, 0, NULL);
    // I don't have anything to migrate I send an NoMoreFiles

    NoMoreFiles* noMore= new NoMoreFiles();
    noMore->setTransactionId(fileToMigrate->transactionId());
    return noMore;
  } 
 
  return response; // check went fine

}


castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){
  	// I received an EndTransferRequest, I send back an EndTransferResponse
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 52, 0, NULL);
	
	castor::stager::Tape* tape=NULL;
	
	
	EndNotification* endRequest = dynamic_cast<EndNotification*>(obj);
	if (0 == endRequest) {
	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, NULL);

	  ErrorReport* errorReport=new ErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  return errorReport;

	}

	try {
    
	  tape=m_dbSvc->updateDbEndTape(endRequest); 
    
	} catch (castor::exception::Exception e){
    
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 38, 2, params);
    
	}

	// release busy tape

	if (tape != NULL && tape->tpmode() == 1) {
	  try {
	    VmgrTapeGatewayHelper vmgrHelper;
	    vmgrHelper.resetBusyTape(tape);
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 66, 0, NULL);
	  
	   } catch (castor::exception::Exception e) {
	     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 67, 0, NULL);
	  }
	}

	NotificationAcknowledge* response = new NotificationAcknowledge();
	response->setTransactionId(endRequest->transactionId());
	
	return  response;

}
