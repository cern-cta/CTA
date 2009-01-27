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

#include "castor/tape/tapegateway/StartWorkerResponse.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/FileToRecallResponse.hpp"
#include "castor/tape/tapegateway/FileRecalledResponse.hpp"

#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrateResponse.hpp"
#include "castor/tape/tapegateway/FileMigratedResponse.hpp"

#include "castor/tape/tapegateway/StartWorkerResponse.hpp"
#include "castor/tape/tapegateway/EndWorkerRequest.hpp"
#include "castor/tape/tapegateway/EndWorkerResponse.hpp"

#include "castor/tape/tapegateway/FileUpdateResponse.hpp"

#include <getconfent.h>
#include <u64subr.h>
#include <sstream>

#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"



//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

castor::tape::tapegateway::WorkerThread::WorkerThread(castor::tape::tapegateway::ITapeGatewaySvc* dbSvc):BaseDbThread(){ 
  m_dbSvc=dbSvc; 
  // populate the map with the different handlers
  m_handlerMap[OBJ_StartWorkerRequest] = &WorkerThread::handleStartWorker;
  m_handlerMap[OBJ_FileRecalledResponse] = &WorkerThread::handleRecallUpdate;
  m_handlerMap[OBJ_FileMigratedResponse] = &WorkerThread::handleMigrationUpdate;
  m_handlerMap[OBJ_FileToMigrateRequest] = &WorkerThread::handleRecallMoreWork;
  m_handlerMap[OBJ_FileToRecallRequest] = &WorkerThread::handleMigrationMoreWork;
  m_handlerMap[OBJ_EndWorkerRequest] = &WorkerThread::handleEndWorker;

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
 
  StartWorkerResponse* response = NULL;
  StartWorkerRequest* startRequest = dynamic_cast<StartWorkerRequest*>(obj);
  if (0 == startRequest) {
	// "Invalid Request" message
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, NULL);
	response = new StartWorkerResponse();
	response->setErrorCode(-1);
	response->setErrorMessage("invalid object");
	return response;
  }
  try {
    
    response = m_dbSvc->updateDbStartTape(startRequest); 
    
  } catch (castor::exception::Exception e){
    if (response) delete response;
    response=new StartWorkerResponse();
    response->setErrorCode(-1);
    response->setErrorMessage("TapeGateway server not available because of database problems");
     
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

      FileUpdateResponse* response = NULL;
      
      FileRecalledResponse* fileRecalled = dynamic_cast<FileRecalledResponse*>(obj);
      if (0 == fileRecalled) {
	// "Invalid Request" message
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 41, 0, NULL);
	response=new FileUpdateResponse();
	response->setErrorCode(-1);
	response->setErrorMessage("invalid object");
	return response;
      }
      if ( !fileRecalled->errorCode()){
     
	// check the nameserver if shit hasn't already happened
	NsTapeGatewayHelper nsHelper;
	int ret = nsHelper.checkRecalledFile(fileRecalled);
	if (ret<0) {
	  // nameserver error
	  fileRecalled->setErrorCode(ret);
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 71, 0, NULL); 
	}

      } else {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 71, 0, NULL); 
      }

      try {

	response = m_dbSvc->fileRecallUpdate(fileRecalled);

      } catch (castor::exception::Exception e) {
	castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
       
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 48, 2, params);
	if (response == NULL)
	  response = new FileUpdateResponse();
	response->setErrorCode(-1);
	response->setErrorMessage("impossible to update the db");
       
      }

      return response;
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleMigrationUpdate( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){
  
  // I received a FileMigratedResponse

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 53, 0, NULL);
  
  VmgrTapeGatewayHelper vmgrHelper;
  NsTapeGatewayHelper nsHelper;
  FileUpdateResponse* response = NULL; 

  
  FileMigratedResponse* fileMigrated = dynamic_cast<FileMigratedResponse*>(obj);
  if (0 == fileMigrated) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 51, 0, NULL);
    response= new FileUpdateResponse();
    response->setErrorCode(-1);
    response->setErrorMessage("invalid object");
    return response;
  }
  
  if (!fileMigrated->errorCode()) {

    // update vmgr
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 60, 0, NULL);
      
    int ret = vmgrHelper.updateTapeInVmgr(fileMigrated);
    
    if (ret < 0){
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 61, 0, NULL);
      fileMigrated->setErrorCode(-1);
      fileMigrated->setErrorMessage("vmgr update failed");

    } else {

      // check if it is repack case
      std::string repackVid;
      try {
	
	repackVid=m_dbSvc->getRepackVid(fileMigrated);
      
      } catch (castor::exception::Exception e){
	castor::dlf::Param params[] =
	  {castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
      
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 62, 2, params);
	response->setErrorCode(-1);
	response->setErrorMessage("db failure");
      
      }
      
      if (response->errorCode() == 0) {
	
	if (repackVid.empty()) {
      
	  // update the name server (standard migration)
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 63, 0, NULL);
	  ret=nsHelper.updateMigratedFile(fileMigrated);

	  if (ret <0) {
	    fileMigrated->setErrorCode(-1);
	    fileMigrated->setErrorMessage("ns update failed");
	    response->setErrorCode(-1);
	    response->setErrorMessage("ns update failed");
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 64, 0, NULL);
	  }

	} else {
	  
	  // update the name server (repacked file)
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 65, 0, NULL);
	  ret=nsHelper.updateRepackedFile(fileMigrated,repackVid);
	  if (ret <0) {
	    fileMigrated->setErrorCode(-1);
	    fileMigrated->setErrorMessage("ns update failed");
	    response->setErrorCode(-1);
	    response->setErrorMessage("ns update failed");
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 64, 0, NULL);
	  }
	
	}
      
      } else {
    
	fileMigrated->setErrorCode(-1);
	fileMigrated->setErrorMessage("check for repack failed");
	
      }

    }
  } else {
    
    // error reported
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 72, 0, NULL);
    
  }
      
  // update the db
  
  try {
    response = m_dbSvc->fileMigrationUpdate(fileMigrated);
  }catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 58, 2, params);
    if (response == NULL ) response = new FileUpdateResponse();  
    response->setErrorCode(-1);
    response->setErrorMessage("impossible to update the db");
  }

  return response;

}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork( castor::io::ServerSocket* sock, castor::IObject* obj,castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){

  // I received FileToRecallRequest
     
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 44, 0, NULL);

  FileToRecallResponse* response=NULL;

  FileToRecallRequest* fileToRecall = dynamic_cast<FileToRecallRequest*>(obj);
  if (0 == fileToRecall) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 41, 0, NULL);
    response->setErrorCode(-1);
    response->setErrorMessage("invalid object");
    return response;
  }

  while (1) { // loop to find a valid segment
    try {
      response = m_dbSvc->fileToRecall(fileToRecall);
    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
    
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 49, 2, params);
      response->setErrorCode(-1);
      response->setErrorMessage("impossible to contact the db");
    }
    
    if (response  == NULL ) {
      // I don't have anything to recall I send an EndWorkerRequest
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 45, 0, NULL);
      EndWorkerRequest* endResponse = new EndWorkerRequest();
      return endResponse;
    }
  
    // check the file in the nameserver to see that it is still there
    NsTapeGatewayHelper nsHelper;
    try {

      nsHelper.checkFileToRecall(response);
      break; // found a valid file

    }catch (castor::exception::Exception e) {
      try {
	m_dbSvc->invalidateSegment(response);
      } catch (castor::exception::Exception ex){

	// just log the error

	castor::dlf::Param params[] =
	{castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
    
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 49, 2, params);
	
      }
    }  
    // continue the loop the segment was not in the nameserver anylonger
  }
  
  return response;
}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc) throw(){
       
  // I received FileToMigrateRequest
  // I get a file from the db
  // send a new file

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 54, 0, NULL);

  FileToMigrateResponse* response=NULL;

  FileToMigrateRequest* fileToMigrate = dynamic_cast<FileToMigrateRequest*>(obj);
  if (0 == fileToMigrate) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 51, 0, NULL);
    response->setErrorCode(-1);
    response->setErrorMessage("invalid object");
    return response;
  }

  try {
      
    response=m_dbSvc->fileToMigrate(fileToMigrate);

  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 59, 2, params);
    response->setErrorCode(-1);
    response->setErrorMessage("impossible to contact the db");
  }
      
  if ( response == NULL ) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 55, 0, NULL);
    // I don't have anything to migrate I send an EndWorkerRequest
    EndWorkerRequest* endResponse = new EndWorkerRequest();
    return endResponse;
  } 
  return response;

}


castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker( castor::io::ServerSocket* sock, castor::IObject* obj, castor::tape::tapegateway::ITapeGatewaySvc* m_dbSvc ) throw(){
  	// I received an EndWorkerRequest, I send back an EndWorkerResponse
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 52, 0, NULL);
	
	castor::stager::Tape* tape=NULL;
	EndWorkerResponse* response = new EndWorkerResponse();
	
	EndWorkerRequest* endRequest = dynamic_cast<EndWorkerRequest*>(obj);
	if (0 == endRequest) {
	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, NULL);
	  response->setErrorCode(-1);
	  response->setErrorMessage("invalid object");
	  return response;
	}

	try {
    
	  tape=m_dbSvc->updateDbEndTape(endRequest); 
    
	} catch (castor::exception::Exception e){
    
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	  response->setErrorCode(-1);
	  response->setErrorMessage("dbError");
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 38, 2, params);
	  if (tape) delete tape;
	  tape=NULL;
    
	}


	// release busy tape

	if (tape != NULL && tape->tpmode() == 1) {
	  VmgrTapeGatewayHelper vmgrHelper;
	  int ret = vmgrHelper.resetBusyTape(tape);
	  if (ret<0) {
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 67, 0, NULL);
	    response->setErrorCode(ret);
	    response->setErrorMessage("cannot release the tape");

	  } else {
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 66, 0, NULL);
	  }
	}


	// TODO error codes nightmare in rtcpcld_workerFinished  and rc = rtcpcld_updateTape(tape,NULL, 0,workerErrorCode,NULL );
	

	return  response;

}
