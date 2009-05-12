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
#include "castor/tape/tapegateway/DlfCodes.hpp"

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
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/ErrorReport.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/FileErrorReport.hpp"

#include <getconfent.h>
#include <u64subr.h>
#include <sstream>
#include <errno.h>

#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"




//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

castor::tape::tapegateway::WorkerThread::WorkerThread():BaseDbThread(){ 

  
  // populate the map with the different handlers
  m_handlerMap[OBJ_VolumeRequest] = &WorkerThread::handleStartWorker;
  m_handlerMap[OBJ_FileRecalledNotification] = &WorkerThread::handleRecallUpdate;
  m_handlerMap[OBJ_FileMigratedNotification] = &WorkerThread::handleMigrationUpdate;
  m_handlerMap[OBJ_FileToRecallRequest] = &WorkerThread::handleRecallMoreWork;
  m_handlerMap[OBJ_FileToMigrateRequest] = &WorkerThread::handleMigrationMoreWork;
  m_handlerMap[OBJ_EndNotification] = &WorkerThread::handleEndWorker;
  m_handlerMap[OBJ_FileErrorReport] = &WorkerThread::handleFileErrorReport;

}



//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::WorkerThread::run(void* arg)
{

  // to comunicate with the tape aggregator
  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  

  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }


  try{

    unsigned short port=0;
    unsigned long ip=0;
    
    // create a socket

    std::auto_ptr<castor::io::ServerSocket> sock((castor::io::ServerSocket*)arg); 

    // Retrieve info on the client for logging 

    try {
      sock->getPeerIp(port, ip);
    } catch(castor::exception::Exception e) {
      // "Exception caught : ignored" message
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_UNKNOWN_CLIENT, 2, params);
      
    }
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("Client IP", ip), 
       castor::dlf::Param("Client port",port)
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MESSAGE_RECEIVED, 2, params);
    
    // get the incoming request

    try {
      std::auto_ptr<castor::IObject>  obj(sock->readObject());
    
      if (obj.get() == NULL){

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_REQUEST, 0, NULL);
	return;

      }

      //let's  call the proper handler 

      HandlerMap::iterator handlerItor = m_handlerMap.find(obj->type());
      
      if(handlerItor == m_handlerMap.end()) {
      
	//object not valid
   
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_INVALID_REQUEST, 0, NULL);
	return;
	
      }
	
      Handler handler = handlerItor->second;
    
      // Dispatch the request to the appropriate handler
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_DISPATCHING, 0, NULL);

      std::auto_ptr<castor::IObject> response( (this->*handler)(*obj,*oraSvc));

      // send back the proper response

      try {

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RESPONDING, 0, NULL);

	sock->sendObject(*response);

      }catch (castor::exception::Exception e){
	castor::dlf::Param params[] =
	  {castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_CANNOT_RESPOND, 2, params);
      }

    } catch (castor::exception::Exception e) {
      
      // "Unable to read Request from socket" message
      
      castor::dlf::Param params[] =
	{castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
	
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,WORKER_CANNOT_RECEIVE, 2, params);
      return;
    }
    
  } catch(...) {
    //castor one are trapped before 
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_UNKNOWN_EXCEPTION, 0, NULL);
    
  }

  // Deliberately slow down processing times to force thread pool expansion.
  sleep(1);

}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleStartWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

  // I received a start worker request
  Volume* response=NULL;

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_VOLUME_REQUESTED, 0, NULL);

  try{
    
    VolumeRequest &startRequest = dynamic_cast<VolumeRequest&>(obj);
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId", startRequest.vdqmVolReqId())
    };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_GETTING_VOLUME, 1, params);
    
    try {
    
      // GET FROM DB

      response = oraSvc.updateDbStartTape(startRequest); 
    
    } catch (castor::exception::Exception e){
    
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
    
      castor::dlf::Param params[] =
	{castor::dlf::Param("transactionId", startRequest.vdqmVolReqId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_NO_VOLUME, 3, params);
      return errorReport;
    }
     
    if (response  == NULL ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_NO_FILE, 1, params);
      
      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setTransactionId(startRequest.vdqmVolReqId());
      return noMore;
    }
    castor::dlf::Param params0[] =      
      {castor::dlf::Param("transactionId", startRequest.vdqmVolReqId()),
				       castor::dlf::Param("VID",response->vid()),
       castor::dlf::Param("mode",response->mode()),
       castor::dlf::Param("label",response->label()),
       castor::dlf::Param("density",response->density())
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_VOLUME_FOUND, 5, params0);

  } catch( std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }

  return response;

}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleRecallUpdate( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){
      // I received a FileRecalledResponse
  NotificationAcknowledge* response = NULL;

  try {

    FileRecalledNotification &fileRecalled = dynamic_cast<FileRecalledNotification&>(obj);

    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId",fileRecalled.transactionId()),
       castor::dlf::Param("NSHOST",fileRecalled.nshost()),
       castor::dlf::Param("FILEID",fileRecalled.fileid()),
       castor::dlf::Param("fseq",fileRecalled.fseq()),
       castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
       castor::dlf::Param("checksum",fileRecalled.checksum())
      };
       
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_NOTIFIED, 6, params);
    response = new NotificationAcknowledge();
    response->setTransactionId(fileRecalled.transactionId());
  
    // GET FILE FROM DB

    NsTapeGatewayHelper nsHelper;
    std::string vid;
    int copyNb;
    try {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RECALL_GET_DB_INFO, 6, params); 
      oraSvc.getSegmentInformation(fileRecalled,vid,copyNb);

    } catch (castor::exception::Exception e){
      castor::dlf::Param params[] = { 
	castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	castor::dlf::Param("FILEID",fileRecalled.fileid()),
	castor::dlf::Param("fseq",fileRecalled.fseq()),
	castor::dlf::Param("errorCode",sstrerror(e.code())),
	castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_FILE_NOT_FOUND, 6, params);
      return response;
    
    }

    // CHECK NAMESERVER

    try{
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_NS_CHECK, 6, params); 
      nsHelper.checkRecalledFile(fileRecalled, vid, copyNb); 
      
      
    } catch (castor::exception::Exception e){
      
      // nameserver error
      
      castor::dlf::Param params[] =
	{ castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	  castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	  castor::dlf::Param("FILEID",fileRecalled.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_NS_FAILURE, 5, params);
      try {
	FileErrorReport failure;
	failure.setTransactionId(fileRecalled.transactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.updateAfterFailure(failure); 
	
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	    castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	    castor::dlf::Param("FILEID",fileRecalled.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
      }
      
      return response;
    }
      
    bool allSegmentsRecalled= false;

    // UPDATE DB FOR SEGMENT

    try {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_DB_UPDATE, 4, params); 
      allSegmentsRecalled = oraSvc.segmentRecallUpdate(fileRecalled);
      
    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{ castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	  castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	  castor::dlf::Param("FILEID",fileRecalled.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
      
      FileErrorReport failure;
      failure.setTransactionId(fileRecalled.transactionId());
      failure.setFileid(fileRecalled.fileid());
      failure.setNshost(fileRecalled.nshost());
      failure.setFseq(fileRecalled.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      try {
	oraSvc.updateAfterFailure(failure);
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	    castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	    castor::dlf::Param("FILEID",fileRecalled.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
	
      }
      return response;
       
    }

    
    if (allSegmentsRecalled) {
      // RECALL COMPLETED
      
      try {
	// CHECK FILE SIZE
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_CHECK_FILE_SIZE, 5, params);
	NsTapeGatewayHelper nsHelper;
	nsHelper.checkFileSize(fileRecalled);
	
      } catch (castor::exception::Exception e) {
	
	castor::dlf::Param params[] =
	  { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	    castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	    castor::dlf::Param("FILEID",fileRecalled.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_WRONG_FILE_SIZE, 5, params);
	try {
	  FileErrorReport failure;
	  failure.setTransactionId(fileRecalled.transactionId());
	  failure.setFileid(fileRecalled.fileid());
	  failure.setNshost(fileRecalled.nshost());
	  failure.setFseq(fileRecalled.fseq());
	  failure.setErrorCode(e.code());
	  failure.setErrorMessage(e.getMessage().str());
	  oraSvc.updateAfterFailure(failure);
	} catch (castor::exception::Exception e) {
	  // db failure to mark the recall as failed
	  castor::dlf::Param params[] =
	    { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	      castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	      castor::dlf::Param("FILEID",fileRecalled.fileid()),
	      castor::dlf::Param("errorCode",sstrerror(e.code())),
	      castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
	  
	}
	return response;

      }
      try {
	// UPDATE DB

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_COMPLETED_UPDATE_DB, 5, params);
	oraSvc.fileRecallUpdate(fileRecalled);
	
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	    castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	    castor::dlf::Param("FILEID",fileRecalled.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
	
	FileErrorReport failure;
	failure.setTransactionId(fileRecalled.transactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	try {
	  oraSvc.updateAfterFailure(failure);
	} catch (castor::exception::Exception e) {
	  // db failure to mark the recall as failed
	  castor::dlf::Param params[] =
	    { castor::dlf::Param("transactionId",fileRecalled.transactionId()),
	      castor::dlf::Param("NSHOST",fileRecalled.nshost()),
	      castor::dlf::Param("FILEID",fileRecalled.fileid()),
	      castor::dlf::Param("errorCode",sstrerror(e.code())),
	      castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RECALL_CANNOT_UPDATE_DB, 5, params);
	  
	}
      
      } 
    }
  } catch  (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_INVALID_CAST, 0, NULL);
    
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
    
  } 
  
 
  return response;      
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleMigrationUpdate(  castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){
  
  // I received a FileMigratedResponse

  
  VmgrTapeGatewayHelper vmgrHelper;
  NsTapeGatewayHelper nsHelper;
  NotificationAcknowledge* response=NULL;
  
  try{

    FileMigratedNotification& fileMigrated = dynamic_cast<FileMigratedNotification&>(obj);
   
    char blockid[5]={fileMigrated.blockId0() , fileMigrated.blockId1() , fileMigrated.blockId2() , fileMigrated.blockId3(), '\0' };
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId",fileMigrated.transactionId()),
       castor::dlf::Param("NSHOST",fileMigrated.nshost()),
       castor::dlf::Param("FILEID",fileMigrated.fileid()),
       castor::dlf::Param("fseq",fileMigrated.fseq()),
       castor::dlf::Param("checksum name",fileMigrated.checksumName()),
       castor::dlf::Param("checksum",fileMigrated.checksum()),
       castor::dlf::Param("fileSize",fileMigrated.fileSize()),
       castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
       castor::dlf::Param("blockid", blockid),
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_NOTIFIED, 9, params);
    response = new NotificationAcknowledge();
    response->setTransactionId(fileMigrated.transactionId());

    // CHECK DB

    std::string repackVid;
    int copyNumber;
    std::string vid;
    u_signed64 lastModificationTime;
    try {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_MIGRATION_GET_DB_INFO, 9, params);
      repackVid=oraSvc.getRepackVidAndFileInformation(fileMigrated,vid,copyNumber,lastModificationTime);
    } catch (castor::exception::Exception e){
      
      castor::dlf::Param params[] =
	{castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	 castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	 castor::dlf::Param("FILEID",fileMigrated.fileid()),
	 castor::dlf::Param("vid",vid)
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_FILE_NOT_FOUND, 4, params);
      return response;
      
    }
      
    // UPDATE VMGR

    try {
      
      castor::dlf::Param params[] =
	{castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	 castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	 castor::dlf::Param("FILEID",fileMigrated.fileid()),
	 castor::dlf::Param("vid",vid)
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_VMGR_UPDATE, 4, params);

      vmgrHelper.updateTapeInVmgr(fileMigrated, vid); 

    } catch (castor::exception::Exception e){
    
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	  castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	  castor::dlf::Param("FILEID",fileMigrated.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_MIGRATION_VMGR_FAILURE, 5, params);
	  
      try {

	FileErrorReport failure;
	failure.setTransactionId(fileMigrated.transactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.updateAfterFailure(failure);
      } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	  castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	  castor::dlf::Param("FILEID",fileMigrated.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_CANNOT_UPDATE_DB, 6, params);
      
      }
    
      return response; 
    }


    //UPDATE NAMESERVER  
    try {
	  
      if (repackVid.empty()) {
	      
	// update the name server (standard migration)
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_NS_UPDATE, 4, params);
	nsHelper.updateMigratedFile( fileMigrated, copyNumber, vid, lastModificationTime);

      } else {
      
	// update the name server (repacked file)
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_REPACK_NS_UPDATE, 4, params);
	nsHelper.updateRepackedFile( fileMigrated, repackVid, copyNumber, vid, lastModificationTime);
	      
      }
	    
    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	  castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	  castor::dlf::Param("FILEID",fileMigrated.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_NS_FAILURE, 5, params);

      try {

	FileErrorReport failure;
	failure.setTransactionId(fileMigrated.transactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.updateAfterFailure(failure);
      } catch(castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	    castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	    castor::dlf::Param("FILEID",fileMigrated.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_CANNOT_UPDATE_DB, 6, params);
	
      }

      return response;
    
    }         
    
    // UPDATE DB
    
    try {
	  
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_DB_UPDATE, 4, params);
      oraSvc.fileMigrationUpdate(fileMigrated);

    }catch (castor::exception::Exception e) {
	  
      
      castor::dlf::Param params[] =
      {
	castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	castor::dlf::Param("FILEID",fileMigrated.fileid()),
	castor::dlf::Param("errorCode",sstrerror(e.code())),
	castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_CANNOT_UPDATE_DB, 6, params);

      try {

	FileErrorReport failure;
	failure.setTransactionId(fileMigrated.transactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.updateAfterFailure(failure);
      } catch(castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("transactionId",fileMigrated.transactionId()),
	    castor::dlf::Param("NSHOST",fileMigrated.nshost()),
	    castor::dlf::Param("FILEID",fileMigrated.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_CANNOT_UPDATE_DB, 6, params);

      }
    
    }
  } catch (std::bad_cast &ex) {
    
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_INVALID_CAST, 0, NULL);
    
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
    
  }

  return response;
    
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork(castor::IObject& obj,castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

  // I received FileToRecallRequest
  FileToRecall* response=NULL;

  try {
    FileToRecallRequest& fileToRecall = dynamic_cast<FileToRecallRequest&>(obj);
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId",fileToRecall.transactionId())
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_RECALL_REQUESTED, 1, params);
    
    
    try {
      // CALL THE DB
      
      response = oraSvc.fileToRecall(fileToRecall);

    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("transactionId",fileToRecall.transactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_RETRIEVING_DB_ERROR, 3, params);
      
      ErrorReport* errorReport=new ErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setTransactionId(fileToRecall.transactionId());
      return errorReport;
     
    }
    
    if (response  == NULL ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_NO_FILE_TO_RECALL, 1, params);
    
      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setTransactionId(fileToRecall.transactionId());
      return noMore;
    }

    char blockid[5]={response->blockId0() , response->blockId1() , response->blockId2() , response->blockId3() , '\0'};
    
    
    castor::dlf::Param completeParams[] =
      {castor::dlf::Param("transactionId",response->transactionId()),
       castor::dlf::Param("nshost",response->nshost()),
       castor::dlf::Param("fileid",response->fileid()),
       castor::dlf::Param("fseq",response->fseq()),
       castor::dlf::Param("path",response->path()),
       castor::dlf::Param("blockid", blockid ),
      };
 
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_RECALL_RETRIEVED, 6, completeParams);
  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_INVALID_CAST, 0, NULL);
    
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  return response;
}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc) throw(){
       
  // I received FileToMigrateRequest
  // I get a file from the db
  // send a new file

  FileToMigrate* response=NULL;
  
  try {
    
    FileToMigrateRequest& fileToMigrate = dynamic_cast<FileToMigrateRequest&>(obj);
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId",fileToMigrate.transactionId())
      };
     
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_REQUESTED, 1, params);
     
    try {
   
      // CALL THE DB

      response=oraSvc.fileToMigrate(fileToMigrate);

    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("transactionId",fileToMigrate.transactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_RETRIEVING_DB_ERROR, 3, params);
    
      ErrorReport* errorReport=new ErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      return errorReport;
    }
      
    if ( response == NULL ) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_NO_FILE_TO_MIGRATE, 1, params);
      // I don't have anything to migrate I send an NoMoreFiles

      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setTransactionId(fileToMigrate.transactionId());
      return noMore;
    }    
    castor::dlf::Param completeParams[] =
      {castor::dlf::Param("transactionId",response->transactionId()),
       castor::dlf::Param("nshost",response->nshost()),
       castor::dlf::Param("fileid",response->fileid()),
       castor::dlf::Param("fseq",response->fseq()),
       castor::dlf::Param("path",response->path()),
       castor::dlf::Param("fileSize", response->fileSize()),
       castor::dlf::Param("lastKnownFileName", response->lastKnownFileName()),
       castor::dlf::Param("lastModificationTime", response->lastModificationTime()),
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_MIGRATION_RETRIEVED, 8, completeParams);

  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_INVALID_CAST, 0, NULL);
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }
  return response; // check went fine

}


castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc ) throw(){
  	// I received an EndTransferRequest, I send back an EndTransferResponse

	NotificationAcknowledge* response=NULL;

	try {
	  EndNotification& endRequest = dynamic_cast<EndNotification&>(obj);
	
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("transactionId", endRequest.transactionId())
	    };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_END_NOTIFICATION, 1, params);
	  response = new NotificationAcknowledge();
	  response->setTransactionId(endRequest.transactionId());

	  try {

	    // ACCESS DB
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_END_DB_UPDATE, 1, params);
	    std::auto_ptr<castor::stager::Tape> tape(oraSvc.updateDbEndTape(endRequest)); 

	    try {
	      
	      // UPDATE VMGR

	      if (tape.get() != NULL && tape->tpmode() == 1) { // just for write
		castor::dlf::Param params[] =
		  {castor::dlf::Param("transactionId", endRequest.transactionId()),
		   castor::dlf::Param("VID", tape->vid())
		  };
	  
	      
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_END_RELEASE_TAPE, 2, params);
		VmgrTapeGatewayHelper vmgrHelper;
		vmgrHelper.resetBusyTape(*tape);

	      }
	      
	    } catch (castor::exception::Exception e) {
	      castor::dlf::Param params[] =
		{castor::dlf::Param("transactionId", endRequest.transactionId()),
		 castor::dlf::Param("VID", tape->vid()),
		 castor::dlf::Param("errorCode",sstrerror(e.code())),
		 castor::dlf::Param("errorMessage",e.getMessage().str())
		};
	      
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, 4, params);
	    }

	  } catch (castor::exception::Exception e){
    
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("transactionId", endRequest.transactionId()),
	       castor::dlf::Param("errorCode",sstrerror(e.code())),
	       castor::dlf::Param("errorMessage",e.getMessage().str())
	      };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_END_DB_ERROR, 3, params);
	    return response;
	  }

	} catch (std::bad_cast){

	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, NULL);
	  
	  ErrorReport* errorReport=new ErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  return errorReport;
	  
	}
	return  response;
}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleFileErrorReport( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

  NotificationAcknowledge* response = NULL;

  try {

    FileErrorReport &fileFailure = dynamic_cast<FileErrorReport&>(obj);

    castor::dlf::Param params[] =
      {castor::dlf::Param("transactionId",fileFailure.transactionId()),
       castor::dlf::Param("NSHOST",fileFailure.nshost()),
       castor::dlf::Param("FILEID",fileFailure.fileid())
      };
       
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_FILE_ERROR_NOTIFIED, 3, params);
    
    response = new NotificationAcknowledge();
    response->setTransactionId(fileFailure.transactionId());
  
    try {
    // DB UPDATE
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,WORKER_FILE_ERROR_DB_UPDATE, 3, params);
      castor::stager::Tape tape = oraSvc.updateAfterFailure(fileFailure);

      try {
	// UPDATE VMGR
	if (fileFailure.errorCode() == ENOSPC ) {
    
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_TAPE_MAKED_FULL, 3, params);
	  VmgrTapeGatewayHelper vmgrHelper;
	  vmgrHelper.setTapeAsFull(tape);
	
	} 
      
      } catch (castor::exception::Exception e) {

	castor::dlf::Param params[] =
	  { castor::dlf::Param("transactionId",fileFailure.transactionId()),
	    castor::dlf::Param("NSHOST",fileFailure.nshost()),
	    castor::dlf::Param("FILEID",fileFailure.fileid()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
    
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_MARK_TAPE_FULL, 6, params);
       
      }

    } catch (castor::exception::Exception e) {
      
      castor::dlf::Param params[] =
	{ castor::dlf::Param("transactionId",fileFailure.transactionId()),
	  castor::dlf::Param("NSHOST",fileFailure.nshost()),
	  castor::dlf::Param("FILEID",fileFailure.fileid()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_FILE_ERROR_CANNOT_UPDATE_DB, 6, params);
    }


  } catch  (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, WORKER_INVALID_CAST, 0, NULL);
    
    ErrorReport* errorReport=new ErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
    
  }
  return response;
}
