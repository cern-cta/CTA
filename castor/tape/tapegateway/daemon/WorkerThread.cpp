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
 * @(#)$RCSfile: WorkerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include <errno.h>
#include <getconfent.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>
#include <memory>
#include <typeinfo>

#include "Cns_api.h" // for log only

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/tape/net/net.hpp"

#include "castor/tape/tapegateway/daemon/DlfCodes.hpp"
#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/WorkerThread.hpp"

#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"

#include "castor/tape/utils/utils.hpp"

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
  m_handlerMap[OBJ_EndNotificationErrorReport] = &WorkerThread::handleFailWorker;

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

    char hostName[net::HOSTNAMEBUFLEN];

    try {
      sock->getPeerIp(port, ip);
      net::getPeerHostName(sock->socket(), hostName);


    } catch(castor::exception::Exception e) {
      // "Exception caught : ignored" message
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_UNKNOWN_CLIENT, 2, params);
      return;
      
    }
 

    castor::dlf::Param params[] =
      {castor::dlf::Param("IP",  castor::dlf::IPAddress(ip)), 
       castor::dlf::Param("Port",port),
       castor::dlf::Param("HostName",hostName)
      };
   

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MESSAGE_RECEIVED, 3, params);

    
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
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_DISPATCHING, 0, NULL);

      std::auto_ptr<castor::IObject> response( (this->*handler)(*obj,*oraSvc));

      // send back the proper response

      try {

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RESPONDING, 0, NULL);

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

}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleStartWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

  // I received a start worker request
  Volume* response=new Volume();

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_VOLUME_REQUESTED, 0, NULL);

  try{
    
    VolumeRequest &startRequest = dynamic_cast<VolumeRequest&>(obj);
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId())
    };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_GETTING_VOLUME, 1, params);

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);
    
    try {
    
      // GET FROM DB

      oraSvc.startTapeSession(startRequest,*response); 
    
    } catch (castor::exception::Exception e){
      if (response) delete response;

      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(startRequest.mountTransactionId());
    
      castor::dlf::Param params[] =
	{castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_VOLUME, 3, params);
      return errorReport;
    }
     
    if (response->mountTransactionId()==0 ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_NO_FILE, 1, params);
      
      delete response;

      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(startRequest.mountTransactionId());
      return noMore;
    }

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

    castor::dlf::Param params0[] =      
      {castor::dlf::Param("mountTransactionId", startRequest.mountTransactionId()),
       castor::dlf::Param("TPVID",response->vid()),
       castor::dlf::Param("mode",response->mode()),
       castor::dlf::Param("label",response->label()),
       castor::dlf::Param("density",response->density()),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_VOLUME_FOUND, 6, params0);

  } catch( std::bad_cast &ex) {

    if (response) delete response;
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
  }

  return response;

}


castor::IObject* castor::tape::tapegateway::WorkerThread::handleRecallUpdate( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

    // I received a FileRecalledResponse
  NotificationAcknowledge* response = new NotificationAcknowledge();
  try {

    FileRecalledNotification &fileRecalled = dynamic_cast<FileRecalledNotification&>(obj);

    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    fileRecalled.nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = fileRecalled.fileid();
    
    char checksumHex[19];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';
    try {
      utils::toHex((uint64_t)fileRecalled.checksum(), &checksumHex[2], 17);
    } catch (castor::exception::Exception e) {

      castor::dlf::Param paramsError[] =
      {castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
       castor::dlf::Param("fseq",fileRecalled.fseq()),
       castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
       castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
       
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 5, paramsError, &castorFileId);
    
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(EINVAL);
      errorReport->setErrorMessage("invalid object");
      errorReport->setMountTransactionId(fileRecalled.mountTransactionId());
      if (response) delete response;
      return errorReport;

    }

    castor::dlf::Param paramsComplete[] =
      {castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
       castor::dlf::Param("fseq",fileRecalled.fseq()),
       castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
       castor::dlf::Param("checksum", checksumHex),
       castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId())
      };
       
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_NOTIFIED, 5, paramsComplete, &castorFileId);
  
    response->setMountTransactionId(fileRecalled.mountTransactionId());
  
    // GET FILE FROM DB

    NsTapeGatewayHelper nsHelper;
    std::string vid;
    int copyNb;

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    try {
      oraSvc.getSegmentInfo(fileRecalled,vid,copyNb);

    } catch (castor::exception::Exception e){
      castor::dlf::Param params[] = { 
	castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	castor::dlf::Param("errorCode",sstrerror(e.code())),
	castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_FILE_NOT_FOUND, 4, params, &castorFileId);
      return response;
    
    }

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
    castor::dlf::Param paramsDb[] =
      {castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
       castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
       castor::dlf::Param("fseq",fileRecalled.fseq()),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001),
       castor::dlf::Param("TPVID",vid),
       castor::dlf::Param("copyNumber",copyNb)
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_GET_DB_INFO, 6, paramsDb, &castorFileId); 

    // CHECK NAMESERVER

    try{

      gettimeofday(&tvStart, NULL);
      nsHelper.checkRecalledFile(fileRecalled, vid, copyNb); 
      gettimeofday(&tvEnd, NULL);

      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
      castor::dlf::Param paramsNs[] =
      {castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
       castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
       castor::dlf::Param("fseq",fileRecalled.fseq()),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001),
       castor::dlf::Param("TPVID",vid),
       castor::dlf::Param("copyNumber",copyNb),
       castor::dlf::Param("checksum name",fileRecalled.checksumName().c_str()),
       castor::dlf::Param("checksum", checksumHex),
      };

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_NS_CHECK, 8, paramsNs, &castorFileId); 
      
      
    } catch (castor::exception::Exception e){
      
      // nameserver error
      
      castor::dlf::Param params[] =
	{ castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_RECALL_NS_FAILURE, 4, params, &castorFileId);

      try {
	FileErrorReport failure;
	failure.setMountTransactionId(fileRecalled.mountTransactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.failFileTransfer(failure); 
	
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, 4, params, &castorFileId);
      }
      
      return response;
    }
      
    try {
      // CHECK FILE SIZE

      gettimeofday(&tvStart, NULL);
      NsTapeGatewayHelper nsHelper;
      nsHelper.checkFileSize(fileRecalled);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsNs[] =
	{castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	 castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	 castor::dlf::Param("file size",fileRecalled.fileSize()),
	 castor::dlf::Param("fseq",fileRecalled.fseq()),
	 castor::dlf::Param("ProcessingTime", procTime * 0.000001),
	 castor::dlf::Param("TPVID",vid)
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_CHECK_FILE_SIZE, 6, paramsNs, &castorFileId);
      
    } catch (castor::exception::Exception e) {
      
      castor::dlf::Param params[] =
	{ castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_WRONG_FILE_SIZE, 4, params, &castorFileId );
      try {
	FileErrorReport failure;
	failure.setMountTransactionId(fileRecalled.mountTransactionId());
	failure.setFileid(fileRecalled.fileid());
	failure.setNshost(fileRecalled.nshost());
	failure.setFseq(fileRecalled.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.failFileTransfer(failure);
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, 4, params, &castorFileId);
	  
      }
      return response;

    }

    gettimeofday(&tvStart, NULL);

    try {
      // UPDATE DB
      oraSvc.setFileRecalled(fileRecalled);
      
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
      castor::dlf::Param paramsDbUpdate[] =
	{castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	 castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	 castor::dlf::Param("fseq",fileRecalled.fseq()),
	 castor::dlf::Param("ProcessingTime", procTime * 0.000001),
	 castor::dlf::Param("TPVID",vid),
	 castor::dlf::Param("copyNumber",copyNb),
	 castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_COMPLETED_UPDATE_DB, 7, paramsDbUpdate, &castorFileId); 

	
    } catch (castor::exception::Exception e) {
      // db failure to mark the recall as failed
      castor::dlf::Param params[] =
	{ castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_CANNOT_UPDATE_DB, 4, params, &castorFileId );
	
      FileErrorReport failure;
      failure.setMountTransactionId(fileRecalled.mountTransactionId());
      failure.setFileid(fileRecalled.fileid());
      failure.setNshost(fileRecalled.nshost());
      failure.setFseq(fileRecalled.fseq());
      failure.setErrorCode(e.code());
      failure.setErrorMessage(e.getMessage().str());
      try {
	oraSvc.failFileTransfer(failure);
      } catch (castor::exception::Exception e) {
	// db failure to mark the recall as failed
	castor::dlf::Param params[] =
	  { castor::dlf::Param("mountTransactionId",fileRecalled.mountTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileRecalled.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_CANNOT_UPDATE_DB, 4, params, &castorFileId);
	
      }
      
    }
 
  } catch  (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
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
  NotificationAcknowledge* response= new NotificationAcknowledge();
  
  try{

    FileMigratedNotification& fileMigrated = dynamic_cast<FileMigratedNotification&>(obj);
   
    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    fileMigrated.nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = fileMigrated.fileid();

    char blockid[5]={fileMigrated.blockId0() , fileMigrated.blockId1() , fileMigrated.blockId2() , fileMigrated.blockId3(), '\0' };

    char checksumHex[19];
    checksumHex[0] = '0';
    checksumHex[1] = 'x';

    try {

      utils::toHex((uint64_t)fileMigrated.checksum(), &checksumHex[2], 17);
    } catch (castor::exception::Exception e) {
       castor::dlf::Param paramsError[] =
	 {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	  castor::dlf::Param("fseq",fileMigrated.fseq()),
	  castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	 };	 
       
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 5, paramsError, &castorFileId);
    
      if (response) delete response;
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(EINVAL);
      errorReport->setErrorMessage("invalid object");
      errorReport->setMountTransactionId(fileMigrated.mountTransactionId());
      return errorReport;

    }
      
    castor::dlf::Param paramsComplete[] =
      {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
       castor::dlf::Param("fseq",fileMigrated.fseq()),
       castor::dlf::Param("checksum name",fileMigrated.checksumName()),
       castor::dlf::Param("checksum",checksumHex),
       castor::dlf::Param("fileSize",fileMigrated.fileSize()),
       castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
       castor::dlf::Param("blockid", blockid),
       castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NOTIFIED, 8, paramsComplete, &castorFileId);
   
    response->setMountTransactionId(fileMigrated.mountTransactionId());

   
    // CHECK DB

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    std::string repackVid;
    int copyNumber;
    std::string vid;
    u_signed64 lastModificationTime;
    try {
      oraSvc.getRepackVidAndFileInfo(fileMigrated,vid,copyNumber,lastModificationTime,repackVid);
    } catch (castor::exception::Exception e){
      
      castor::dlf::Param params[] =
	{castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	 castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_FILE_NOT_FOUND, 4, params, &castorFileId);
      return response;
      
    }

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
  
    castor::dlf::Param paramsDb[] =
      {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
       castor::dlf::Param("fseq",fileMigrated.fseq()),
       castor::dlf::Param("blockid", blockid),
       castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
       castor::dlf::Param("TPVID",vid),
       castor::dlf::Param("copyNb",copyNumber),
       castor::dlf::Param("lastModificationTime",lastModificationTime),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_MIGRATION_GET_DB_INFO, 8, paramsDb, &castorFileId);
      
    // UPDATE VMGR

    try {
      
      gettimeofday(&tvStart, NULL);
      
      vmgrHelper.updateTapeInVmgr(fileMigrated, vid);

      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


       castor::dlf::Param paramsVmgr[] =
      {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
       castor::dlf::Param("fseq",fileMigrated.fseq()),
       castor::dlf::Param("blockid", blockid),
       castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
       castor::dlf::Param("TPVID",vid),
       castor::dlf::Param("copyNb",copyNumber),
       castor::dlf::Param("lastModificationTime",lastModificationTime),
       castor::dlf::Param("fileSize",fileMigrated.fileSize()),
       castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
       castor::dlf::Param("blockid", blockid),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_VMGR_UPDATE, 11, paramsVmgr, &castorFileId);
      
 

    } catch (castor::exception::Exception e){
    
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_MIGRATION_VMGR_FAILURE, 4, params, &castorFileId);
	  
      try {

	FileErrorReport failure;
	failure.setMountTransactionId(fileMigrated.mountTransactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.failFileTransfer(failure);
      } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, 4, params,&castorFileId);
      
      }
    
      return response; 
    }

    

    //UPDATE NAMESERVER  
    try {
	  
      if (repackVid.empty()) {
	gettimeofday(&tvStart, NULL);
      
	// update the name server (standard migration)

	nsHelper.updateMigratedFile( fileMigrated, copyNumber, vid, lastModificationTime);

	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	castor::dlf::Param paramsNs[] =
	  {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	   castor::dlf::Param("fseq",fileMigrated.fseq()),
	   castor::dlf::Param("blockid", blockid),
	   castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	   castor::dlf::Param("TPVID",vid),
	   castor::dlf::Param("copyNb",copyNumber),
	   castor::dlf::Param("lastModificationTime",lastModificationTime),
	   castor::dlf::Param("fileSize",fileMigrated.fileSize()),
	   castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
	   castor::dlf::Param("blockid", blockid),
	   castor::dlf::Param("checksum name",fileMigrated.checksumName()),
	   castor::dlf::Param("checksum",checksumHex),
	   castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };

	      
	// update the name server (standard migration)
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NS_UPDATE, 13, paramsNs,&castorFileId);

      } else {
	

	// update the name server (repacked file)

	gettimeofday(&tvStart, NULL);
	
	nsHelper.updateRepackedFile( fileMigrated, repackVid, copyNumber, vid, lastModificationTime);
	
	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	castor::dlf::Param paramsRepack[] =
	  {castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	   castor::dlf::Param("fseq",fileMigrated.fseq()),
	   castor::dlf::Param("blockid", blockid),
	   castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	   castor::dlf::Param("TPVID",vid),
	   castor::dlf::Param("copyNb",copyNumber),
	   castor::dlf::Param("lastModificationTime",lastModificationTime),
	   castor::dlf::Param("fileSize",fileMigrated.fileSize()),
	   castor::dlf::Param("compressedFileSize",fileMigrated.compressedFileSize()),
	   castor::dlf::Param("blockid", blockid),
	   castor::dlf::Param("checksum name",fileMigrated.checksumName()),
	   castor::dlf::Param("checksum",checksumHex),
	   castor::dlf::Param("repack vid",repackVid),
	   castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };
         
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_REPACK_NS_UPDATE, 14, paramsRepack,&castorFileId);

      }
	    
    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	  castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	  castor::dlf::Param("errorCode",sstrerror(e.code())),
	  castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_NS_FAILURE, 4, params,&castorFileId);

      try {

	FileErrorReport failure;
	failure.setMountTransactionId(fileMigrated.mountTransactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.failFileTransfer(failure);
      } catch(castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, 4, params,&castorFileId);
	
      }

      return response;
    
    }         
    
    // UPDATE DB
    gettimeofday(&tvStart, NULL);
    
    try {
	  
      oraSvc.setFileMigrated(fileMigrated);

    }catch (castor::exception::Exception e) {
	  
      
      castor::dlf::Param params[] =
      {
	castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	castor::dlf::Param("errorCode",sstrerror(e.code())),
	castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, 4, params, &castorFileId);

      try {

	FileErrorReport failure;
	failure.setMountTransactionId(fileMigrated.mountTransactionId());
	failure.setFileid(fileMigrated.fileid());
	failure.setNshost(fileMigrated.nshost());
	failure.setFseq(fileMigrated.fseq());
	failure.setErrorCode(e.code());
	failure.setErrorMessage(e.getMessage().str());
	oraSvc.failFileTransfer(failure);
      } catch(castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	    castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	    castor::dlf::Param("errorCode",sstrerror(e.code())),
	    castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_CANNOT_UPDATE_DB, 4, params, &castorFileId);

      }
    
    }

    gettimeofday(&tvEnd, NULL);
    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    
    castor::dlf::Param paramsDbUpdate[] =
      {
	castor::dlf::Param("mountTransactionId",fileMigrated.mountTransactionId()),
	castor::dlf::Param("fseq",fileMigrated.fseq()),
	castor::dlf::Param("blockid", blockid),
	castor::dlf::Param("fileTransactionId",fileMigrated.fileTransactionId()),
	castor::dlf::Param("TPVID",vid),
	castor::dlf::Param("copyNb",copyNumber),
	castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_DB_UPDATE, 7, paramsDbUpdate,&castorFileId);


  } catch (std::bad_cast &ex) {
    
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    
    if (response) delete response;
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    return errorReport;
    
  }

  return response;
    
}

castor::IObject*  castor::tape::tapegateway::WorkerThread::handleRecallMoreWork(castor::IObject& obj,castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw(){

  // I received FileToRecallRequest
  FileToRecall* response= new FileToRecall();

  try {
    FileToRecallRequest& fileToRecall = dynamic_cast<FileToRecallRequest&>(obj);
    castor::dlf::Param params[] =
      {castor::dlf::Param("mountTransactionId",fileToRecall.mountTransactionId())
      };
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_RECALL_REQUESTED, 1, params);
    
    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    try {
      // CALL THE DB
      
      oraSvc.getFileToRecall(fileToRecall,*response);

    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("mountTransactionId",fileToRecall.mountTransactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_RETRIEVING_DB_ERROR, 3, params);
      
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(fileToRecall.mountTransactionId());
      if (response) delete response;
      return errorReport;
     
    }
    
    if (response->mountTransactionId()  == 0 ) {
      // I don't have anything to recall I send a NoMoreFiles
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_RECALL, 1, params);
      delete response;
      
      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(fileToRecall.mountTransactionId());
      return noMore;
    }

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
  
    char blockid[5]={response->blockId0() , response->blockId1() , response->blockId2() , response->blockId3() , '\0'};
    
    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    response->nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = response->fileid();

    castor::dlf::Param completeParams[] =
      {castor::dlf::Param("mountTransactionId",response->mountTransactionId()),
       castor::dlf::Param("fseq",response->fseq()),
       castor::dlf::Param("path",response->path()),
       castor::dlf::Param("blockid", blockid ),
       castor::dlf::Param("fileTransactionId", response->fileTransactionId()),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
 
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_RECALL_RETRIEVED, 6, completeParams, &castorFileId);
  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_INVALID_CAST, 0, NULL);
    
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    if (response) delete response;
    return errorReport;
  }
  return response;
}

castor::IObject* castor::tape::tapegateway::WorkerThread::handleMigrationMoreWork( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc) throw(){
       
  // I received FileToMigrateRequest
  // I get a file from the db
  // send a new file

  FileToMigrate* response=new FileToMigrate();
  
  try {
    
    FileToMigrateRequest& fileToMigrate = dynamic_cast<FileToMigrateRequest&>(obj);
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("mountTransactionId",fileToMigrate.mountTransactionId())
      };
     
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_REQUESTED, 1, params);

    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    try {
   
      // CALL THE DB

      oraSvc.getFileToMigrate(fileToMigrate,*response);

    } catch (castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("mountTransactionId",fileToMigrate.mountTransactionId()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_MIGRATION_RETRIEVING_DB_ERROR, 3, params);
    
      EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
      errorReport->setErrorCode(e.code());
      errorReport->setErrorMessage(e.getMessage().str());
      errorReport->setMountTransactionId(fileToMigrate.mountTransactionId());
      if (response) delete response;
      return errorReport;
    }
      
    if ( response->mountTransactionId() == 0 ) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_NO_FILE_TO_MIGRATE, 1, params);
      // I don't have anything to migrate I send an NoMoreFiles

      NoMoreFiles* noMore= new NoMoreFiles();
      noMore->setMountTransactionId(fileToMigrate.mountTransactionId());
      delete response;
      return noMore;
    }
    
    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

    struct Cns_fileid castorFileId;
    memset(&castorFileId,'\0',sizeof(castorFileId));
    strncpy(
	    castorFileId.server,
	    response->nshost().c_str(),
	    sizeof(castorFileId.server)-1
	    );
    castorFileId.fileid = response->fileid();

    castor::dlf::Param paramsComplete[] =
      {castor::dlf::Param("mountTransactionId",response->mountTransactionId()),
       castor::dlf::Param("fseq",response->fseq()),
       castor::dlf::Param("path",response->path()),
       castor::dlf::Param("fileSize", response->fileSize()),
       castor::dlf::Param("lastKnownFileName", response->lastKnownFilename()),
       castor::dlf::Param("lastModificationTime", response->lastModificationTime()),
       castor::dlf::Param("fileTransactionId", response->fileTransactionId()),
       castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_MIGRATION_RETRIEVED, 8, paramsComplete, &castorFileId);

  } catch (std::bad_cast &ex) {
    // "Invalid Request" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
    EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
    errorReport->setErrorCode(EINVAL);
    errorReport->setErrorMessage("invalid object");
    if (response) delete response;
    return errorReport;
  }
  return response; // check went fine

}


castor::IObject*  castor::tape::tapegateway::WorkerThread::handleEndWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc ) throw(){
  	// I received an EndTransferRequest, I send back an EndTransferResponse

	NotificationAcknowledge* response=new NotificationAcknowledge();

	try {
	  EndNotification& endRequest = dynamic_cast<EndNotification&>(obj);
	
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId())
	    };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_NOTIFICATION, 1, params);
	  
	  response->setMountTransactionId(endRequest.mountTransactionId());

	  timeval tvStart,tvEnd;
	  gettimeofday(&tvStart, NULL);
	  

	  try {

	    // ACCESS DB to get tape to release
	    
	    castor::stager::Tape tape;
	    oraSvc.getTapeToRelease(endRequest.mountTransactionId(),tape); 

	    gettimeofday(&tvEnd, NULL);
	    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	    castor::dlf::Param paramsComplete[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	     castor::dlf::Param("TPVID",tape.vid()),
	     castor::dlf::Param("mode",tape.tpmode()),
	     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_GET_TAPE_TO_RELEASE, 4, paramsComplete);


	    try {
	      
	      // UPDATE VMGR

	      if (tape.tpmode() == 1) { // just for write
	
		gettimeofday(&tvStart, NULL);
     
		VmgrTapeGatewayHelper vmgrHelper;
		vmgrHelper.resetBusyTape(tape);

		gettimeofday(&tvEnd, NULL);
		procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		castor::dlf::Param paramsVmgr[] =
		  {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		   castor::dlf::Param("TPVID",tape.vid()),
		   castor::dlf::Param("mode",tape.tpmode()),
		   castor::dlf::Param("ProcessingTime", procTime * 0.000001)
		  };
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_END_RELEASE_TAPE, 4, paramsVmgr);

	      }
	      
	    } catch (castor::exception::Exception e) {
	      castor::dlf::Param params[] =
		{castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		 castor::dlf::Param("TPVID", tape.vid()),
		 castor::dlf::Param("errorCode",sstrerror(e.code())),
		 castor::dlf::Param("errorMessage",e.getMessage().str())
		};
	      
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, 4, params);
	    }

	    // ACCESS DB now we mark it as done
	    

	    gettimeofday(&tvStart, NULL);
	    
	    oraSvc.endTapeSession(endRequest);

	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


	    castor::dlf::Param paramsDb[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	     castor::dlf::Param("TPVID",tape.vid()),
	     castor::dlf::Param("mode",tape.tpmode()),
	     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_END_DB_UPDATE, 4, paramsDb);
    

	  } catch (castor::exception::Exception e){
    
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	       castor::dlf::Param("errorCode",sstrerror(e.code())),
	       castor::dlf::Param("errorMessage",e.getMessage().str())
	      };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_END_DB_ERROR, 3, params);
	    return response;
	  }

	} catch (std::bad_cast){

	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
	  
	  EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  if (response) delete response;
	  return errorReport;
	  
	}
	return  response;
}



castor::IObject*  castor::tape::tapegateway::WorkerThread::handleFailWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc ) throw(){
  	// I received an EndNotificationErrorReport 

	NotificationAcknowledge* response= new NotificationAcknowledge();

	try {
	  EndNotificationErrorReport& endRequest = dynamic_cast<EndNotificationErrorReport&>(obj);
	
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	     castor::dlf::Param("errorcode", endRequest.errorCode()),
	     castor::dlf::Param("errorMessage", endRequest.errorMessage())
	    };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, WORKER_FAIL_NOTIFICATION, 3, params);
	 
	  response->setMountTransactionId(endRequest.mountTransactionId());
	  timeval tvStart,tvEnd;
	  gettimeofday(&tvStart, NULL);

	  try {

	    // ACCESS DB to get tape to release

	    castor::stager::Tape tape;
	    oraSvc.getTapeToRelease(endRequest.mountTransactionId(),tape);
	    
	    gettimeofday(&tvEnd, NULL);
	    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

	    castor::dlf::Param paramsComplete[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	     castor::dlf::Param("TPVID",tape.vid()),
	     castor::dlf::Param("mode",tape.tpmode()),
	     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_FAIL_GET_TAPE_TO_RELEASE, 4, paramsComplete);
	   
	      
	    // UPDATE VMGR
	    
	    if (tape.tpmode() == 1) { // just for write
	      VmgrTapeGatewayHelper vmgrHelper;
	

	      // CHECK IF THE ERROR WAS DUE TO A FULL TAPE
	      if (endRequest.errorCode() == ENOSPC ) {

		
		try {
		  
		  gettimeofday(&tvStart, NULL);

		  vmgrHelper.setTapeAsFull(tape);

		  gettimeofday(&tvEnd, NULL);
		  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		  castor::dlf::Param paramsVmgr[] =
		    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		     castor::dlf::Param("TPVID",tape.vid()),
		     castor::dlf::Param("mode",tape.tpmode()),
		     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
		    };
		  
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_TAPE_MAKED_FULL, 4, paramsVmgr);


		  
		} catch (castor::exception::Exception e) {
		  castor::dlf::Param params[] =
		    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		     castor::dlf::Param("TPVID", tape.vid()),
		     castor::dlf::Param("errorCode",sstrerror(e.code())),
		     castor::dlf::Param("errorMessage",e.getMessage().str())
		    };
    
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_MARK_TAPE_FULL, 4, params);
       
		}
		
	      } else {

		try {
		  // We just release the tape
		  
		  gettimeofday(&tvStart, NULL);

		  vmgrHelper.resetBusyTape(tape);

		  gettimeofday(&tvEnd, NULL);
		  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

		  castor::dlf::Param paramsVmgr[] =
		    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		     castor::dlf::Param("TPVID",tape.vid()),
		     castor::dlf::Param("mode",tape.tpmode()),
		     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
		    };
		   
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_FAIL_RELEASE_TAPE, 4, paramsVmgr);
	      
		} catch (castor::exception::Exception e) {
		  castor::dlf::Param params[] =
		    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
		     castor::dlf::Param("TPVID", tape.vid()),
		     castor::dlf::Param("errorCode",sstrerror(e.code())),
		     castor::dlf::Param("errorMessage",e.getMessage().str())
		    };
	      
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, 4, params);
		}
	      }
	    }

	    // ACCESS db now we fail it 
	    gettimeofday(&tvStart, NULL);

	    oraSvc.failTapeSession(endRequest); 

	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


	    castor::dlf::Param paramsDb[] =
	    {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	     castor::dlf::Param("TPVID",tape.vid()),
	     castor::dlf::Param("mode",tape.tpmode()),
	     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,WORKER_FAIL_DB_UPDATE, 4, paramsDb);
	    

	    
	  } catch (castor::exception::Exception e){
    
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("mountTransactionId", endRequest.mountTransactionId()),
	       castor::dlf::Param("errorCode",sstrerror(e.code())),
	       castor::dlf::Param("errorMessage",e.getMessage().str())
	      };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_FAIL_DB_ERROR, 3, params);
	    return response;
	  }

	 

	} catch (std::bad_cast){

	  // "Invalid Request" message
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, WORKER_INVALID_CAST, 0, NULL);
	  
	  EndNotificationErrorReport* errorReport=new EndNotificationErrorReport();
	  errorReport->setErrorCode(EINVAL);
	  errorReport->setErrorMessage("invalid object");
	  if (response) delete response;
	  return errorReport;
	  
	}
	return  response;
}
