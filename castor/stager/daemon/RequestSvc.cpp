/*************************************/
/* main class for the ___RequestSvc */
/***********************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/RequestSvc.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cglobals.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"


#include "castor/Constants.hpp"

#include "marshall.h"
#include "net.h"

#include "getconfent.h"
#include "Cnetdb.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>
#include <list>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>



namespace castor{
  namespace stager{
    namespace dbService{



      /*****************************************************************/
      /* function to perform the common flow for all the __RequestSvc */
      /* basically, calls to the helpers to create the objects and   */
      /* to make the necessary links on DB */
      /************************************/
      void RequestSvc::preprocess(castor::stager::SubRequest* subrequest) throw(castor::exception::Exception)
      {
	
	/*******************************************/
	/* We create the helpers at the beginning */
	/*****************************************/
	
	stgCnsHelper = new StagerCnsHelper();
	if(stgCnsHelper == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(RequestSvc) Impossible to create the StagerCnsHelper"<<std::endl;
	  throw ex;
	}
	
	stgRequestHelper = new StagerRequestHelper(subrequest);
	if(stgRequestHelper == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<< "(RequestSvc) Impossible to create the StagerRequestHelper"<<std::endl;
	  throw ex;
	}
	
	
	/* we get the Cuuid_t fileid (needed to logging in dl)  */
	castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","Helpers successfully created")};
	castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 1, param);/*   */
	
	/* settings BaseAddress */
	stgRequestHelper->settingBaseAddress();
	
	/* get the uuid subrequest string version and check if it is valid */
	/* we can create one !*/
	stgRequestHelper->setSubrequestUuid();
	
	/* obtain all the fileRequest associated to the subrequest using the dbService to get the foreign representation */
	stgRequestHelper->getFileRequest();/* get from subrequest */
	
	
	this->typeRequest = stgRequestHelper->fileRequest->type();
	
	/* get the associated client and set the iClientAsString variable */
	stgRequestHelper->getIClient();
	
	
	/* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
	stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
	
	/* get the uuid request string version and check if it is valid */
	stgRequestHelper->setRequestUuid();
	
	
	/* get the svcClass */
	stgRequestHelper->getSvcClass();
	
	
	/* create and fill request->svcClass link on DB */
	stgRequestHelper->linkRequestToSvcClassOnDB();
		
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	this->fileExist = stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
	
	/* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	stgRequestHelper->checkFilePermission();
	
	castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","(RequestSvc) Detailed subrequest(fileName,{euid,egid},{userName,groupName},mode mask, cliendId, svcClassName,cnsFileid.fileid, cnsFileid.server"),
					  castor::dlf::Param("Standard Message",stgCnsHelper->subrequestFileName),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->euid),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->egid),
					  castor::dlf::Param("Standard Message",stgRequestHelper->username),
					  castor::dlf::Param("Standard Message",stgRequestHelper->groupname),
					  castor::dlf::Param("Standard Message",stgRequestHelper->fileRequest->mask()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->iClient->id()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->svcClassName),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.fileid),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.server)};
	castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 11, parameter);
       
      }
      
     

      /**********************************************************************************/
      /* function to handle the exception (set subrequest to FAILED and replyToClient) */
      /********************************************************************************/
      void RequestSvc::handleException(int errorCode, std::string errorMessage){
	
	const char* errorMessageC = errorMessage.c_str();
	if(stgRequestHelper!=NULL){
	  
	  /* we have to set the subrequest status to SUBREQUEST_FAILED_FINISHED */
	  /* but not setGetNextSubrequestStatus!!  */
	  if(stgRequestHelper->subrequest != NULL) stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED_FINISHED);
	  
	  /* reply to the client in case of error*/
	  if(stgRequestHelper->iClient != NULL){
	    StagerReplyHelper *stgReplyHelper = new StagerReplyHelper(SUBREQUEST_FAILED_FINISHED);
	    if(stgReplyHelper == NULL){
	      std::cerr<<"(RequestSvc handleException)"<<strerror(errorCode)<<errorMessage<<std::endl;
	    }else{
	      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,errorCode,errorMessageC);
	      stgReplyHelper->endReplyToClient(stgRequestHelper);
	      delete stgReplyHelper->ioResponse;
	      delete stgReplyHelper;
	    }
	  }else{
	    if((stgRequestHelper->dbService)&&(stgRequestHelper->subrequest)) stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	    std::cerr<<"(RequestSvc handleException)"<<strerror(errorCode)<<errorMessage<<std::endl;
	  }
	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper != NULL) delete stgCnsHelper;
	  
	}else{
	  std::cerr<<"(RequestSvc handleException)"<<strerror(errorCode)<<errorMessage<<std::endl;
	}
      }
         
        
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
