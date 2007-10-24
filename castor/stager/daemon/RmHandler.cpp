
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the StagerRequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/

#include "castor/stager/dbService/StagerRmHandler.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"



#include "castor/stager/IStagerSvc.hpp"

#include "stager_constants.h"

#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      
      
      /* constructor */
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw() :
        StagerRequestHandler()
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StageRmRequest;
        
      }
      
      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the svcClass */
      void StagerRmHandler::preHandle() throw(castor::exception::Exception)
      {

	/* get the uuid subrequest string version and check if it is valid */
	/* we can create one !*/
	stgRequestHelper->setSubrequestUuid();
	
	/* get the associated client and set the iClientAsString variable */
	stgRequestHelper->getIClient();
	
	
	/* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
	stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
	
	/* get the uuid request string version and check if it is valid */
	stgRequestHelper->setRequestUuid();
	
	/*********************************************************************************************************************************/
	/* JUST FOR StagerRmHandler TO IMPLEMENT CONSIDERING THAT * CAN BE VALID : get the svcClass and set it on the  stgRequestHelper */
	/* an link it to */
	this->rmGetSvcClass();
	/*****************************************************************************************************************************/
		
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
	
	/* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	stgRequestHelper->checkFilePermission();
       


      }
      
      
      /*************************************************************************/
      /* TO BE CONFIRMED */
      /* we just need to GET THE svcClassId by getting the right svcClass */
      void StagerRmHandler::rmGetSvcClass() throw (castor::exception::Exception)
      {

	
	 stgRequestHelper->svcClassName=stgRequestHelper->fileRequest->svcClassName(); 
	  
	 if(stgRequestHelper->svcClassName.empty()){  /* we set the default svcClassName */
	   stgRequestHelper->svcClassName="default";

	   /* the goal is to get svcClassId, so the following step might be useless (commented!) */
	   /* stgRequestHelper->fileRequest->setSvcClassName(stgRequestHelper->svcClassName);*/
	   stgRequestHelper->svcClass=stgRequestHelper->stagerService->selectSvcClass(stgRequestHelper->svcClassName);
	   if(stgRequestHelper->svcClass == NULL){
	     castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Rm"),
					  castor::dlf::Param(stgRequestHelper->subrequestUuid),
					  castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					  castor::dlf::Param("UserName",stgRequest->username),
					  castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					  castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	     };
	     castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_SVCCLASS_EXCEPTION, 6 ,params, &(stgCnsHelper->cnsFileid));
	     
	     castor::exception::Exception ex(SEINTERNAL);
	     ex.getMessage()<<"Impossible to get the svcClass"<<std::endl;
	     throw(ex);
	   }
	   this->svcClassId = stgRequestHelper->svcClass->id();	   
	   
	 }else if(stgRequestHelper->svcClassName == "*"){
	   this->svcClassId = 0;
	
	 }else{
	   stgRequestHelper->svcClass=stgRequestHelper->stagerService->selectSvcClass(stgRequestHelper->svcClassName);
	   if(stgRequestHelper->svcClass == NULL){
	     castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Rm"),
					  castor::dlf::Param(stgRequestHelper->subrequestUuid),
					  castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					  castor::dlf::Param("UserName",stgRequest->username),
					  castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					  castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	     };
	     castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_SVCCLASS_EXCEPTION, 6 ,params, &(stgCnsHelper->cnsFileid));
	 
	     castor::exception::Exception ex(SEINTERNAL);
	     ex.getMessage()<<"Impossible to get the svcClass"<<std::endl;
	     throw(ex);
	   }
	   this->svcClassId = stgRequestHelper->svcClass->id();	  
	 }


      }
      
      
      /******************************/
      /* handle for the rm request */
      /****************************/
      void StagerRmHandler::handle() throw(castor::exception::Exception)
      {

	StagerReplyHelper* stgReplyHelper=NULL;
	try{

	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/
	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_RM, 5 ,params, &(stgCnsHelper->cnsFileid));
	  


	  /* execute the main function for the rm request                 */
	  /* basically, a call to the corresponding stagerService method */
	  std::string server(stgCnsHelper->cnsFileid.server);
	  if(stgRequestHelper->stagerService->stageRm(stgRequestHelper->subrequest->id(),stgCnsHelper->cnsFileid.fileid, server,this->svcClassId, 0)) {

	  /* FOR THE SYSTEM PART:*/
	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("Subrequest id",stgRequestHelper->subrequest->id()),
				       castor::dlf::Param("File id",stgCnsHelper->cnsFileid.fileid),
				       castor::dlf::Param("nsHost", server),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_RM_DETAILS, 8,params, &(stgCnsHelper->cnsFileid));

    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
            
      /* replyToClient Part: */
      stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);	  
      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
      stgReplyHelper->endReplyToClient(stgRequestHelper);
      delete stgReplyHelper;
    }
    else {  // user error, log it
	    castor::dlf::Param params[]={castor::dlf::Param("Request type", "StageRm"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 6 ,params, &(stgCnsHelper->cnsFileid));
    }	  
            
	}catch(castor::exception::Exception e){
	  if(stgReplyHelper != NULL) delete stgReplyHelper;	 
	  castor::dlf::Param params[]={castor::dlf::Param{"Error Code",sstrerror(e.code())},
				       castor::dlf::Param{"Error Message",e.getMessage().str()}
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_UPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw(e);
	}
      }


      StagerRmHandler::~StagerRmHandler() throw()
      {

      }
      
    }//end dbService
  }//end stager
}//end castor
