
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

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      


      /* constructor */
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StageRmRequest;

	/* get the right svcClassId needed to call stagerService->stageRm  */
	if((stgRequestHelper->svcClassName) =="*"){
	  this->svcClassId = 0;
	}
	
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
      }


      /* only handler which overwrite the preprocess part due to the specific behavior related with the svcClass */
      void preHandle(castor::stager::SubRequest* subrequest) throw(castor::exception::Exception);



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
	  
	  /* execute the main function for the rm request                 */
	  /* basically, a call to the corresponding stagerService method */
	  std::string server(stgCnsHelper->cnsFileid.server);
	  stgRequestHelper->stagerService->stageRm(stgCnsHelper->cnsFileid.fileid, server,this->svcClassId);
	  
	  /**************************************************/
	  /* we don t need to update the subrequestStatus  */
	  /* but we have to archive the subrequest        */
	  /* the same as for StagerSetGCHandler          */
	  stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());

	  /* replyToClient Part: *//* we always have to reply to the client in case of exception! */
	  this->newSubrequestStatus = SUBREQUEST_READY;/* even if we dont change the status, we need it toReplyToClient*/ 
	  stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);	  
	  if(stgReplyHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
	  stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper->ioResponse;
	  delete stgReplyHelper;

	}catch(castor::exception::Exception e){
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerRmHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	 
	}
      }


      StagerRmHandler::~StagerRmHandler() throw()
      {
      }
      
    }//end dbService
  }//end stager
}//end castor
