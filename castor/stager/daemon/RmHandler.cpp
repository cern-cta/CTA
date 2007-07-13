
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
#include "serrno.h"

#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;


	/* get the right svcClassId needed to call stagerService->stageRm  */
	if(!strcmp(stgRequestHelper->svcClassName.c_str(),"*")){
	  this->svcClassId = 0;
	}
	
      }

      void StagerRmHandler::handle() throw(castor::exception::Exception)
      {
	try{
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
	  this->stgReplyHelper = new StagerReplyHelper;	  
	  if((this->stgReplyHelper) == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  this->stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0, "No error");
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper->ioResponse;
	  delete stgReplyHelper;

	}catch(castor::exception::Exception ex){
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}
      }


      StagerRmHandler::~StagerRmHandler() throw()
      {
      }
      
    }//end dbService
  }//end stager
}//end castor
