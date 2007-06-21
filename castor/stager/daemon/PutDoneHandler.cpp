/***************************************************************************************************/
/* StagerPutDoneHandler: Constructor and implementation of the PutDone request's handle        */
/*********************************************************************************************** */

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"
#include "StagerPutDoneHandler.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../IClientFactory.h"
#include "../SubRequestStatusCodes.hpp"
#include "../DiskCopyForRecall.hpp"

#include "../../exception/Exception.hpp"

#include <iostream>
#include <string>








namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerPutDoneHandler::StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
     	
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
      }

      void StagerPutDoneHandler::handle() throw(castor::exception::Exception)
      {
	
	try{

	  jobOriented();/* until it will be explored */
	  /* is PutDone jobOriented? I don' t think so, then I won't call this method */
	  /* ask about the state of the sources */
	  stgRequestHelper->stagerService->isSubRequestToSchedule((stgRequestHelper->subrequest), &(this->sources));
	  
	  if(sources->size()<=0){
	    castor::exception::Exception ex(EPERM);
	    ex.getMessage()<<"(StagerPutDoneHandler handle) PutDone without a Put (sources.size <0)"<<std::endl;
	    throw ex;
	  }
	  
	  this->jobService = new castor::stager::IJobSvc;
	  if(this->jobService == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerPutDoneHandler handle) Impossible to get the jobService"<<std::endl;
	    throw ex;
	  }
	  jobService->prepareForMigration(stgRequestHelper->subrequest,0,0);
	  delete jobService;

	  /* we never change the subrequestStatus !!!*/	  
	  
	  /* for the PutDone, if everything is ok, we archive the subrequest */
	  stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	  
	  /* replyToClient Part: */
	  this->stgReplyHelper = new StagerReplyHelper;
	  if((this->stgReplyHelper) == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw ex;
	  }
	  this->stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0, "No error");
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper;
	   
	}catch(castor::exception::Exception ex){
	  if(jobService != NULL){
	    delete jobService;
	  }
	  if(stgReplyHelper != NULL){
	    delete stgReplyHelper;
	  }
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}
      }


      StagerPutDoneHandler::~StagerPutDoneHandler() throw()
      {
      }



    }//end namespace dbService
  }//end namespace stager
}//end namespace castor

