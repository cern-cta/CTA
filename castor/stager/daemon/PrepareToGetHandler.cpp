/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"


#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      StagerPrepareToGetHandler::StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;

	this->maxReplicaNb= this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
	
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
      }


      void StagerPrepareToGetHandler::handle() throw(castor::exception::Exception)
      {
	StagerReplyHelper* stgReplyHelper=NULL;
	try{
	
	  jobOriented();
	  
	  /* scheduling Part */
	  /* first use the stager service to get the possible sources for the required file */
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, this->sources);
	  
	  switch(caseToSchedule){

	  case 0:
	    /* we update the subrequestStatus*/
	    this->newSubrequestStatus = SUBREQUEST_READY;
	    if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
	      stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);
	      /* since newSubrequestStatus == SUBREQUEST_READY, we have to setGetNextStatus */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
	      
	      /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
	    }

	    /* we archive the subrequest*/
	    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());

	    /* we will replyToClient*/
	    break;


	  case 2: //normal tape recall
	    try{
	      stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);//throw exception
	      
	      /* we dont change subrequestStatus or archive the subrequest: but we need to set the newSubrequestStatus to replyToClient */
	      this->newSubrequestStatus = SUBREQUEST_READY;

	    }catch(castor::exception::Exception ex){
	      /* internally of the createRecallCandidate: we reply to the client and we setSubrequestStatus to FAILED */
	      return;
	    }
	    break;
	    
	    
	  case 4:
	    /* we dont archiveSubrequest, changeSubrequestStatus or ReplyToClient */
	    break;

	    /* case 1: NEVER for a PrepareToGet (coming from the latest stager_db_service.c) */
	    
	    
	  default:
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerPrepareToGetHandler handle) stagerService->isSubRequestToSchedule returns an invalid value"<<std::endl;
	    throw (ex);
	    break;
	    
	  }
	  
	  
	  if(caseToSchedule != 4){
	    stgReplyHelper = new StagerReplyHelper(this->newSubrequestStatus);
	    if(stgReplyHelper == NULL){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerPrepareToGetHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	      throw(ex);
	    }
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0, "No error");
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	    delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }

	}catch(castor::exception::Exception e){

	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse != NULL) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerPrepareToGetHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	}  

      }

	
      /***********************************************************************/
      /*    destructor                                                      */
      StagerPrepareToGetHandler::~StagerPrepareToGetHandler() throw()
      {
	
      }
      
    }//end namespace dbservice
  }//end namespace stager
}//end namespace castor
