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
      }


      void StagerPrepareToGetHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  /* job oriented part */
	  jobOriented();
	  
	  /* scheduling Part */
	  /* first use the stager service to get the possible sources for the required file */
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, this->sources);
	  
	  switch(caseToSchedule){
	  case 0:
	    /* we archive the subrequest, we don't need to update the subrequest status afterwards */
	    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	    break;

	  case 2: //normal tape recall
	    
	    stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);//throw exception
	    break;
	    
	    
	  case 4:
	    break;

	    /* case 1: NEVER for a PrepareToGet (coming from the latest stager_db_service.c) */
	    
	    
	  default:
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerPrepareToGetHandler handle) stagerService->isSubRequestToSchedule returns an invalid value"<<std::endl;
	    throw (ex);
	    break;
	    
	  }
	  
	  
	  /* updateSubrequestStatus Part: */
	  if((caseToSchedule != 2)&&(caseToSchedule != 4)){
	    this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
	  }
	  if(caseToSchedule != 4){
	    /* replyToClient Part: */
	    /* to take into account!!!! if an exeption happens, we need also to send the response to the client */
	    /* so copy and paste for the exceptions !!!*/
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
	  }
	}catch(castor::exception::Exception e){

	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse != NULL) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerPrepareToGetHandler) Error"<<e.getMessage()<<std::endl;
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
