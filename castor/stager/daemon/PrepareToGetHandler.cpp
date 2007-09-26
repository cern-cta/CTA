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

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "serrno.h"
#include <errno.h>

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
	  switchScheduling(caseToSchedule);
	 
	  
	  
	  if(caseToSchedule != 4){
	    stgReplyHelper = new StagerReplyHelper(this->newSubrequestStatus);
	    if(stgReplyHelper == NULL){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerPrepareToGetHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	      throw(ex);
	    }
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
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
