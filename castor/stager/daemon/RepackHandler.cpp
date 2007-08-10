/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerRepackHandler.hpp"

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

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
	
	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


      	
	/* get the request's size required on disk */
	/* depending if the file exist, we ll need to update this variable */
	this->xsize = stgRequestHelper->subrequest->xsize();

	if( xsize > 0){
	  if( xsize < (stgCnsHelper->cnsFilestat.filesize)){
	    /* warning, user is requesting less bytes than the real size */
	    /* just print a message. we don't update xsize!!! */	 
	  }

	}else{
	    xsize = stgCnsHelper->cnsFilestat.filesize;
	    /* before enter the job, we ll need to print a message */
	}

	this->default_protocol = "rfio";
	
      }


      void StagerRepackHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	try{
	  /* job oriented part */
	  jobOriented();
	  
	  /* scheduling Part */
	  /* first use the stager service to get the possible sources for the required file */
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, this->sources);
	  switchScheduling(caseToSchedule);

	  /* updateSubrequestStatus Part: */
	  if((caseToSchedule != 2) && (caseToSchedule != 4)){
	    stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
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
	 
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse != NULL) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerRepackHandler) Error"<<e.getMessage()<<std::endl;
	  throw ex;
	}
      }


      /***********************************************************************/
      /*    destructor                                                      */
      StagerRepackHandler::~StagerRepackHandler() throw()
      {
      }

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
