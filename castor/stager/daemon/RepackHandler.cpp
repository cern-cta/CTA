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
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	/* error message needed for the exceptions, for the replyToClient... */
	this->message(message);
	
	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif
	/* there isn't any size requirement for a readMode request */
	this->xsize=0;
	this->openflags=RM_O_RDONLY;
	this->default_protocol = "rfio";
	
      }


      void StagerRepackHandler::handle()
      {
	/**/
	try{

	  /* job oriented part */
	  this.jobOriented();

	  /* scheduling Part */
	  /* first use the stager service to get the possible sources for the required file */
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToBeScheduled(stgRequestHelper->subrequest, &(this->sources));
	  switchScheduling(caseToSchedule);

	  
	  /* build the rmjob struct and submit the job */
	  this->rmjob = stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	  buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	  rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)); //throw exception



	  /* updateSubrequestStatus Part: */
	  SubRequestStatusCode currentSubrequestStatus = stgRequestHelper->subrequest->status();
	  SubRequestStatusCodes newSubrequestStatus = SUBREQUEST_READY;

	  if(newSubrequestStatus != currentSubrequestStatus){//common for the StagerGetRequest
	  
	    stgRequestHelper->subrequest->setStatus(newSubrequestStatus);
	    stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUSFILESTAGED);
	    
	  }



	  /* replyToClient Part: */
	  /* to take into account!!!! if an exeption happens, we need also to send the response to the client */
	  /* so copy and paste for the exceptions !!!*/
	  this->stgReplyHelper = new StagerReplyHelper*;

	  this->stgReplyHelper->friendSetAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid,serrno, message);
	  this->stgReplyHelper->friendEndReplyToClient(stgRequestHelper);
	 
	  

	}catch{
	}
	
      }



      /***********************************************************************/
      /*    destructor                                                      */
      StagerRepackHandler::~StagerRepackHandler()
      {
	
	delete stgReplyHelper;
      }

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
