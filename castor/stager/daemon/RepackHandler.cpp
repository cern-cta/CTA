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
     
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
	
	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif
      	
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

	this->openflags=RM_O_RDONLY;
	this->default_protocol = "rfio";
	
	this->caseSubrequestFailed = false;
      }


      void StagerRepackHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	try{
	  /* job oriented part */
	  jobOriented();
	  
	  /* scheduling Part */
	  /* first use the stager service to get the possible sources for the required file */
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, this->sources);/*84 */
	  switchScheduling(caseToSchedule);
	  
	  if((rfs.empty()) == false){
	    /* if the file exists we don't have any size requirements */
	    this->xsize = 0;
	  }
	  
	  /* build the rmjob struct and submit the job */
	  stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	  buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	  if(rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Error on rm_enterjob"<<std::endl;
	    throw ex;
	  }
	  rm_freejob(rmjob_out);
	
	  if(this->caseSubrequestFailed == false){
	    /* updateSubrequestStatus Part: */
	    if((caseToSchedule != 2) && (caseToSchedule != 0)){
	      stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
	    }
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

	}catch(castor::exception::Exception ex){
	  if(rmjob_out != NULL){
	    rm_freejob(rmjob_out);
	  }
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse != NULL) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw(ex);
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
