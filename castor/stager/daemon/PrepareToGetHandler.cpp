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
#include "rm_api.h"
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


	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif
	
	/* get the request's size required on disk */
	/* depending if the file exist, we ll need to update this variable */
	this->xsize = this->stgRequestHelper->subrequest->xsize();

	if( xsize > 0 ){
	  if(xsize < (stgCnsHelper->cnsFilestat.filesize)){
	    /* warning, user is requesting less bytes than the real size */
	    //just print message
	  }

	  
	}else{
	  this->xsize = stgCnsHelper->cnsFilestat.filesize;
	}


	this->openflags=RM_O_RDONLY;
	this->default_protocol = "rfio";
	
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
	    
	  default:
	    /* second, process as a tapeRecall, as a replica or just change the subrequest.status */
	    /* it corresponds to the huge switch on the stager_db_service.c  */
	    switchScheduling(caseToSchedule);
	    
	    if((rfs.empty()) == false){
	      /* if the file exists we don't have any size requirements */
	      this->xsize = 0;
	    }
	    
	    /* build the rmjob struct and submit the job */
	    stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	    buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	    if(rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)) != 0 ){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerPrepareToGetHandler handle) Error on rm_enterjob"<<std::endl;
	      throw(ex);
	    }	
	    rm_freejob(rmjob_out);
	    break;
	  }
	  
	  /* updateSubrequestStatus Part: */
	  if(caseToSchedule != 2){
	    this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
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

	}catch(castor::exception::Exception ex){
	  if(rmjob_out != NULL){
	    rm_freejob(rmjob_out);
	  }
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}

      }


      /********************************************************************************/
      /* we are overwritting this function inherited from the StagerJobRequestHandler */
      /* because of the case 0                                                      */
      /* after asking the stagerService is it is toBeScheduled                     */
      /* - do a normal tape recall                                                */
      /* - check if it is to replicate:                                          */
      /*         +processReplica if it is needed:                               */
      /*                    +make the hostlist if it is needed                 */
      /* now we don't have anymore the case 0 for the PrepareToGet */
      /************************************************************************/
      void StagerPrepareToGetHandler::switchScheduling(int caseToSchedule) throw(castor::exception::Exception)
      {
	
	switch(caseToSchedule){

	case 2: //normal tape recall
	 
	  stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid());//throw exception
	  
	  break;


	case 1:	 
	  
	  bool isToReplicate=replicaSwitch();
	  
	  if(isToReplicate){

	    processReplicaAndHostlist();
	  }

	  break;

	
	default:
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerPrepareToGetHandler handle) stagerService->isSubRequestToSchedule returns an invalid value"<<std::endl;
	  throw (ex);
	  break;
	}
	
      }//end switchScheduling
      

      /***********************************************************************/
      /*    destructor                                                      */
      StagerPrepareToGetHandler::~StagerPrepareToGetHandler() throw()
      {
	
      }
      
    }//end namespace dbservice
  }//end namespace stager
}//end namespace castor
