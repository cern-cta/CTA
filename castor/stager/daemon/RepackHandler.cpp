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

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StageRepackRequest;
	
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
	
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status(); 
      }


      /*******************************************/	
      /*     switch(getDiskCopyForJob):         */  
      /*        case 0: (staged) call startRepackMigration (once implemented) */                                   
      /*        case 1: (staged) waitD2DCopy  */
      /*        case 2: (waitRecall) createRecallCandidate */
      void StagerRepackHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
	switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,this->sources)){
	case 0:
	  {
	    /* to be done */
	    /* stgRequestHelper->stagerService->startRepackMigration */
	  }break;
	case 1:
	  {
	     bool isToReplicate= replicaSwitch();
	    if(isToReplicate){
	      processReplica();
	    }
	    
	    /* build the rmjob struct and submit the job */
	    jobManagerPart();
	    
	    this->newSubrequestStatus = SUBREQUEST_READYFORSCHED;
	    if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
	      stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      /* we have to setGetNextStatus since the newSub...== SUBREQUEST_READYFORSCHED */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); 
	    }

	    /* and we have to notify the jobManager */
	    this->notifyJobManager();
	    
	  }break;
	case 2:
	  {
	    stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	    /* though we wont update the subrequestStatus, we need to flag it for the stgReplyHelper */
	    this->newSubrequestStatus= SUBREQUEST_READY;
	  }break;

	}//end switch

      }
      


      void StagerRepackHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	StagerReplyHelper* stgReplyHelper= NULL;
	try{

	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/


	  /* job oriented part */
	  jobOriented();
	  
	  /* depending on the value returned by getDiskCopiesForJob */
	  /* if needed, we update the subrequestStatus internally */
	  switchDiskCopiesForJob();
	 
	  stgReplyHelper = new StagerReplyHelper(this->newSubrequestStatus);
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
	    if(stgReplyHelper->ioResponse != NULL) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	  
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerRepackHandler) Error"<<e.getMessage().str()<<std::endl;
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
