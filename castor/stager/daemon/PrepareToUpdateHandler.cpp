/***********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"
#include "StagerPrepareToUpdateHandler.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../IClientFactory.hpp"
#include "../SubRequestStatusCodes.hpp"
#include "../DiskCopyForRecall.hpp"

#include "../../exception/Exception.hpp"


#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, bool toRecreateCastorFile) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
		
	/* depending on this flag, we ll execute the huge flow or the small one*/
	this->toRecreateCastorFile = toRecreateCastorFile;

	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
	
	
	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif


	/* get the subrequest's size required on disk */
	this->xsize = this->stgRequestHelper->subrequest->xsize();
	if(this->xsize > 0){
	  
	  if( this->xsize < (this->stgCnsHelper->cnsFilestat.filesize) ){
	    /* print warning! */
	  }
	}else{
	  /* we get the defaultFileSize */
	  xsize = stgRequestHelper->svcClass->defaultFileSize();
	  if( xsize <= 0){
	    xsize = DEFAULTFILESIZE;
	  }
	}
	
	
	this->openflags=RM_O_RDWR; /* since we aren't gonna use the rm, we don't really need it */
	this->default_protocol = "rfio";
	
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToUpdate request  */
      /****************************************************************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	try{
	  jobOriented();

	  
	  /*************************/
	  /* huge or small flow?? */
	  /***********************/
	  
	  if( toRecreateCastorFile){/* we recreate, small flow*/
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      /* we archive the subrequest */
	      stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	      castor::exception::Exception ex(EBUSY);
	      ex.getMessage()<<"(StagerPrepareToUpdateHandler handle) Recreation is not possible"<<std::endl;
	      throw(ex);
	    }
	    /* updateSubrequestStatus Part: */
	    stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);

	  }else{/* we schedule, huge flow */
	    
	    int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, &(this->sources));
	    switchScheduling(caseToSchedule);
	    
	    /* build the rmjob struct and submit the job */
	    stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	    buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	    if(rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)) != 0){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerPrepareToUpdateHandler handle) Error on rm_enterjob"<<std::endl;
	      throw ex;
	    }
	    rm_freejob(this->rmjob_out);

	    /* updateSubrequestStatus Part: */
	    if((caseToSchedule != 2) && (caseToSchedule != 0)){
	      stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);	    
	    }
	  }
	  
	 
	 
	  /* replyToClient Part: */
	  this->stgReplyHelper = new StagerReplyHelper;
	  if((this->stgReplyHelper) == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  this->stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0,"No Error");
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper;
	
	}catch(castor::exception::Exception ex){

	  if(stgReplyHelper != NULL){
	    delete stgReplyHelper;
	  }
	  if(rmjob_out != NULL){
	    rm_freejob(this->rmjob_out);
	  }
	  stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw(ex);
	}
      }


      


      StagerPrepareToUpdateHandler::~StagerPrepareToUpdateHandler()throw(){
	
      }
      
      

      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

