/***********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

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
	
	
	this->default_protocol = "rfio";
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
	
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToUpdate request  */
      /****************************************************************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	StagerReplyHelper* stgReplyHelper=NULL;
	try{
	  
	 
	  jobOriented();

	  
	  int caseToSchedule=-1;
	  /*************************/
	  /* huge or small flow?? */
	  /***********************/
	  
	  if( toRecreateCastorFile){/* we recreate, small flow*/
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      /* WE DONT DO ANYTHING */
	    }else{

	      /* we update the subrequestSatus*/
	      this->newSubrequestStatus = SUBREQUEST_READY;
	      if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
		stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);
		/* since newSubrequestStatus == SUBREQUEST_READY, we have to setGetNextStatus */
		stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);

		/* we are gonna replyToClient so we dont  updateRep on DB explicitly */
	      }

	      /*  replyToClient  */
	      stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);
	      if(stgReplyHelper == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
		throw(ex);
	      }
	      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0,"No Error");
	      stgReplyHelper->endReplyToClient(stgRequestHelper);
	      delete stgReplyHelper->ioResponse;
	      delete stgReplyHelper; 
	      
	    }
	    
	  }else{/* we schedule, huge flow */
	    
	    caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, this->sources);
	    switchScheduling(caseToSchedule);
	    /* we update the subrequestStatus internally */
	    
	    stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);
	    if(stgReplyHelper == NULL){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	      throw(ex);
	    }
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0,"No Error");
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	    delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper; 
	  
	  }  
	
	}catch(castor::exception::Exception e){

	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	 
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerPrepareToUpdateHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	}
      }


      


      StagerPrepareToUpdateHandler::~StagerPrepareToUpdateHandler()throw(){
	
      }
      
      

      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

