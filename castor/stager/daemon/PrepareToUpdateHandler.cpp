/*********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler */
/*******************************************************************************************************/



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"

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
     
      StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	/* error message needed for the exceptions, for the replyToClient... */
	this->message(message);
	
	/* we won' t execute the replication part so we don' t care about the maxReplicaNb or the replicationPolicy or useHostlist or whatever */
	
	
	this->xsize = this->stgRequestHelper->subrequest->xsize();
	if(this->xsize > 0){
	  
	  if( this->xsize < (this->stgCnsHelper->cnsFilestat.filesize) ){
	    /* print warning! */
	  }
	}else{
	  //we don' t do nothing????
	}
	
	
	this->openflags=RM_O_WRONLY; /* since we aren't gonna use the rm, we don't really need it */
	this->default_protocol = "rfio";
	
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToUpdate request  */
      /****************************************************************************************/
      void StagerPrepareToUpdateHandler::handle()
      {
	/**/
	try{


	  this.jobOriented();

	  this->diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);

	  if(diskCopyForRecall == NULL){
	    // we throw exception!!!
	  }


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
	  
	  this->stgReplyHelper->setAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid,serrno, message);
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  

	}catch{
	}
	
      }


      


      StagerPrepareToUpdateHandler::~StagerPrepareToUpdateHandler()throw(){
	
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

