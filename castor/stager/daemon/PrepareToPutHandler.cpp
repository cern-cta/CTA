/**************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToPut request handler */
/************************************************************************************************/



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"

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
#include "castor/stager/DiskCopyForRecall.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerPrepareToPutHandler::StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	/* error message needed for the exceptions, for the replyToClient... */
	this->message(message);
	
	/* since we don't call the rm: we don't care about maxReplicaNb, ...xsize, ...  */	
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToPut request  */
      /****************************************************************************************/
      void StagerPrepareToPutHandler::handle()
      {
	/**/
	try{


	  this.jobOriented();
	  
	  /* use the stagerService to recreate castor file */
	  castor::stager::DiskCopyForRecall* diskCopyForRecall;
	  diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);

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


      


      StagerPrepareToPutHandler::~StagerPrepareToPutHandler()throw(){
	delete stgReplyHelper;
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

