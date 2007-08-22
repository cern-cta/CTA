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
     
      StagerPrepareToPutHandler::StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
	/* since we don't call the rm: we don't care about maxReplicaNb, ...xsize, ...  */	
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToPut request  */
      /****************************************************************************************/
      void StagerPrepareToPutHandler::handle() throw(castor::exception::Exception)
      {
	StagerReplyHelper* stgReplyHelper=NULL;
	try{
	  
	  jobOriented();
	  
	  /* use the stagerService to recreate castor file */
	  castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	  
	  
	  if(diskCopyForRecall == NULL){
	    /* WE DONT DO ANYTHING  */
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

	}catch(castor::exception::Exception e){
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete (stgReplyHelper->ioResponse);
	    delete stgReplyHelper;
	  }
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerPrepareToPutHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	  throw(ex);
	}
	
      }


      


      StagerPrepareToPutHandler::~StagerPrepareToPutHandler()throw(){
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

