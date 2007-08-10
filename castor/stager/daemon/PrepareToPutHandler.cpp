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
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"


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
      }


      
      /****************************************************************************************/
      /* handler for the PrepareToPut request  */
      /****************************************************************************************/
      void StagerPrepareToPutHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  jobOriented();
	  
	  /* use the stagerService to recreate castor file */
	  castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	  
	  
	  if(diskCopyForRecall != NULL){
	    /* updateSubrequestStatus Part: */
	    stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY); 	    
	    
	    /* replyToClient Part: */
	    this->stgReplyHelper = new StagerReplyHelper;
	    if((this->stgReplyHelper) == NULL){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	      throw(ex);
	    }
	    this->stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0, "No error");
	    this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	    delete (stgReplyHelper->ioResponse);
	    delete stgReplyHelper;
	  }else{
	    /* in this case we have to archiveSubrequest */
	    /* updateStatus to FAILED_FINISHED and replyToClient (both to be dones on StagerDBService, catch exception) */
	    this->stgRequestHelper->stagerService->archiveSubReq(this->stgRequestHelper->subrequest->id());
	    castor::exception::Exception ex(EBUSY);
	    ex.getMessage()<<"(StagerPrepareToPutHandler handle) Recreation is not possible (null DiskCopyForRecall)"<<std::endl;
	    throw ex;
	  }

	}catch(castor::exception::Exception ex){
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete (stgReplyHelper->ioResponse);
	     delete stgReplyHelper;
	  }
	  throw(ex);
	}
	
      }


      


      StagerPrepareToPutHandler::~StagerPrepareToPutHandler()throw(){
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

