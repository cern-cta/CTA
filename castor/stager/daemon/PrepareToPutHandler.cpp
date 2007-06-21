/**************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToPut request handler */
/************************************************************************************************/



#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"
#include "StagerPrepareToPutHandler.hpp"

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
#include "../SubRequestGetNextStatusCodes.hpp"

#include "../DiskCopyForRecall.hpp"

#include "../../exception/Exception.hpp"


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
	  
	  if(diskCopyForRecall == NULL){
	    /* we have to archive the subrequest */
	    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	    castor::exception::Exception ex(EBUSY);
	    ex.getMessage()<<"(StagerPrepareToPutHandler handle) Recreation is not possible"<<std::endl;
	    throw(ex);
	    
	  }
	  
	  /* updateSubrequestStatus Part: */
	  stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY); 
	
	  
	  /* replyToClient Part: */
	  /* to take into account!!!! if an exception happens, we need also to send the response to the client */
	  /* so copy and paste for the exceptions !!!*/
	  this->stgReplyHelper = new StagerReplyHelper;
	  if((this->stgReplyHelper) == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  this->stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0, "No error");
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper;

	}catch(castor::exception::Exception ex){
	  if(stgReplyHelper != NULL){
	    delete stgReplyHelper;
	  }
	  stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw(ex);
	}
	
      }


      


      StagerPrepareToPutHandler::~StagerPrepareToPutHandler()throw(){
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

