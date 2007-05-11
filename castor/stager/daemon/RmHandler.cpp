
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the StagerRequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/

#include "castor/stager/dbService/StagerRmHandler.hpp"
#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	/* error message needed for the exceptions, for the replyToClient... */
	this->message(message);
      }


      StagerRmHandler::handle()
      {
	
	
	try{
	  
	  /* execute the main function for the rm request                 */
	  /* basically, a call to the corresponding stagerService method */
	  stgRequestHelper->stagerService->stageRm(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server);
	 
	  /**********************************************************************************************/
	  /* once the rm request is done, we update the status (if needed) and we reply to the client  */

	  /* check subrequest status and update if needed */
	  SubRequestStatusCode currentSubrequestStatus = stgRequestHelper->subrequest->status();
	  SubRequestStatusCodes newSubrequestStatus = SUBREQUEST_READY;
	  
	  if(newSubrequestStatus != currentSubrequestStatus){
	    stgRequestHelper->subrequest->setStatus(newSubrequestStatus);
	    stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUSFILESTAGED);
	    
	  }
	  	  
	  /* replyToClient Part: *//* we always have to reply to the client in case of exception! */
	  this->stgReplyHelper = new StagerReplyHelper*;	  
	  this->stgReplyHelper->setAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid,serrno, message);
	  this->stgReplyHelper->endReplyToClient(stgRequestHelper);
	  
	  
	}catch{
	}
      }


      StagerRmHandler::~StagerRmHandler()
      {
	delete stgReplyHelper;
      }
      
    }//end dbService
  }//end stager
}//end castor
