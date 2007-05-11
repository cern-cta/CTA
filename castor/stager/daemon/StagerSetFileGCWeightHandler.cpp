/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "castor/stager/dbService/StagerSetFileGCWeightHandler.hpp"
#include "castor/stager/StagerRequestHelper.hpp"
#include "castor/stager/StagerCnsHelper.hpp"
#include "castor/stager/StagerReplyHelper.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SetFileGCWeitght.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerSetFileGCWeightHandler::StagerSetFileGCWeightHandler(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper, std::string message)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	/* error message needed for the exceptions, for the replyToClient... */
	this->message(message);

	/* main object needed to perform the SetFileGCRequest processing */
	this->setFileGCWeight = new castor::stager::SetFileGCWeight(stgRequestHelper->filerequest);



      }
      StagerSetFileGCWeightHandler::handle()
      {

	try{

	  /* execute the main function for the setFileGCWeight request   */
	  /* basically a call to the corresponding stagerService method */
	  /* passing the SetFileGCWeight object                        */
	  stgRequestHelper->stagerService->setFileGCWeight(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server,setFileGCWeight->weight());
	  
	  
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
	  
	  stgReplyHelper->setAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid,serrno, message);
	  stgReplyHelper->endReplyToClient(stgRequestHelper);
	  
	  
	}catch{
	}
      }


      StagerSetFileGCWeightHandler::~StagerSetFileGCWeightHandler()
      {
	delete stgReplyHelper;
	delete setFileGCWeight;
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
