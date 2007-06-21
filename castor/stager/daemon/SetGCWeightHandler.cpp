/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "StagerSetGCHandler.hpp"

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"


#include "../SetFileGCWeight.hpp"

#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"
#include "../IStagerSvc.hpp"
#include "../SubRequest.hpp"
#include "../SetFileGCWeight.hpp"
#include "../FileRequest.hpp"
#include "../SubRequestStatusCodes.hpp"
#include "../SubRequestGetNextStatusCodes.hpp"

#include "../../exception/Exception.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerSetGCHandler::StagerSetGCHandler(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;


	/* main object needed to perform the SetFileGCRequest processing */
	this->setFileGCWeight = new castor::stager::SetFileGCWeight(stgRequestHelper->fileRequest);
	if(this->setFileGCWeight == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerSetFileGCWeightHandler constructor) Impossible to get setFileGCWeight"<<std::endl;
	  throw ex;
	}

      }

      void StagerSetGCHandler::handle() throw(castor::exception::Exception)
      {
       
	try{
	  /* execute the main function for the setFileGCWeight request   */
	  /* basically a call to the corresponding stagerService method */
	  /* passing the SetFileGCWeight object                        */
	  stgRequestHelper->stagerService->setFileGCWeight(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server,setFileGCWeight->weight());
	  delete setFileGCWeight;

	  /**************************************************/
	  /* we don t need to update the subrequestStatus  */
	  /* but we have to archive the subrequest        */
	  /* the same as for StagerRmHandler             */
	  stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());	 
	  
	  /* replyToClient Part: *//* we always have to reply to the client in case of exception! */
	  this->stgReplyHelper = new StagerReplyHelper;
	  if((this->stgReplyHelper) == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0,"No error");
	  stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper;

	}catch(castor::exception::Exception ex){
	  if(setFileGCWeight != NULL){
	    delete setFileGCWeight;
	  }
	  if(stgReplyHelper != NULL){
	    delete stgReplyHelper;
	  }
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}
	
      }



      StagerSetGCHandler::~StagerSetGCHandler() throw()
      {
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
