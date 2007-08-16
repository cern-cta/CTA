/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "castor/stager/dbService/StagerSetGCHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"


#include "castor/stager/SetFileGCWeight.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

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
	this->setFileGCWeight = new castor::stager::SetFileGCWeight(*(dynamic_cast<castor::stager::SetFileGCWeight*> (stgRequestHelper->fileRequest)));
	if(this->setFileGCWeight == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerSetFileGCWeightHandler constructor) Impossible to get setFileGCWeight"<<std::endl;
	  throw ex;
	}
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
      }

      void StagerSetGCHandler::handle() throw(castor::exception::Exception)
      {
	StagerReplyHelper* stgReplyHelper;
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
	  this->newSubrequestStatus = SUBREQUEST_READY;
	  stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);
	  if(stgReplyHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	    throw(ex);
	  }
	  stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,0,"No error");
	  stgReplyHelper->endReplyToClient(stgRequestHelper);
	  delete stgReplyHelper->ioResponse;
	  delete stgReplyHelper;

	}catch(castor::exception::Exception ex){
	  if(setFileGCWeight != NULL){
	    delete setFileGCWeight;
	  }
	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	 
	  throw ex;
	}
	
      }



      StagerSetGCHandler::~StagerSetGCHandler() throw()
      {
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
