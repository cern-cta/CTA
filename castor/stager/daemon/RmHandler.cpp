
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the StagerRequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/

#include "StagerRmHandler.hpp"
#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"



#include "../IStagerSvc.hpp"

#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../IClientFactory.h"
#include "../SubRequestStatusCodes.hpp"
#include "../SubRequestGetNextStatusCodes.hpp"
#include "../../exception/Exception.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	
      }

      void StagerRmHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  /* execute the main function for the rm request                 */
	  /* basically, a call to the corresponding stagerService method */
	  std::string server(stgCnsHelper->cnsFileid.server);
	  stgRequestHelper->stagerService->stageRm(stgCnsHelper->cnsFileid.fileid, server);
	  
	  /**************************************************/
	  /* we don t need to update the subrequestStatus  */
	  /* but we have to archive the subrequest        */
	  /* the same as for StagerSetGCHandler          */
	  stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());

	  /* replyToClient Part: *//* we always have to reply to the client in case of exception! */
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
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}
      }


      StagerRmHandler::~StagerRmHandler() throw()
      {
      }
      
    }//end dbService
  }//end stager
}//end castor
