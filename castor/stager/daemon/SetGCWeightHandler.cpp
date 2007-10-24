/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "castor/stager/dbService/StagerSetGCHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "stager_constants.h"
#include "castor/stager/SetFileGCWeight.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerSetGCHandler::StagerSetGCHandler(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_SetFileGCWeight;
      }
      
      void StagerSetGCHandler::handle() throw(castor::exception::Exception)
      {

	StagerReplyHelper* stgReplyHelper=NULL;
	try{

	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/
	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_SETGC, 5 ,params, &(stgCnsHelper->cnsFileid));
	  

	
	  /* execute the main function for the setFileGCWeight request   */
	  /* basically a call to the corresponding stagerService method */
	  /* passing the SetFileGCWeight object                        */
	  castor::stager::SetFileGCWeight* setGCWeightReq = dynamic_cast<castor::stager::SetFileGCWeight*>(stgRequestHelper->fileRequest);
          stgRequestHelper->stagerService->setFileGCWeight(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server,setGCWeightReq->weight());

	   /* FOR THE SYSTEM PART:*/
	  castor::dlf::Param params[]={castor::dlf::Param{stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param{"DiskCopy Weight setted:",setGCWeightReq->weight()},
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_SETGC_DETAILS, 6,params, &(stgCnsHelper->cnsFileid));

	  /**************************************************/
	  /* we don t need to update the subrequestStatus  */
	  /* but we have to archive the subrequest        */
	  /* the same as for StagerRmHandler             */
	  stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());	 
	  
	  /* replyToClient Part: *//* we always have to reply to the client in case of exception! */
	
	  stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0,"No error");
    stgReplyHelper->endReplyToClient(stgRequestHelper);
    delete stgReplyHelper->ioResponse;
    delete stgReplyHelper;

	}catch(castor::exception::Exception e){
	  if(setFileGCWeight != NULL) delete setFileGCWeight;
	  if(stgReplyHelper != NULL) delete stgReplyHelper;
	   castor::dlf::Param params[]={castor::dlf::Param{"Error Code",sstrerror(e.code())},
				       castor::dlf::Param{"Error Message",e.getMessage().str()}
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_UPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw(e); 
       
	}
	
      }
      
      
      
      StagerSetGCHandler::~StagerSetGCHandler() throw()
      {
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
