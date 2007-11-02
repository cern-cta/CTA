
/************************************************************************************************/
/* StagerPutDoneHandler: Constructor and implementation of the PutDone request's handle        */
/******************************************************************************************** */

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"


#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"


#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPutDoneHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "castor/Constants.hpp"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

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
      
      StagerPutDoneHandler::StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {     	
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StagePutDoneRequest;

	
      }

       /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerPutDoneHandler::handlerSettings() throw(castor::exception::Exception)
      {	
	/* no settings */
      }

      void StagerPutDoneHandler::handle() throw(castor::exception::Exception)
      {
	StagerReplyHelper* stgReplyHelper= NULL;
	try{

	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/
	 
	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_PUTDONE, 5 ,params, &(stgCnsHelper->cnsFileid));
	  



	  jobOriented();/* until it will be explored */
	 
	  
	  if(stgRequestHelper->stagerService->processPutDone(stgRequestHelper->subrequest)){
	 
	    /* for the PutDone, if everything is ok, we archive the subrequest */
	    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	    
	    /* replyToClient Part: */
	    stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	    
	    delete stgReplyHelper;
	  }

	}catch(castor::exception::Exception e){
	  if(stgReplyHelper != NULL) delete stgReplyHelper;
	  
	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUTDONE, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw(e);
	}
      }





    }//end namespace dbService
  }//end namespace stager
}//end namespace castor

