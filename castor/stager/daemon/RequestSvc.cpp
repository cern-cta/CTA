/*************************************/
/* main class for the ___RequestSvc */
/***********************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/RequestSvc.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cglobals.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"


#include "castor/Constants.hpp"

#include "marshall.h"
#include "net.h"

#include "getconfent.h"
#include "Cnetdb.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>
#include <list>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>



namespace castor{
  namespace stager{
    namespace dbService{


      /*************************************************************/
      /* Method to get a subrequest to do using the StagerService */
      /***********************************************************/
      castor::IObject* RequestSvc::select() throw() {
        castor::IService* svc =
          castor::BaseObject::services()->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
        castor::stager::IStagerSvc* stgService =
          dynamic_cast<castor::stager::IStagerSvc*>(svc);
        // we have already initialized it in the main, so we know the pointer is valid
        return stgService->subRequestToDo(m_name);
      }
     
     

      /**********************************************************************************/
      /* function to handle the exception (set subrequest to FAILED and replyToClient) */
      /* things to do: */
      /********************************************************************************/
      void RequestSvc::handleException(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, int errorCode, std::string errorMessage){
	
	const char* errorMessageC = errorMessage.c_str();

	/* we have to set the subrequest status to SUBREQUEST_FAILED_FINISHED */
	/* but not setGetNextSubrequestStatus!!  */
	if(stgRequestHelper->subrequest != NULL) stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED_FINISHED);
	
	
	/***/
	/* reply to the client in case of error*/
	if(stgRequestHelper->iClient != NULL){
	  StagerReplyHelper *stgReplyHelper = new StagerReplyHelper();
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,errorCode,errorMessageC);
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	    delete stgReplyHelper;
	}else{
	  if((stgRequestHelper->dbService)&&(stgRequestHelper->subrequest)) stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	}
	
	
      }
    
         
        
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
