
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the RequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/

#include "castor/stager/daemon/RmHandler.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"



#include "castor/stager/IStagerSvc.hpp"

#include "stager_constants.h"

#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"


#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      
      
      /* constructor */
      RmHandler::RmHandler(RequestHelper* stgRequestHelper) throw() :
      RequestHandler()
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StageRmRequest;
      }
      
      
      void RmHandler::preHandle() throw(castor::exception::Exception)
      {
        
        /* get the uuid request string version and check if it is valid */
        stgRequestHelper->setRequestUuid();
        
        /* we create the CnsHelper inside and we pass the requestUuid needed for logging */
        this->stgCnsHelper = new CnsHelper(stgRequestHelper->requestUuid);
                
        /* set the username and groupname needed to print them on the log */
        stgRequestHelper->setUsernameAndGroupname();
        
        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        // the rest (getting svcClass, checking nameServer) is done in the handle
      }
        
      
      void RmHandler::handle() throw(castor::exception::Exception)
      {
        // check the existence of the file. Don't stop if ENOENT
        try {
          stgCnsHelper->checkFileOnNameServer(stgRequestHelper->subrequest, stgRequestHelper->svcClass);

          // check if the user (euid,egid) has the right permission for the request's type. otherwise throw exception
          stgRequestHelper->checkFilePermission(false, stgCnsHelper);
        }
        catch(castor::exception::Exception e) {
          if(serrno != ENOENT) {
            throw e;
          }
          // else the file does not exist, go on and try to cleanup stager db.
          // Note that in this case we don't check permissions, but that's fine as
          // the cleanup would anyway need to be done
        }

        // check the service class, and handle the '*' case
        std::string svcClassName = stgRequestHelper->fileRequest->svcClassName();
        u_signed64 svcClassId = 0;

        if(svcClassName != "*") {
          if(svcClassName.empty()) {
            svcClassName = "default";
          }
          stgRequestHelper->svcClass = stgRequestHelper->stagerService->selectSvcClass(svcClassName);
          if(stgRequestHelper->svcClass == NULL) {
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_SVCCLASS_EXCEPTION, &(stgCnsHelper->cnsFileid));

            castor::exception::Exception ex(SEINTERNAL);
            ex.getMessage() << "Service class '" << svcClassName << "' not found";
            throw(ex);
          }
          svcClassId = stgRequestHelper->svcClass->id();
        }
          
        ReplyHelper* stgReplyHelper=NULL;
        try{
          // try to perform the stageRm; internally, the method checks for non existing files
          if(stgRequestHelper->stagerService->stageRm(stgRequestHelper->subrequest,
            stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server, svcClassId)) {
            
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_ARCHIVED);
            
            stgReplyHelper = new ReplyHelper();	  
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "No error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            delete stgReplyHelper; 
            stgReplyHelper = NULL;
            stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
          }
          else {
	    // user error, log it, only in case the file existed
	    // Otherwise, it is an internal double check that failed and this is not really relevant
	    if (stgCnsHelper->cnsFileid.fileid > 0) {
	      stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
	    }
          }
        }
        catch(castor::exception::Exception e){
          if(stgReplyHelper != NULL) { 
	    delete stgReplyHelper;
	    stgReplyHelper = NULL;
	  }
          castor::dlf::Param params[] = {
            castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())};
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_RM, 2, params, &(stgCnsHelper->cnsFileid));
          throw(e);
        }
      }
      
    }//end daemon
  }//end stager
}//end castor
