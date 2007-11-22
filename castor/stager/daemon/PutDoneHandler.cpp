
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
      
      StagerPutDoneHandler::StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {     	
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StagePutDoneRequest;
        
        
      }
      
      
      void StagerPutDoneHandler::handle() throw(castor::exception::Exception)
      {
        StagerReplyHelper* stgReplyHelper= NULL;
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PUTDONE, &(stgCnsHelper->cnsFileid));
          
          jobOriented();/* until it will be explored */
          
          
          switch(stgRequestHelper->stagerService->processPutDoneRequest(stgRequestHelper->subrequest)) {
            
            case -1:  // request put in wait
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
            break;
            
            case 0:   // error
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            break;
            
            case 1:   // ok
            /* we archive the subrequest */
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_ARCHIVED);
            stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
            
            /* replyToClient Part: */
            stgReplyHelper = new StagerReplyHelper();
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0,  "No error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            delete stgReplyHelper;
            break;
          }
          
        }
        catch(castor::exception::Exception e){
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

