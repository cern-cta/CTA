
/************************************************************************************************/
/* PutDoneHandler: Constructor and implementation of the PutDone request's handle        */
/******************************************************************************************** */

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"


#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"


#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/PutDoneHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "castor/Constants.hpp"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

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
      
      PutDoneHandler::PutDoneHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {     	
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StagePutDoneRequest;
        
        
      }
      
      
      void PutDoneHandler::handle() throw(castor::exception::Exception)
      {
        ReplyHelper* stgReplyHelper= NULL;
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
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
            
            stgReplyHelper = new ReplyHelper();
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0,  "No error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            delete stgReplyHelper;
            stgReplyHelper = 0;
            break;
          }
          
        }
        catch(castor::exception::Exception& e){
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUTDONE, 2 ,params, &(stgCnsHelper->cnsFileid));
          throw(e);
        }
      }
      
      
      
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor

