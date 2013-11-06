
/************************************************************************************************/
/* PutDoneHandler: Constructor and implementation of the PutDone request's handle        */
/******************************************************************************************** */

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/PutDoneHandler.hpp"

#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"
#include "castor/Constants.hpp"
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

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
      
      bool PutDoneHandler::handle() throw(castor::exception::Exception)
      {
        RequestHandler::handle();

        ReplyHelper* stgReplyHelper = NULL;
        
        // check if file exists         
        reqHelper->statNameServerFile();
        if(serrno == ENOENT) {
          reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 0);
          castor::exception::Exception e(ENOENT);
          throw e;
        }

        // check write permissions
        if(0 != Cns_accessUser(reqHelper->subrequest->fileName().c_str(), W_OK,
                               reqHelper->fileRequest->euid(), reqHelper->fileRequest->egid())) {
          castor::exception::Exception ex(serrno);
          reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION, &(reqHelper->cnsFileid));
          throw ex;
        }
        
        // get/create CastorFile
        reqHelper->getCastorFile();
          
        try{
          reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PUTDONE, &(reqHelper->cnsFileid));
          
          switch(reqHelper->stagerService->processPutDoneRequest(reqHelper->subrequest)) {
            
            case -1:  // request put in wait
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(reqHelper->cnsFileid));
            break;
            
            case 0:   // error
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
            break;
            
            case 1:   // ok
            reqHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
            
            stgReplyHelper = new ReplyHelper();
            stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "No error");
            stgReplyHelper->endReplyToClient(reqHelper);
            delete stgReplyHelper;
            stgReplyHelper = 0;
            break;
          }
          
        }
        catch(castor::exception::Exception& e){
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          
          castor::dlf::Param params[] = {
            castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUTDONE, 2, params, &(reqHelper->cnsFileid));
          throw e;
        }
        return true;
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
