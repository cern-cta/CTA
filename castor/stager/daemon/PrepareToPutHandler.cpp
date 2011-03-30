/**************************************************************************************************/
/* PrepareToPutHandler: Constructor and implementation of the PrepareToPut request handler */
/************************************************************************************************/



#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/PrepareToPutHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
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
                  
      /* handler for the PrepareToPut request  */
      void PrepareToPutHandler::handle() throw(castor::exception::Exception)
      {
        // Inherited behavior
        OpenRequestHandler::handle();
        
        handlePut();
      }
      
      void PrepareToPutHandler::handlePut() throw(castor::exception::Exception)
      {        
        ReplyHelper* stgReplyHelper=NULL;
        try {
          reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOPUT, &(reqHelper->cnsFileid));
          /* use the stagerService to recreate castor file */
          castor::stager::DiskCopyForRecall* diskCopyForRecall =
            reqHelper->stagerService->recreateCastorFile(reqHelper->castorFile,reqHelper->subrequest);
          if(diskCopyForRecall == NULL){
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(reqHelper->cnsFileid));
          }
          else{
            reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(reqHelper->cnsFileid));
            reqHelper->subrequest->setStatus(SUBREQUEST_READY);
            
            /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
            stgReplyHelper = new ReplyHelper();
            stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "No Error");
            stgReplyHelper->endReplyToClient(reqHelper);
            delete stgReplyHelper;
            delete diskCopyForRecall;
          }
        } catch(castor::exception::Exception& e) {          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };          
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOPUT, 2 ,params, &(reqHelper->cnsFileid));
          throw(e);
        }  
      }
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 
