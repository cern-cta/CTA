/**************************************************************************************************/
/* PrepareToPutHandler: Constructor and implementation of the PrepareToPut request handler */
/************************************************************************************************/



#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/PrepareToPutHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
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
      
      PrepareToPutHandler::PrepareToPutHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePrepareToPutRequest;
      }
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void PrepareToPutHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        /* for the moment, there isnt needed to set it */
      }
      
      
      
      
      /****************************************************************************************/
      /* handler for the PrepareToPut request  */
      /****************************************************************************************/
      void PrepareToPutHandler::handle() throw(castor::exception::Exception)
      {
        ReplyHelper* stgReplyHelper=NULL;
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          
          /**********/
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOPUT, &(stgCnsHelper->cnsFileid));
          
          jobOriented();
          
          /* use the stagerService to recreate castor file */
          castor::stager::DiskCopyForRecall* diskCopyForRecall =
            stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
          
          
          if(diskCopyForRecall == NULL){
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(stgCnsHelper->cnsFileid));
          }
          else{
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(stgCnsHelper->cnsFileid));
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_READY);
            
            /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
            stgReplyHelper = new ReplyHelper();
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "No Error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            
            delete stgReplyHelper;
            delete diskCopyForRecall;
          }
          
        }catch(castor::exception::Exception& e){
          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOPUT, 2 ,params, &(stgCnsHelper->cnsFileid));
          
          throw(e);
        }
        
      }
      
      
      
      
      
      PrepareToPutHandler::~PrepareToPutHandler()throw(){
      }
      
      
      
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 

