/**************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToPut request handler */
/************************************************************************************************/



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
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
      
      StagerPrepareToPutHandler::StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePrepareToPutRequest;
      }
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerPrepareToPutHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        /* for the moment, there isnt needed to set it */
      }
      
      
      
      
      /****************************************************************************************/
      /* handler for the PrepareToPut request  */
      /****************************************************************************************/
      void StagerPrepareToPutHandler::handle() throw(castor::exception::Exception)
      {
        StagerReplyHelper* stgReplyHelper=NULL;
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          
          /**********/
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOPUT, &(stgCnsHelper->cnsFileid));
          
          jobOriented();
          
          /* use the stagerService to recreate castor file */
          castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
          
          
          if(diskCopyForRecall == NULL){
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(stgCnsHelper->cnsFileid));
          }
          else{
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(stgCnsHelper->cnsFileid));
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_READY);
            /* since newSubrequestStatus == SUBREQUEST_READY, we have to setGetNextStatus */
            stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
            
            /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
            stgReplyHelper = new StagerReplyHelper();
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "No Error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            
            delete stgReplyHelper;
            delete diskCopyForRecall;
          }
          
        }catch(castor::exception::Exception e){
          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOPUT, 2 ,params, &(stgCnsHelper->cnsFileid));
          
          throw(e);
        }
        
      }
      
      
      
      
      
      StagerPrepareToPutHandler::~StagerPrepareToPutHandler()throw(){
      }
      
      
      
      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

