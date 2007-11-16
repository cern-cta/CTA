/***********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"

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
      
      StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StagePrepareToUpdateRequest;
        
      }
      
      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerPrepareToUpdateHandler::preHandle() throw(castor::exception::Exception)
      {
        
        /* get the uuid request string version and check if it is valid */
        stgRequestHelper->setRequestUuid();
        
        /* we create the StagerCnsHelper inside and we pass the requestUuid needed for logging */
        this->stgCnsHelper = new StagerCnsHelper(stgRequestHelper->requestUuid);
        
        /* set the username and groupname needed to print them on the log */
        stgRequestHelper->setUsernameAndGroupname();
        
        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* get the associated client and set the iClientAsString variable */
        stgRequestHelper->getIClient();
        
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        
        /* get the svcClass */
        stgRequestHelper->getSvcClass();
        
        
        /* create and fill request->svcClass link on DB */
        stgRequestHelper->linkRequestToSvcClassOnDB();
        
        /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
        /* create the file if it is needed/possible */
        this->fileExist = stgCnsHelper->checkAndSetFileOnNameServer(stgRequestHelper->subrequest->fileName(), this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
        
        /* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
        stgRequestHelper->checkFilePermission();
        
        this->toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
      }
      
      
      
      /*********************************************/
      /* handler for the PrepareToUpdate request  */
      /*******************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
        StagerReplyHelper* stgReplyHelper=NULL;
        try{
          
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, &(stgCnsHelper->cnsFileid));
          
          StagerRequestHandler* h = 0;
          if(toRecreateCastorFile) {
            // delegate to Put
            h = new StagerPrepareToPutHandler(stgRequestHelper, stgCnsHelper);
          } else {
            // delegate to Get
            h = new StagerPrepareToGetHandler(stgRequestHelper, stgCnsHelper);
          }      
          h->handle();
          delete h;
          
        }catch(castor::exception::Exception e){
          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOUPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
          
          throw(e);
        }
      }
      
      
      
      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

