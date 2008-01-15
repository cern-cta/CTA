/************************************************************************************************************/
/* StagerUpdateHandler: Contructor and implementation of the Update request handler                        */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                  */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put)  */
/* We dont  need to reply to the client (just in case of error )                                        */
/*******************************************************************************************************/




#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "castor/stager/daemon/StagerRequestHandler.hpp"
#include "castor/stager/daemon/StagerJobRequestHandler.hpp"
#include "castor/stager/daemon/StagerUpdateHandler.hpp"
#include "castor/stager/daemon/StagerPutHandler.hpp"
#include "castor/stager/daemon/StagerGetHandler.hpp"

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
#include "castor/stager/daemon/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      /* constructor */
      StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StageUpdateRequest;     
      }
      
      
      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerUpdateHandler::preHandle() throw(castor::exception::Exception)
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
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        /* get the svcClass */
        stgRequestHelper->getSvcClass();
        
        /* create and fill request->svcClass link on DB */
        stgRequestHelper->linkRequestToSvcClassOnDB();
        
        /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
        /* create the file if it is needed/possible */
        bool fileCreated = stgCnsHelper->checkFileOnNameServer(stgRequestHelper->subrequest, stgRequestHelper->svcClass);
        
        /* check if the user (euid,egid) has the right permission for the request's type. otherwise -> throw exception  */
        stgRequestHelper->checkFilePermission(fileCreated);
        
        recreate = fileCreated || ((stgRequestHelper->subrequest->flags() & O_TRUNC) == O_TRUNC);
      }
      
      
      /************************************/
      /* handler for the update request  */
      /**********************************/
      void StagerUpdateHandler::handle() throw(castor::exception::Exception)
      {
        stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_UPDATE, &(stgCnsHelper->cnsFileid));
        
        StagerRequestHandler* h = 0;
        if(recreate) {
          // delegate to Put
          h = new StagerPutHandler(stgRequestHelper, stgCnsHelper);
        }
        else {
          // delegate to Get
          h = new StagerGetHandler(stgRequestHelper, stgCnsHelper);
        }      
        h->handle();   // may throw exception, just forward it - logging is done in the callee
        delete h;
        stgCnsHelper = 0;   // the delegated handler has already deleted this object
      }
      
      
      /* destructor */
      StagerUpdateHandler::~StagerUpdateHandler() throw()
      {
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
