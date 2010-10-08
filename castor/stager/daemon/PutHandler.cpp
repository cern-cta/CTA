/****************************************************************************/
/* PutHandler: Constructor and implementation of Put request handler */
/**************************************************************************/



#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/PutHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
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
      
      PutHandler::PutHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePutRequest;
      }
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void PutHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        /* get the request's size required on disk */
        xsize = stgRequestHelper->subrequest->xsize();
        
        if(xsize > 0){
          
          if(xsize < stgCnsHelper->cnsFilestat.filesize) {
            /* print warning! */
          }
        }else{
          /* we get the defaultFileSize */
          xsize = stgRequestHelper->svcClass->defaultFileSize();
          if( xsize <= 0){
            xsize = DEFAULTFILESIZE;
          }
        }
        
        default_protocol = "rfio";
      }
      
      /*********************************/
      /* handler for the Put request  */
      /*******************************/
      void PutHandler::handle() throw(castor::exception::Exception)
      {
        castor::stager::DiskCopyForRecall* dc = 0;
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          
          /**********/
          
          handlerSettings();
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PUT, &(stgCnsHelper->cnsFileid));
          
          jobOriented();
          
          /* use the stagerService to recreate castor file */
          dc = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile, stgRequestHelper->subrequest);
          
          if(dc) {
            // check for request in wait due to concurrent puts
            if(dc->status() == DISKCOPY_WAITFS_SCHEDULING) {
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
              // we don't need to do anything, the request will be restarted
              // however we have to commit the transaction
              stgRequestHelper->dbSvc->commit();
            }
            else {
              // schedule the put
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, &(stgCnsHelper->cnsFileid));
              if(!dc->mountPoint().empty()) {
                stgRequestHelper->subrequest->setRequestedFileSystems(dc->diskServer() + ":" + dc->mountPoint());
              }
              stgRequestHelper->subrequest->setXsize(this->xsize);
              
              stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
              stgRequestHelper->dbSvc->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
              
              // we have to notify the jobmanager daemon
              m_notifyJobManager = true;
            }
            delete dc;
          }
          else {
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(stgCnsHelper->cnsFileid));
          }
        }
        catch(castor::exception::Exception& e) {
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUT, 2 ,params, &(stgCnsHelper->cnsFileid));
          if(dc) delete dc;
          throw(e);
        }
      }/* end PutHandler::handle()*/
      
      
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 

