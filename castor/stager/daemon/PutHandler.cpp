/****************************************************************************/
/* StagerPutHandler: Constructor and implementation of Put request handler */
/**************************************************************************/



#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "castor/stager/daemon/StagerRequestHandler.hpp"
#include "castor/stager/daemon/StagerJobRequestHandler.hpp"
#include "castor/stager/daemon/StagerPutHandler.hpp"

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
#include "castor/stager/daemon/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace daemon{
      
      StagerPutHandler::StagerPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePutRequest;
      }
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerPutHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        /* we don't care about: maxReplicaNb, replicationPolicy, hostlist */
        
        /* get the request's size required on disk */
        this->xsize = this->stgRequestHelper->subrequest->xsize();
        
        if(this->xsize > 0){
          
          if( this->xsize < (this->stgCnsHelper->cnsFilestat.filesize) ){
            /* print warning! */
          }
        }else{
          /* we get the defaultFileSize */
          xsize = stgRequestHelper->svcClass->defaultFileSize();
          if( xsize <= 0){
            xsize = DEFAULTFILESIZE;
          }
        }
        
        this->default_protocol = "rfio";
      }
      
      /*********************************/
      /* handler for the Put request  */
      /*******************************/
      void StagerPutHandler::handle() throw(castor::exception::Exception)
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
              stgRequestHelper->daemon->commit();
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
              stgRequestHelper->daemon->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
              
              // we have to notify the jobManager
              m_notifyJobManager = true;
            }
            delete dc;
          }
          else {
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, &(stgCnsHelper->cnsFileid));
          }
        }
        catch(castor::exception::Exception e) {
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUT, 2 ,params, &(stgCnsHelper->cnsFileid));
          if(dc) delete dc;
          throw(e);
        }
      }/* end StagerPutHandler::handle()*/
      
      
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 

