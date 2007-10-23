/****************************************************************************/
/* StagerPutHandler: Constructor and implementation of Put request handler */
/**************************************************************************/



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPutHandler.hpp"

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

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerPutHandler::StagerPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePutRequest;
        
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
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          preHandle();
          /**********/
          
          jobOriented();
          
          /* use the stagerService to recreate castor file */
          castor::stager::DiskCopyForRecall* diskCopyForRecall = 
            stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
          
          if(diskCopyForRecall){  	  
            /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
            if(!diskCopyForRecall->mountPoint().empty()) {
              stgRequestHelper->subrequest->setRequestedFileSystems(diskCopyForRecall->diskServer() + ":" + diskCopyForRecall->mountPoint());
            }
            stgRequestHelper->subrequest->setXsize(this->xsize);
            
            /* updateSubrequestStatus Part: */
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
            stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
            stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
            
            /* and we have to notify the jobManager */
            m_notifyJobManager = true;
          } /* diskCopyForRecall != NULL */
          
        }catch(castor::exception::Exception e){
          /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
          /* we don t execute: dbService->updateRep ..*/
          
          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"(StagerPutHandler) Error"<<e.getMessage().str()<<std::endl;
          throw ex;
        }
        
      }/* end StagerPutHandler::handle()*/
      
      
      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

