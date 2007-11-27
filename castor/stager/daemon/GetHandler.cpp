/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "osdep.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
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
      
      StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StageGetRequest;
      }
      
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerGetHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();	
        this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
        
        /* get the request's size required on disk */
        /* depending if the file exist, we ll need to update this variable */
        this->xsize = this->stgRequestHelper->subrequest->xsize();
        
        if( xsize > 0 ){
          if(xsize < (stgCnsHelper->cnsFilestat.filesize)){
            /* warning, user is requesting less bytes than the real size */
            /* just print a message. we don't update xsize!!! */
          }
          
        }else{
          this->xsize = stgCnsHelper->cnsFilestat.filesize;
        }
        
        
        this->default_protocol = "rfio";
        
      }
      
      
      
      
      /********************************************/	
      /* for Get, Update                         */
      /*     switch(getDiskCopyForJob):         */                                     
      /*        case 0,1: (staged) jobManager  */
      /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
      bool StagerGetHandler::switchDiskCopiesForJob() throw(castor::exception::Exception)
      {
        
        switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,this->sources)){
          case -2:
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
            break;
          
          case -1:
            stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
            break;
          
          case 0: // DISKCOPY_STAGED, schedule job
            {
              bool isToReplicate= replicaSwitch();
              if(isToReplicate){
                processReplica();
              }
              
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_SCHEDULINGJOB, &(stgCnsHelper->cnsFileid));
              
              jobManagerPart();
              
              stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
              stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
              
              /* and we have to notify the jobManager */
              m_notifyJobManager = true;
            }
            break;
          
          case 1:   // DISK2DISKCOPY - will disappear soon
            {
              bool isToReplicate= replicaSwitch();
              if(isToReplicate){
                processReplica();
              }
              
              stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_DISKTODISK_COPY, &(stgCnsHelper->cnsFileid));
              /* build the rmjob struct and submit the job */
              jobManagerPart();
              
              
              stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
              stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
              
              /* and we have to notify the jobManager */
              m_notifyJobManager = true;
            }
            break;
          
          case 2: /* create a tape copy and corresponding segment objects on stager catalogue */
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, &(stgCnsHelper->cnsFileid));
            
            // here we don't care about the return value: we don't need to answer the client in any case
            stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest, stgRequestHelper->fileRequest->euid(),
              stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
            break;
          
        }//end switch
        
        return false;        
      }
      
      
      
      
      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void StagerGetHandler::handle() throw (castor::exception::Exception)
      {
        try{
          handlerSettings();
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_GET, &(stgCnsHelper->cnsFileid));
          
          jobOriented();
          switchDiskCopiesForJob(); 
          
        }catch(castor::exception::Exception e){
          
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_GET, 2, params, &(stgCnsHelper->cnsFileid));
          throw(e);
        }  
      }
      
      StagerGetHandler::~StagerGetHandler()throw(){
        
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
