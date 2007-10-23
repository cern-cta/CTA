/************************************************************************************************************/
/* StagerUpdateHandler: Contructor and implementation of the Update request handler                        */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                  */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put)  */
/* We dont  need to reply to the client (just in case of error )                                        */
/*******************************************************************************************************/




#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      /* constructor */
      StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StageUpdateRequest;
        
        
        this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();	
        this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
        
        
        
        /* get the subrequest's size required on disk */
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
        
      }
      
      
      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerUpdateHandler::preHandle() throw(castor::exception::Exception)
      {
        
        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* get the associated client and set the iClientAsString variable */
        stgRequestHelper->getIClient();
        
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        /* get the uuid request string version and check if it is valid */
        stgRequestHelper->setRequestUuid();
        
        
        /* get the svcClass */
        stgRequestHelper->getSvcClass();
        
        
        /* create and fill request->svcClass link on DB */
        stgRequestHelper->linkRequestToSvcClassOnDB();
        
        /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
        /* create the file if it is needed/possible */
        this->fileExist = stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
        
        /* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
        stgRequestHelper->checkFilePermission();
        
        this->toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
        
        castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","(RequestSvc) Detailed subrequest(fileName,{euid,egid},{userName,groupName},mode mask, cliendId, svcClassName,cnsFileid.fileid, cnsFileid.server"),
          castor::dlf::Param("Standard Message",stgCnsHelper->subrequestFileName),
          castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->euid),
          castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->egid),
          castor::dlf::Param("Standard Message",stgRequestHelper->username),
          castor::dlf::Param("Standard Message",stgRequestHelper->groupname),
          castor::dlf::Param("Standard Message",stgRequestHelper->fileRequest->mask()),
          castor::dlf::Param("Standard Message",stgRequestHelper->iClient->id()),
          castor::dlf::Param("Standard Message",stgRequestHelper->svcClassName),
          castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.fileid),
        castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.server)};
        castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 11, parameter);
        
        
        
        
      }
      
      
      /************************************/
      /* handler for the update request  */
      /**********************************/
      void StagerUpdateHandler::handle() throw(castor::exception::Exception)
      {
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          preHandle();
          /**********/
          
          jobOriented();
          
          
          /* huge or small flow?? */
          if(toRecreateCastorFile){/* we skip the processReplica part :) */    
            /* use the stagerService to recreate castor file */
            castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
            
            if(diskCopyForRecall == NULL){
              /* in this case we have to archiveSubrequest */
              /* updateStatus to FAILED_FINISHED and replyToClient (both to be dones on StagerDBService, catch exception) */
              this->stgRequestHelper->stagerService->archiveSubReq(this->stgRequestHelper->subrequest->id());
              castor::exception::Exception ex(EBUSY);
              ex.getMessage()<<"(StagerUpdateHandler handle) Recreation is not possible (null DiskCopyForRecall)"<<std::endl;
              throw ex;
              
            }else{ /* coming from the latest stager_db_service.c */
              /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
              std::string diskServerName(diskCopyForRecall->diskServer());
              std::string mountPoint(diskCopyForRecall->mountPoint());
              
              if((diskServerName.empty() == false) && (mountPoint.empty() == false)){
                rfs.resize(CA_MAXRFSLINELEN);
                rfs.clear();
                this->rfs = diskServerName + ":" + mountPoint;
                
                if(rfs.size() >CA_MAXRFSLINELEN ){
                  castor::exception::Exception ex(SENAMETOOLONG);
                  ex.getMessage()<<"(Stager_Handler) Not enough space for filesystem constraint"<<std::endl;
                  throw ex;
                }else{
                  /* update the subrequest table (coming from the latest stager_db_service.c) */
                  stgRequestHelper->subrequest->setRequestedFileSystems(this->rfs);
                  stgRequestHelper->subrequest->setXsize(this->xsize);
                } 
              }
              
              /* updateSubrequestStatus Part: */
              stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
              stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
              stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
              /* and we have to notify the jobManager */
              m_notifyJobManager = true;
              
            }/* notSchedule && diskCopyForRecall != NULL  */
            
          }else{/*if notToRecreateCastorFile */
            
            /* for Get, Update                         */
            /*     switch(getDiskCopyForJob):         */                                     
            /*        case 0,1: (staged) jobManager  */
            /*        case 2: (waitRecall) createTapeCopyForRecall */
            /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
            switchDiskCopiesForJob(); 
          }
          
          
        }catch(castor::exception::Exception e){
          
          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"(StagerUpdateHandler) Error"<<e.getMessage().str()<<std::endl;
          throw ex;
        }
      }
      
      
      
      
      /* destructor */
      StagerUpdateHandler::~StagerUpdateHandler() throw()
      {
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
