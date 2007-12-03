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


#include "castor/stager/DiskCopyForRecall.hpp"
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
        this->maxReplicaNb = stgRequestHelper->svcClass->maxReplicaNb();	
        this->replicationPolicy = stgRequestHelper->svcClass->replicationPolicy();
        
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
          
          case 0:   // DISKCOPY_STAGED, schedule job
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_SCHEDULINGJOB, &(stgCnsHelper->cnsFileid));
            
            processReplica();
            jobManagerPart();
            
            stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
            stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);	      
            stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
            
            /* and we have to notify the jobManager */
            m_notifyJobManager = true;
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
      

      /************************************************************************************/
      /* decides if it is to replicate considering: */
      /* - sources.size() */
      /* - maxReplicaNb */
      /* - replicationPolicy (call to the expert system) */
      /**********************************************************************************/
      void StagerGetHandler::processReplica() throw(castor::exception::Exception)
      {
        bool replicate = true;
        
        /* if the status of the unique copy is STAGEOUT, then don't replicate */
        if(sources.size() == 1 && sources.front()->status() == DISKCOPY_STAGEOUT) {
          maxReplicaNb = 1;
        }
        
        if(sources.size() > 0) {
          if(maxReplicaNb > 0) {
            if(maxReplicaNb <= sources.size()) {
              replicate = false;
            }
          }
          else if((replicationPolicy.empty()) == false) { 
            maxReplicaNb = checkReplicationPolicy();
            if(maxReplicaNb > 0){
              if(maxReplicaNb <= sources.size()){
                replicate = false;
              }
            }
          }
        }

        if(replicate) {
          // We need to replicate, create a diskCopyReplica request
          stgRequestHelper->stagerService->createDiskCopyReplicaRequest(0, sources.front(), stgRequestHelper->svcClass);
        }
        
        // Fill the requested filesystems for the request being processed: we won't wait
        // for the disk-to-disk copy if it's to be done
        std::list<DiskCopyForRecall *>::iterator iter = sources.begin();
        rfs = "";
        for(unsigned int iReplica=0; (iReplica<maxReplicaNb) && (iter != sources.end()); iReplica++, iter++){
          if(!rfs.empty())
            rfs += "|";
          rfs += (*iter)->diskServer()+":"+(*iter)->mountPoint();
        }
        
        // cleanup sources list
        for(iter = sources.begin(); iter != sources.end(); iter++) {
          delete (*iter);
        }
        sources.clear();
      }

     
      /***************************************************************************************************************************/
      /* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
      /**************************************************************************************************************************/
      int StagerGetHandler::checkReplicationPolicy() throw(castor::exception::Exception)/* changes coming from the latest stager_db_service.cpp */
      {
        const std::string filename = stgRequestHelper->subrequest->fileName();
        std::string expQuestion=replicationPolicy + " " + filename;
        char expAnswer[21];//SINCE unsigned64 maxReplicaNb=expAnswer.stringTOnumber() THEN  expAnswer.size()=21 (=us64.size)
        int fd;
        
	  
        if(expert_send_request(&fd, EXP_RQ_REPLICATION)){//connecting to the expert system
            
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Error on expert_send_request";
          throw ex;
        }
        if((unsigned)(expert_send_data(fd, expQuestion.c_str(), expQuestion.size())) != (expQuestion.size())) {  //sending question
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Error on expert_send_data";
          throw ex;
        }
        
        memset(expAnswer, '\0',sizeof(expAnswer));
        if((expert_receive_data(fd,expAnswer,sizeof(expAnswer),STAGER_DEFAULT_REPLICATION_EXP_TIMEOUT)) <= 0){
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Error on expert_receive_data";
          throw ex;
        }
        
        return(atoi(expAnswer));
      }
      
       
      StagerGetHandler::~StagerGetHandler()throw(){        
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
