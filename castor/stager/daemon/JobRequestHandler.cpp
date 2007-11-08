/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cglobals.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/Constants.hpp"

#include "marshall.h"
#include "net.h"

#include "getconfent.h"
#include "Cnetdb.h"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>
#include <list>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


namespace castor{
  namespace stager{
    namespace dbService{
      
      
      
      /****************************************************************************************/
      /* job oriented block                                                                  */
      /**************************************************************************************/
      void StagerJobRequestHandler::jobOriented() throw(castor::exception::Exception)
      {
        
        try{
          
          memset(&(stgCnsHelper->cnsFileclass), 0, sizeof(stgCnsHelper->cnsFileclass));
          Cns_queryclass(stgCnsHelper->cnsFileid.server,stgCnsHelper->cnsFilestat.fileclass, NULL, &(stgCnsHelper->cnsFileclass));
          /* free the tppols*/
          free(stgCnsHelper->cnsFileclass.tppools);
          
          /* link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name ) */
          /* the exception is throwing internally in the helper method */
          stgRequestHelper->getFileClass(stgCnsHelper->cnsFileclass.name);/* first we need to get the FileClass */
          
          /* the exception is throwing internally in the helper method */
          stgRequestHelper->getCastorFileFromSvcClass(stgCnsHelper);
          
          
          /*  fill castorFile's FileClass: called in StagerRequest.jobOriented() */
          stgRequestHelper->setFileClassOnCastorFile();
          
          /*in c code this part also includes to print the protocol(=subrequest.protocol()) or the default protocol */
        }catch (castor::exception::Exception e){
          /* maybe Cns_query throws an exception */
          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"(StagerJobRequestHandler jobOriented)"<<e.getMessage().str()<<std::endl;
          throw ex;
        }
      }
      

     
      
      
      
      /************************************************************************************/
      /* return if it is to replicate considering: */
      /* - sources.size() */
      /* - maxReplicaNb */
      /* - replicationPolicy (call to the expert system) */
      /**********************************************************************************/
      bool StagerJobRequestHandler::replicaSwitch() throw(castor::exception::Exception)
      {
        bool toProcessReplicate=false;
        
        /* the exception can just be throwed on "checkReplicationPolicy" */
        /*if the status of the unique copy is STAGE_OUT, then force to don' t replicate */
        if((sources.size())==1)
        {
          if((sources.front()->status()) == DISKCOPY_STAGEOUT){/* coming from the latest stager_db_service.c */
            maxReplicaNb = 1;
          }
        }
        
        /* rfs.clear(); coming from the latest stager_db_service.c */
        if(sources.size() > 0){
          if(maxReplicaNb>0){
            if(maxReplicaNb <= sources.size()){
              toProcessReplicate = true;
            }
            
          }else if((replicationPolicy.empty()) == false){ /* replicationPolicy(= svcClass.replicationPolicy())  EXISTS */
            
            maxReplicaNb = checkReplicationPolicy();
            if(maxReplicaNb > 0){
              if(maxReplicaNb <= sources.size()){
                toProcessReplicate = true;
              }
            }
            
          }
        }
        
        return(toProcessReplicate);
      }
      
      
      
      
      /***************************************************************************************************************************/
      /* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
      /**************************************************************************************************************************/
      int StagerJobRequestHandler::checkReplicationPolicy() throw(castor::exception::Exception)/* changes coming from the latest stager_db_service.cpp */
      {
	 const std::string filename = stgRequestHelper->subrequest->fileName();
	 std::string expQuestion=replicationPolicy + " " + filename;
	 char expAnswer[21];//SINCE unsigned64 maxReplicaNb=expAnswer.stringTOnumber() THEN  expAnswer.size()=21 (=us64.size)
	 int fd;
        
	try{ 

	  
	  if(expert_send_request(&fd, EXP_RQ_REPLICATION)){//connecting to the expert system
	    castor::dlf::Param params[]={ castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_EXPERT_EXCEPTION, 5 ,params, &(stgCnsHelper->cnsFileid));
	      
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"Error on expert_send_request"<<std::endl;
	    throw ex;
	  }
	  if((expert_send_data(fd,expQuestion.c_str(),expQuestion.size())) != (expQuestion.size())){//sending question
	    castor::dlf::Param params[]={ castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_EXPERT_EXCEPTION, 5 ,params, &(stgCnsHelper->cnsFileid));
	    
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"Error on expert_send_data"<<std::endl;
	    throw ex;
	  }
	  
	  memset(expAnswer, '\0',sizeof(expAnswer));
	  if((expert_receive_data(fd,expAnswer,sizeof(expAnswer),STAGER_DEFAULT_REPLICATION_EXP_TIMEOUT)) <= 0){
	    castor::dlf::Param params[]={ castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_EXPERT_EXCEPTION, 5 ,params, &(stgCnsHelper->cnsFileid));
	    
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"Error on expert_receive_data"<<std::endl;
	    throw ex;
	  }
	}catch(castor::exception::Exception e){ /* maybe Cns_query throws an exception */
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy)"<<e.getMessage().str()<<std::endl;
          throw ex;
	}
	return(atoi(expAnswer));
      }
      
      
      /************************************************************************************/
      /* process the replicas = build rfs string (and hostlist) */
      /* - rfs + = ("|") + diskServerName + ":" + mountPoint */
      /* changes coming from the latest stager_db_service.c */
      /**********************************************************************************/
      void StagerJobRequestHandler::processReplica() throw(castor::exception::Exception)
      {	//it will represent the case1:
        //build "rfs" (and "hostlist" if it is defined)
        
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
      
      
      /*******************************************************************************************/
      /* build the rmjob needed structures(buildRmJobHelperPart() and buildRmJobRequestPart())  */
      /* and submit the job  */
      /****************************************************************************************/
      void StagerJobRequestHandler::jobManagerPart() throw(castor::exception::Exception){
        
        if(((typeRequest==OBJ_StageGetRequest) || (typeRequest==OBJ_StagePrepareToGetRequest)|| (typeRequest == OBJ_StageRepackRequest))&& (rfs.empty() == false)){
          /* if the file exists we don't have any size requirements */
          this->xsize = 0;
        }
        
        /* update the subrequest table (coming from the latest stager_db_service.c) */
        if(rfs.empty() == false){
          stgRequestHelper->subrequest->setRequestedFileSystems(this->rfs);
        }
        stgRequestHelper->subrequest->setXsize(this->xsize);
        
      }      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
