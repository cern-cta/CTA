/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"

#include "../IStagerSvc.hpp"
#include "../../Services.hpp"
#include "../../BaseAddress.hpp"
#include "../SubRequest.hpp"
#include "../FileRequest.hpp"
#include "../../IClient.hpp"
#include "../SvcClass.hpp"
#include "../CastorFile.hpp"
#include "../FileClass.hpp"


#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/Cglobals.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/rm_struct.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../IClientFactory.h"

#include "../DiskCopyForRecall.hpp"
#include "../DiskCopyStatusCodes.hpp"

#include "../../exception/Exception.hpp"
#include "../../../h/serrno.h"
#include "../../Constants.hpp"

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
	  stgRequestHelper->linkCastorFileToServiceClass(*stgCnsHelper);
	  
	  /*check if the castorFile is linked to the service Class */
	  /* the exception is throwing internally in the helper method */ 
	  stgRequestHelper->isCastorFileLinkedToSvcClass();
	  
	  /* set svcClass.name() as a partitionMask (it'll be copy to rmjob.partitionmask) */
	  /*the exception is throwing internally in the helper method */
	  stgRequestHelper->getPartitionMask();
	  
	  /*  fill castorFile's FileClass: called in StagerRequest.jobOriented() */
	  stgRequestHelper->setFileClassOnCastorFile();
	  
	  /*in c code this part also includes to print the protocol(=subrequest.protocol()) or the default protocol */
	}catch (castor::exception::Exception e){
	  /* maybe Cns_query throws an exception */
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerJobRequestHandler jobOriented)"<<e.getMessage()<<std::endl;
	  throw ex;
	}
      }
      
      
      
      /***********************************************************************************************/
      /* after asking the stagerService is it is toBeScheduled                                      */
      /* - do a normal tape recall                                                                 */
      /* - check if it is to replicate:                                                           */
      /*         +processReplica if it is needed:                                                */
      /*                    +make the hostlist if it is needed                                  */
      /* it is gonna be overwriten on the stagerPrepareToGetHandler because of the case 0 !!!! */
      /****************************************************************************************/
      void StagerJobRequestHandler::switchScheduling(int caseToSchedule) throw(castor::exception::Exception)
      {
	
	try{
	  switch(caseToSchedule){
	    
	  case 2: //normal tape recall
	    try{
	      stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid());//throw exception
	    }catch(castor::exception::Exception ex){
	      castor::exception::Exception e(ESTSEGNOACC);
	      e.getMessage()<<"(Stager__Handler switchScheduling) stagerService->createRecallCandidate"<<std::endl;
	      throw(e);
	    }
	    break;
	    
	  case 1:	 
	    bool isToReplicate=replicaSwitch();
	    
	    if(isToReplicate){  
	      processReplicaAndHostlist();
	    }
	    break;
	    
	  case 0:
	  stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
	  /// ADD!! :  change_subrequest_status = 0
	  
	  
	  default:
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerJobRequestHandler switchScheduling) stagerservice->isSubRequestToSchedule returns an invalid value"<<std::endl;
	    throw ex;
	    break;
	  }
	}catch (castor::exception::Exception e){
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(Stager__Handler) Error"<<e.getMessage()<<std::endl;
	  throw ex;
	}
	
      }//end switchScheduling
      

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
	    if((sources.front()->status()) == STAGE_OUT){
	      maxReplicaNb = 1;
	    }
	  }
	
	
	if(sources.size()>0){
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
      int StagerJobRequestHandler::checkReplicationPolicy() throw(castor::exception::Exception)
      {
	//ask the expert system...

	const std::string filename = stgRequestHelper->subrequest->fileName();
	std::string expQuestion=replicationPolicy + " " + filename;
	char expAnswer[21];//SINCE unsigned64 maxReplicaNb=expAnswer.stringTOnumber() THEN  expAnswer.size()=21 (=us64.size)
	int fd;
	
	if(expert_send_request(&fd, EXP_RQ_REPLICATION)){//connecting to the expert system
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy) Error on expert_send_request"<<std::endl;
	  throw ex;
	}
	if(expert_send_data(fd,expQuestion.c_str(),expQuestion.size())){//sending question
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy) Error on expert_send_data"<<std::endl;
	  throw ex;
	}

	memset(expAnswer, '\0',sizeof(expAnswer));
	if(expert_receive_data(fd,expAnswer,sizeof(expAnswer),STAGER_DEFAULT_REPLICATION_EXP_TIMEOUT)){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy) Error on expert_receive_data"<<std::endl;
	  throw ex;
	}
	return(atoi(expAnswer));
      }


   






      /************************************************************************************/
      /* process the replicas = build rfs string (and hostlist) */
      /* - rfs + = ("|") + diskServerName + ":" + mountPoint */
      /* - hostlist + = (":") + diskServerName ()if it isn' t already in hostlist  */
      /*  */
      /**********************************************************************************/
      void StagerJobRequestHandler::processReplicaAndHostlist() throw(castor::exception::Exception)
      {	//it will represent the case1:
	//build "rfs" (and "hostlist" if it is defined)
	
	std::string fullMountPoint;
	std::string diskServerName;
	
	/* rfs and hostlist are attributes !*/ 
	rfs.clear();
	hostlist.clear();
	rfs.resize(CA_MAXLINELEN+1);
	hostlist.resize(CA_MAXHOSTNAMELEN+1);
	
	std::list<DiskCopyForRecall *>::iterator iter = sources.begin();
	for(int iReplica=0;(iReplica<maxReplicaNb) && (iter != sources.end()); iReplica++, iter++){
	  
	  diskServerName=(*iter)->diskServer();
	  fullMountPoint=diskServerName+":"+(*iter)->mountPoint();
	  if(this->rfs.empty()== false){
	    this->rfs+="|";
	  }
	  this->rfs+=fullMountPoint;
	  if((this->rfs.size())>CA_MAXLINELEN){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerJobRequestHandler processReplicaAndHostlist) rfs string size surpased "<<std::endl;
	    throw ex;
	  }
	  
	  if(useHostlist){ //do we need to add the diskServer to the hostlist:
	    //check if diskServerName is already in hostlist
	    
	    if((this->hostlist.find(diskServerName))==(std::string::npos)){//diskServerName not already 
	      //diskServer NOT ALREADY in hostlist: ADD IT!
	      if(hostlist.empty()==false){
		hostlist+=":";
	      }
	      hostlist+=diskServerName;
	      if(hostlist.size()>CA_MAXHOSTNAMELEN){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerJobRequestHandler processReplicaAndHostlist) hostlist size surpased"<<std::endl;
	      }  
	    }
	  }
	  
	}//end for(iReplica)    

      }



      /*****************************************************************************************************/
      /* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
      /* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
      /**************************************************************************************************/
      /* inline void StagerJobRequestHandler::buildRmJobRequestPart() throw(castor::exception::Exception)//after processReplica (if it is necessary) */
      

        
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
