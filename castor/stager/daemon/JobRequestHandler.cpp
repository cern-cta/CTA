/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{



      /****************************************************************************************/
      /* job oriented block                                                                  */
      /**************************************************************************************/
      void StagerJobRequestHandler::jobOriented()
      {
	
	try{
	  
	  memset(&(stgCnsHelper->cnsFileClass), 0, sizeof(stgCnsHelper->cnsFileClass) );
	  Cns_queryclass(stgCnsHelper->fileid.server,stgCnsHelper->cnsFilestat.fileclass, NULL, &(stgCnsHelper->cnsFileclass));
	  
	  /* free the tppols*/
	  free(stgCnsHelper->cnsFileclass.tppools);
	  
	  /* link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name )*/
	  stgRequestHelper->getFileClass(stgCnsHelper->cnsFileClass.name);/* first we need to get the FileClass*/
	  stgRequestHelper->linkCastorFileToServiceClass(*stgCnsHelper);
	  
	  /*check if the castorFile is linked to the service Class */
	  stgRequestHelper->isCastorFileLinkedToSvcClass();
	  
	  /* set svcClass.name() as a partitionMask (it'll be copy to rmjob.partitionmask)*/
	  stgRequestHelper->getPartitionMask();
	  
	  /*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	  stgRequestHelper->setFileClassOnCastorFile();
	  
	  /*in c code this part also includes to print the protocol(=subrequest.protocol()) or the default protocol */
	}catch (){
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
      void StagerJobRequestHandler::switchScheduling(int caseToSchedule)
      {
	
	switch(caseToSchedule){

	case 2: //normal tape recall
	 
	  
	  stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,(const) stgRequestHelper->filerequest->euid(), (const) stgRequestHelper->filerequest->egid());//throw exception
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
	  //throw exception!
	  break;
	}
	
      }//end switchScheduling
      

      /************************************************************************************/
      /* return if it is to replicate considering: */
      /* - sources.size() */
      /* - maxReplicaNb */
      /* - replicationPolicy (call to the expert system) */
      /**********************************************************************************/
      bool StagerJobRequestHandler::replicaSwitch()
      {
	bool toProcessReplicate=false;
	

	//if the status of the unique copy is STAGE_OUT, then force to don' t replicate
	if((sources.size())==1)
	  {
	    if((sources[0].status) == STAGE_OUT){
	      maxReplicaNb = 1;
	    }
	  }
	
	
	if(sources.size()>0){
	  if(maxReplicaNb>0){
	    if(maxReplicaNb <= sources.size()){
	      toProcessReplicate = true;
	    }
	    
	  }else if(!(replicationPolicy.empty())){ //replicationPolicy(= svcClass.replicationPolicy())  EXISTS
	    
	    maxReplicaNb = checkReplicationPolicy();
	    if(maxReplicaNb > 0){
	      if(maxReplicaNb <= sources.size()){
		toProcessReplicate = true;
	      }
	    }
	    
	  }
	}
	return(isToReplicate);
      }




      /***************************************************************************************************************************/
      /* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
      /**************************************************************************************************************************/
      int StagerJobRequestHandler::checkReplicationPolicy ()
      {
	//ask the expert system...

	const std::string filename = stgRequestHelper->subrequest->fileName();
	std::string expQuestion=replicationPolicy + " " + filename;
	char expAnswer[21];//SINCE unsigned64 maxReplicaNb=expAnswer.stringTOnumber() THEN  expAnswer.size()=21 (=us64.size)
	int fd;
	
	if(expert_send_request(&fd, EXP_RQ_REPLICATION)){//connecting to the expert system
	  //throw exception
	}
	if(expert_send_data(fd,expQuestion.c_str(),expQuestion.size())){//sending question
	  //throw exception
	}

	memset(expAnswer, '\0',sizeof(expAnswer));
	if(expert_receive_data(fd,expAnswer,sizeof(expAnswer))){
	  //throw exception
	}
	return(atoi(expAnswer));
      }


   






      /************************************************************************************/
      /* process the replicas = build rfs string (and hostlist) */
      /* - rfs + = ("|") + diskServerName + ":" + mountPoint */
      /* - hostlist + = (":") + diskServerName ()if it isn' t already in hostlist  */
      /*  */
      /**********************************************************************************/
      void StagerJobRequestHandler::processReplicaAndHostlist()
      {	//it will represent the case1:
	//build "rfs" (and "hostlist" if it is defined)
	
	
	try{
	  int iReplica;
	  std::string fullMountPoint;
	  std::string diskServerName;
	 
	  /* rfs and hostlist are attributes !*/ 
	  rfs.clear();
	  hostlist.clear();
	  rfs.rsize(CA_MAXLINELEN+1);
	  hostlist.rsize(CA_MAXHOSTNAMELEN+1);


	  for(int iReplica=0;iReplica<maxReplicaNb;iReplica++){
	    diskServerName=sources[iReplica].diskServer();
	    fullMountPoint=diskServerName+":"+sources[iReplica].mountPoint();
	    if(this->rfs.empty()== FALSE){
	      this->rfs+="|";
	    }
	    this->rfs+=fullMountPoint;
	    if((this->rfs.size())>CA_MAXLINELEN){
	      //throw exception!
	    }
	    
	    if(useHostlist){ //do we need to add the diskServer to the hostlist:
	      //check if diskServerName is already in hostlist
	      
	      if((this->hostlist.find(diskServerName))==(string::npos)){//diskServerName not already 
		//diskServer NOT ALREADY in hostlist: ADD IT!
		if(this->hostlist.empty()==FALSE){
		  hostlist+=":";
		}
		this->hostlist+=diskServerName;
		if(this->hostlist.size()>CA_MAXHOSTNAMELEN){
		  //throw exception
		}  
	      }
	    }
	    
	  }//end for(iReplica)    
	}catch ( ){
	}
      }



      /*****************************************************************************************************/
      /* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
      /* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
      /**************************************************************************************************/
      void StagerJobRequestHandler::buildRmJobRequestPart()//after processReplica (if it is necessary)
      {
	if(!rfs.empty()){
	  strncpy(this->rmjob.rfs,rfs.c_str(), CA_MAXPATHLEN);
	}
	
	if((useHostlist)&&(!hostlist.empty())){
	  strncpy(this->rmjob.hostlist, hostlist.c_str(), CA_MAXHOSTNAMELEN);
	}

	

	rmjob.ddisk = this->xsize;//depending on the request's type
	rmjob.openflags = this->openflags;//depending on the request's type
	
	
      }


      
      

      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
