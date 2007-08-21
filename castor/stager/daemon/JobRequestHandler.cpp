/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

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
#include "serrno.h"
#include "castor/Constants.hpp"

#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "getconfent.h"
#include "Cnetdb.h"

#include <iostream>
#include <string>
#include <list>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* (from the latest stager_db_service.cpp) copied from the JobManagerDaemon.cpp */
#define DEFAULT_NOTIFICATION_PORT 15011


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
	  stgRequestHelper->getCastorFileFromSvcClass(*stgCnsHelper);
	  
	  
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
	    /* in this case we dont archiveSubrequest, we dont changeSubrequestStatus  */
	    try{
	      stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);//throw exception
	      /* we dont change subrequestStatus or archive the subrequest: but we need to set the newSubrequestStatus to replyToClient */
	      this->newSubrequestStatus = SUBREQUEST_READY;
	    }catch(castor::exception::Exception ex){
	      /* internally of the createRecallCandidate: we reply to the client and we setSubrequestStatus to FAILED */
	      return;
	    }
	    break;
	    
	  case 1:/* just for get and the special update */
	    int type = stgRequestHelper->fileRequest->type();
	    if((type == OBJ_StageGetRequest)||(type == OBJ_StageUpdateRequest)){
	      /* just for Get and Update(special) */
	      /* in this case we dont archiveSubrequest, but we do changeSubrequestStatus, and we dont replyToClient */
	      bool isToReplicate=replicaSwitch();
	      
	      if(isToReplicate){  
		processReplica();
	      }
	      
	      /**********************************************/
	      /* build the rmjob struct and submit the job */
	      jobManagerPart(); 
	      
	      this->newSubrequestStatus = SUBREQUEST_READYFORSCHED;
	      if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
		stgRequestHelper->subrequest->setStatus(newSubrequestStatus);
		stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
		/* we have to setGetNextStatus */
		stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);		
	      }

	      /* and we have to notify the jobManager */
	      /* do the same for the special Update and for the put on the handle() method */
	      this->notifyJobManager();
	    }else{
	      castor::exception::Exception e(SEOPNOTSUP);
	      e.getMessage()<<"(Stager__Handler switchScheduling) stagerService->createRecallCandidate"<<std::endl;
	      throw(e);

	    }
	    break;

	  case 4:
	    /* in this case we wont: changeSubrequestStatus, archiveSubrequest or replyToClient */
	    break;
 
	  case 0:
	    /* this function is reimplemented on PrepareToGet because of this case */
	    /* and we dont updateRep on DB */
	    stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
	    break;
	  
	  default:
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerJobRequestHandler switchScheduling) stagerservice->isSubRequestToSchedule returns an invalid value"<<std::endl;
	    throw ex;
	    break;
	  }
	}catch (castor::exception::Exception e){
	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(Stager__Handler) Error"<<e.getMessage().str()<<std::endl;
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
	    if((sources.front()->status()) == DISKCOPY_STAGEOUT){/* coming from the latest stager_db_service.c */
	      maxReplicaNb = 1;
	    }
	  }
	
	rfs.clear();/* coming from the latest stager_db_service.c */
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
	if((expert_send_data(fd,expQuestion.c_str(),expQuestion.size())) != (expQuestion.size())){//sending question
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy) Error on expert_send_data"<<std::endl;
	  throw ex;
	}

	memset(expAnswer, '\0',sizeof(expAnswer));
	if((expert_receive_data(fd,expAnswer,sizeof(expAnswer),STAGER_DEFAULT_REPLICATION_EXP_TIMEOUT)) <= 0){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerJobRequestHandler checkReplicationPolicy) Error on expert_receive_data"<<std::endl;
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
	
	std::string fullMountPoint;
	std::string diskServerName;
	
	/* rfs and hostlist are attributes !*/ 
	rfs.clear();
	rfs.resize(CA_MAXRFSLINELEN);

	
	std::list<DiskCopyForRecall *>::iterator iter = sources.begin();
	for(int iReplica=0;(iReplica<maxReplicaNb) && (iter != sources.end()); iReplica++, iter++){
	  
	  diskServerName=(*iter)->diskServer();
	  fullMountPoint=diskServerName+":"+(*iter)->mountPoint();
	  
	  /* coming from the latest stager_db_service.c */
	  if((rfs.empty() == true) && (fullMountPoint.size() > CA_MAXRFSLINELEN)){
	    castor::exception::Exception ex(SENAMETOOLONG);
	    ex.getMessage()<<"(Stager_Handler) Not enough space for filesystem constraint"<<std::endl;
	    throw ex;
	  }else if((fullMountPoint.size() + 1) > CA_MAXRFSLINELEN){
	    /* +1 because of the "|"*/
	    break; /* we dont add it to the rfs */
	  }
	  
	  
	  if(this->rfs.empty()==false){
	    this->rfs+="|";
	  }
	  this->rfs+=fullMountPoint;
	  
	}//end for(iReplica)    

      }

	
      /*******************************************************************************************/
      /* build the rmjob needed structures(buildRmJobHelperPart() and buildRmJobRequestPart())  */
      /* and submit the job  */
      /****************************************************************************************/
      void StagerJobRequestHandler::jobManagerPart() throw(castor::exception::Exception){

	int type = stgRequestHelper->fileRequest->type();
	
	/* just Get, Update and Put are allowed to call the JOB Manager (coming from the latest stager_db_service.c) */
	if((type != OBJ_StageGetRequest) && (type != OBJ_StageUpdateRequest) && (type != OBJ_StagePutRequest)){
	  castor::exception::Exception ex(SEOPNOTSUP);
	  ex.getMessage()<<"(Stager__Handler) Invalid type to call the JOB Manager"<<std::endl;
	  throw ex;
	}
	
	if((type==OBJ_StageGetRequest)&& (rfs.empty() == false)){
	  /* if the file exists we don't have any size requirements */
	  this->xsize = 0;
	}
	
	/* update the subrequest table (coming from the latest stager_db_service.c) */
	if(rfs.empty() == false){
	  stgRequestHelper->subrequest->setRequestedFileSystems(this->rfs);
	}
	stgRequestHelper->subrequest->setXsize(this->xsize);
	
      }


      /*********************************************************************/
      /* for Get, Update and Put, send the notification to the jobManager */
      /*******************************************************************/    
      void StagerJobRequestHandler::notifyJobManager() throw(castor::exception::Exception){

	/* it should happen always, but we check the condition, just in case...*/
	if(this->newSubrequestStatus == SUBREQUEST_READYFORSCHED){
	  char jobManagerHost[CA_MAXHOSTNAMELEN+1];
	  jobManagerHost[0]='\0';
	  int jobManagerPort = DEFAULT_NOTIFICATION_PORT;

	  char *value = getconfent("JOBMANAGER", "NOTIFYPORT", 0);
	  
	  if(value){	    
	    jobManagerPort = std::strtol(value, 0, 10);
	    if (jobManagerPort < 0) {
	      jobManagerPort = DEFAULT_NOTIFICATION_PORT;
	    } else if (jobManagerPort > 65535) {
	      castor::exception::Exception e(EINVAL);
	      e.getMessage() << "(StagerJobRequestHandler)Invalid NOTIFYPORT value configured: " << jobManagerPort<< "- must be < 65535" << std::endl;
	      throw e;
	    }
	  }else{
	    castor::exception::Exception e(EINVAL);
	    e.getMessage() << "(StagerJobRequestHandler)Null JobManager NOTIFYPORT value configured: " << jobManagerPort << std::endl;
	    throw e;
	  }

	  
	  value =  getconfent("JOBMANAGER", "HOST", 0);
	  if(value){
	    strncpy(jobManagerHost, value, CA_MAXHOSTNAMELEN);
	    jobManagerHost[CA_MAXHOSTNAMELEN] = '\0';
	  }else{
	    castor::exception::Exception e(EINVAL);
	    e.getMessage() << "(StagerJobRequestHandler)Null JobManager HOST value configured: "<< jobManagerHost<<std::endl;
	    throw e;
	  }
	  
	  sendNotification(jobManagerHost, jobManagerPort, 1);
	}
      
      }



      /* --------------------------------------------------- */
      /* sendNotification()                                  */
      /*                                                     */
      /* Send a notification message to another CASTOR2      */
      /* daemon using the NotificationThread model. This     */
      /* will wake up the process and trigger it to perform  */
      /* a dedicated action.                                 */
      /*                                                     */
      /* This function is copied from BaseServer.cpp. We     */
      /* could have wrapped the C++ call to be callable in   */
      /* C. However, as the stager is being re-written in    */
      /* C++ anyway we take this shortcut.                   */
      /*                                                     */
      /* Input:  (const char *) host - host to notify        */
      /*         (const int) port - notification por         */
      /*         (const int) nbThreads - number of threads   */
      /*                      to wake up on the server       */
      /*                                                     */
      /* Return: nothing (void). All errors ignored          */
      /* --------------------------------------------------- */
      void StagerJobRequestHandler::sendNotification(const char *host, const int port, const int nbThreads) {
	struct hostent *hp;
	struct sockaddr_in sin;
	int s;
	char buf[HYPERSIZE + LONGSIZE];
	char *p;
	
	/* Check arguments */
	if ((host[0] == '\0') || !port) {
	  return;
	}
	
	/* Resolve host address */
	if ((hp = Cgethostbyname(host)) == NULL) {
	  return;
	}
	
	/* Prepare the request */
	p = buf;
	const long NOTIFY_MAGIC = 0x44180876;
	marshall_LONG(p, NOTIFY_MAGIC);
	marshall_LONG(p, nbThreads);
	
	// Create socket
	if ((s = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
	  return;
	}
	
	/* Send packet containing notification magic number + service number */
	memset((char *) &sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
	sendto(s, buf, sizeof(buf), 0, (struct sockaddr *) &sin, sizeof(struct sockaddr_in));
	netclose(s);
      }



        
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
