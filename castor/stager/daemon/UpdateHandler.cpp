/****************************************************************************************************************************/
/* handler for the Update subrequest, since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class*/
/* depending if the file exist, it can follow the huge flow (jobOriented, as Get) or a small one                          */
/* we dont  need to reply to the client (just necessary )*/
/************************************************************************************************************************/




#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      /* constructor */
      StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message, bool toRecreateCastorFile)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->message(message);

	/* depending on this flag, we ll execute the huge flow or the small one*/
	this->toRecreateCastorFile = toRecreateCastorFile;

	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();

	
	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif
	
	/* get the request's size required on disk */
	this->xsize = this->stgRequestHelper->subrequest->xsize();

	if( xsize <= 0 ){
	  /* get the default filesize */
	  u_signed64 defaultFileSize = this->stgRequestHelper->svcClass->defaultFileSize();
	  if( defaultFileSize <= 0){
	    xsize = DEFAULTFILESIZE;
	    /* before enter the job, we ll need to print a message */
	  }
	}

	this->openflags=RM_O_WRONLY;
	this->default_protocol = "rfio";	
	
      }
      

      /************************************/
      /* handler for the update request  */
      /**********************************/
      void :StagerUpdateHandler::handle(void *param) throw()
      {
	/**/
	try{
	  
	  
	  this.jobOriented();


	  /* huge or small flow?? */
	  if(toRecreateCastorFile){/* we skip the processReplica part :) */


	    /* use the stagerService to recreate castor file */
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    	    
	    if(diskCopyForRecall == NULL){
	      /*throw exception
	       there is no need to change the subrequest status!!!
	       archive_subrequest
	      */
	    }

	    /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
	    std::string diskServername = diskCopyForRecall->diskServerName();
	    std::string mountPoint = diskCopyForRecall->mountPoint();
	    
	    if((!diskServerName.empty())&&(!mountPoint.empty())){
	      this->rfs = diskServerName + ":" + mountPoint;
	    }
	    this->hostlist.clear();
	    

	  }else{

	    int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToBeScheduled(stgRequestHelper->subrequest,&(this->sources));
	    switchScheduling(caseToSchedule);
	    

	  }
	  /* build the rmjob struct and submit the job */
	  this->rmjob = stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	  buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	  rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)); //throw exception
	  
	  
	  SubRequestStatusCodes currentSubrequestStatus = stgRequestHelper->subrequest->status();
	  SubRequestStatusCodes newSubrequestStatus = SUBREQUEST_READY;
	  
	  if(newSubrequestStatus != currentSubrequestStatus){
	    
	    stgRequestHelper->subrequest->setStatus(newSubrequestStatus);
	    stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUSFILESTAGED);

	    /* since we don't reply to the client, we have to update explicitly the representation*/
	    stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, iObj_subrequest, STAGER_AUTOCOMMIT_TRUE);
	  }
	  
	  
	  
	  
	  
	}catch{
	}	
      
	
      }
      
      /* destructor */
      StagerUpdateHandler::~StagerUpdateHandler() throw()
      {
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
