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
     
      StagerPutHandler::StagerPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {


	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->message(message);


	/* we don't care about: maxReplicaNb, replicationPolicy, hostlist */
	
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


      
      /*********************************/
      /* handler for the Put request  */
      /*******************************/
      void StagerPutHandler::handle()
      {
	/**/
	try{


	  this.jobOriented();
	  
	  /* use the stagerService to recreate castor file */
	  castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);

	  if(diskCopyForRecall == NULL){
	    // we throw exception!!!
	    //and we don't update the subrequest status
	  }


	  /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
	  std::string diskServername = diskCopyForRecall->diskServerName();
	  std::string mountPoint = diskCopyForRecall->mountPoint();

	  if((!diskServerName.empty())&&(!mountPoint.empty())){
	    this->rfs = diskServerName + ":" + mountPoint;
	  }
	  this->hostlist.clear();

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


      


      StagerPutHandler::~StagerPutHandler()throw(){
	
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

