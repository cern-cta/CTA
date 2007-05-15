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
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
     StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message)
      {

	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->message(message);

	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


	this->useHostlist = false;
#ifdef USE_HOSTLIST
	this->useHostlist=true;
#endif

	
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

	this->openflags=RM_O_RDONLY;
	this->default_protocol = "rfio";	
      }
      



      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void StagerGetHandler::handle()
      {
	/**/
	try{


	  this.jobOriented();

	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, &(this->sources));
	  switchScheduling(caseToSchedule);
	  
	  if((rfs != NULL)&&(!rfs.empty())){
	    /* if the file exists we don't have any size requirements */
	    this->xsize = 0;
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


      


      StagerGetHandler::~StagerGetHandler()throw(){
	
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
