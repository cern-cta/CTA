/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/


#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"
#include "StagerGetHandler.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/rm_struct.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/osdep.h"

#include "../../IClientFactory.hpp"
#include "../SubRequestStatusCodes.hpp"
#include "../SubRequestGetNextStatusCodes.hpp"
#include "../../exception/Exception.hpp"


#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {

	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	

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
      void StagerGetHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  jobOriented();
	  
	  int caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest, &(this->sources));
	  switchScheduling(caseToSchedule);
	  
	  if(rfs.empty() == false){
	    /* if the file exists we don't have any size requirements */
	    this->xsize = 0;
	  }
	  
	  
	  /* build the rmjob struct and submit the job */
	  stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	  this->buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	  if(rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerGetHandler handle) Error on rm_enterjob"<<std::endl;
	    throw(ex);	  
	  }
	  rm_freejob(this->rmjob_out);

	  /*  Update subrequestStatus */
	  if((caseToSchedule != 2) && (caseToSchedule != 0)){
	    stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
	    stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	  }
	}catch(castor::exception::Exception ex){
	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  if(rmjob_out != NULL){
	    rm_freejob(this->rmjob_out);
	  }
	  stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw(ex);
	}
	
	  
      }


      


      StagerGetHandler::~StagerGetHandler()throw(){
	
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
