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
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerPutHandler::StagerPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {


	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;


	/* we don't care about: maxReplicaNb, replicationPolicy, hostlist */
	
	/* get the request's size required on disk */
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
	
	
	this->openflags=RM_O_WRONLY;
	this->default_protocol = "rfio";	

	this->caseSubrequestFailed = false;
       
      }


      
      /*********************************/
      /* handler for the Put request  */
      /*******************************/
      void StagerPutHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	try{
	  jobOriented();
	  
	  /* use the stagerService to recreate castor file */
	  castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	  
	  if(diskCopyForRecall == NULL){
	    /* we don't archiveSubrequest, changeSubrequestStatus or replyToClient */
	    this->caseSubrequestFailed = true;
	  }
	  
	  
	  /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
	  std::string diskServerName(diskCopyForRecall->diskServer());
	  std::string mountPoint(diskCopyForRecall->mountPoint());
	  
	  if((!diskServerName.empty())&&(!mountPoint.empty())){
	    this->rfs = diskServerName + ":" + mountPoint;
	  }
	  this->hostlist.clear();
	  
	  /* build the rmjob struct and submit the job */
	  stgRequestHelper->buildRmJobHelperPart(&(this->rmjob)); /* add euid, egid... on the rmjob struct  */
	  buildRmJobRequestPart();/* add rfs and hostlist strings on the rmjob struct */
	  if(rm_enterjob(NULL,-1,(u_signed64) 0, &(this->rmjob), &(this->nrmjob_out), &(this->rmjob_out)) !=0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerPutHandler handle) Error on rm_enterjob"<<std::endl;
	    throw ex;
	  }
	  rm_freejob(rmjob_out);
	  
	  /* updateSubrequestStatus Part: */
	  if(this->caseSubrequestFailed == false){
	    this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READY);
	    stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest,true);
	  }

	}catch(castor::exception::Exception ex){
	  if(rmjob_out != NULL){
	    rm_freejob(rmjob_out);
	  }
	  this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	  throw ex;
	}
      }/* end StagerPutHandler::handle()*/
      
      
      
      

      StagerPutHandler::~StagerPutHandler()throw(){
	
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

