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
	
	this->default_protocol = "rfio";	
       
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
	  
	  if(diskCopyForRecall != NULL){  	  
	    /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
	    std::string diskServerName(diskCopyForRecall->diskServer());
	    std::string mountPoint(diskCopyForRecall->mountPoint());

	    if((diskServerName.empty() == false) && (mountPoint.empty() == false)){
	      rfs.resize(CA_MAXRFSLINELEN);
	      rfs.clear();
	      this->rfs = diskServerName + ":" + mountPoint;

	      if(rfs.size() >CA_MAXRFSLINELEN ){
		 castor::exception::Exception ex(SENAMETOOLONG);
		 ex.getMessage()<<"(Stager_Handler) Not enough space for filesystem constraint"<<std::endl;
		 throw ex;
	      }else{
		/* update the subrequest table (coming from the latest stager_db_service.c) */
		stgRequestHelper->subrequest->setRequestedFileSystems(this->rfs);
		stgRequestHelper->subrequest->setXsize(this->xsize);
		
	      } 
	    }
	    
	    /* updateSubrequestStatus Part: */
	    this->stgRequestHelper->updateSubrequestStatus(SUBREQUEST_READYFORSCHED);
	    stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest,true);

	  }else{
	    /* in this case we have to archiveSubrequest */
	    /* updateStatus to FAILED_FINISHED and replyToClient (both to be dones on StagerDBService, catch exception) */
	    this->stgRequestHelper->stagerService->archiveSubReq(this->stgRequestHelper->subrequest->id());
	    castor::exception::Exception ex(EBUSY);
	    ex.getMessage()<<"(StagerPutHandler handle) Recreation is not possible (null DiskCopyForRecall)"<<std::endl;
	    throw ex; 
	  }
	  
	}catch(castor::exception::Exception e){
	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerGetHandler) Error"<<e.getMessage()<<std::endl;
	  throw ex;
	}
	
      }/* end StagerPutHandler::handle()*/
      
      
      
      

      StagerPutHandler::~StagerPutHandler()throw(){
	
      }



 
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

