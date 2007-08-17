/************************************************************************************************************/
/* StagerUpdateHandler: Contructor and implementation of the Update request handler                        */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                  */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put)  */
/* We dont  need to reply to the client (just in case of error )                                        */
/*******************************************************************************************************/




#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      /* constructor */
      StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, bool toRecreateCastorFile) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;


	/* depending on this flag, we ll execute the huge flow or the small one*/
	this->toRecreateCastorFile = toRecreateCastorFile;

	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();	
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


	
	/* get the subrequest's size required on disk */
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
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
      }
      

      /************************************/
      /* handler for the update request  */
      /**********************************/
      void StagerUpdateHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  jobOriented();
	  int caseToSchedule = -1;
	  
	  /* huge or small flow?? */
	  if(toRecreateCastorFile){/* we skip the processReplica part :) */    
	    /* use the stagerService to recreate castor file */
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      /* in this case we have to archiveSubrequest */
	      /* updateStatus to FAILED_FINISHED and replyToClient (both to be dones on StagerDBService, catch exception) */
	      this->stgRequestHelper->stagerService->archiveSubReq(this->stgRequestHelper->subrequest->id());
	      castor::exception::Exception ex(EBUSY);
	      ex.getMessage()<<"(StagerUpdateHandler handle) Recreation is not possible (null DiskCopyForRecall)"<<std::endl;
	      throw ex;

	    }else{ /* coming from the latest stager_db_service.c */
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
	      this->newSubrequestStatus= SUBREQUEST_READYFORSCHED;
	      if( (this->newSubrequestStatus) != (this->currentSubrequestStatus)){
		stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);

		/* since the newSubrequest... != SUBREQUEST_READY, we dont setGetNextStatus... = GETNEXTSTATUS_FILESTAGED */

		/* we dontReplyToClient so we have to update the subrequest on DB explicitly  */
		stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      }

	    }/* notSchedule && diskCopyForRecall != NULL  */

	  }else{/*if notToRecreateCastorFile */
	   
	    /* since the file exist, we need the read flag */
	    caseToSchedule = stgRequestHelper->stagerService->isSubRequestToSchedule(stgRequestHelper->subrequest,this->sources);
	    switchScheduling(caseToSchedule);/* we call internally the rmjob */
	    /* we update internally the subrequestStatus */
	  }
	  
	 
	}catch(castor::exception::Exception e){
	 
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerUpdateHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	}
      }
      



      /* destructor */
      StagerUpdateHandler::~StagerUpdateHandler() throw()
      {
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
