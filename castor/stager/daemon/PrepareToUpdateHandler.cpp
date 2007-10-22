/***********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
     
      StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StagePrepareToUpdateRequest;
	
	

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
	
	
	this->default_protocol = "rfio";
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();
	
      }

      /*******************************************/	
      /*     switch(getDiskCopyForJob):         */  
      /*        case 0: (staged) nothing to be done */                                   
      /*        case 1: (staged) waitD2DCopy  */
      /*        case 2: (waitRecall) createTapeCopyForRecall */
      void StagerPrepareToUpdateHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
	
	switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,this->sources)){
	case 0:
	  {
	    /* nothing to be done */
	  }break;
	case 1:
	  {
	    bool isToReplicate= replicaSwitch();
	    if(isToReplicate){
	      processReplica();
	    }
	    
	    /* build the rmjob struct and submit the job */
	    jobManagerPart();
	    
	    this->newSubrequestStatus = SUBREQUEST_READYFORSCHED;
	    if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
	      stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      /* we have to setGetNextStatus since the newSub...== SUBREQUEST_READYFORSCHED */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); 
	    }
	    
	    /* and we have to notify the jobManager */
	    m_notifyJobManager = true;
	    
	  }break;
	case 2:
	  {
	    stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	    /* though we wont update the subrequestStatus, we need to flag it for the stgReplyHelper */
	    this->newSubrequestStatus= SUBREQUEST_READY;
	  }break;
	  
	}//end switch

      }
      

      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerPrepareToUpdateHandler::preHandle() throw(castor::exception::Exception)
      {
	/* get the uuid subrequest string version and check if it is valid */
	/* we can create one !*/
	stgRequestHelper->setSubrequestUuid();
	
	/* get the associated client and set the iClientAsString variable */
	stgRequestHelper->getIClient();
	
	
	/* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
	stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
	
	/* get the uuid request string version and check if it is valid */
	stgRequestHelper->setRequestUuid();
	
	
	/* get the svcClass */
	stgRequestHelper->getSvcClass();
	
	
	/* create and fill request->svcClass link on DB */
	stgRequestHelper->linkRequestToSvcClassOnDB();
		
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	this->fileExist = stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
	
	/* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	stgRequestHelper->checkFilePermission();

	this->toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	
	castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","(RequestSvc) Detailed subrequest(fileName,{euid,egid},{userName,groupName},mode mask, cliendId, svcClassName,cnsFileid.fileid, cnsFileid.server"),
					  castor::dlf::Param("Standard Message",stgCnsHelper->subrequestFileName),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->euid),
					  castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->egid),
					  castor::dlf::Param("Standard Message",stgRequestHelper->username),
					  castor::dlf::Param("Standard Message",stgRequestHelper->groupname),
					  castor::dlf::Param("Standard Message",stgRequestHelper->fileRequest->mask()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->iClient->id()),
					  castor::dlf::Param("Standard Message",stgRequestHelper->svcClassName),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.fileid),
					  castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.server)};
	castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 11, parameter);

      }

      
      
      /*********************************************/
      /* handler for the PrepareToUpdate request  */
      /*******************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	StagerReplyHelper* stgReplyHelper=NULL;
	try{
	  
	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/

	  jobOriented();

	  
	 
	  /*************************/
	  /* huge or small flow?? */
	  /***********************/
	  
	  if( toRecreateCastorFile){/* we recreate, small flow*/
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      /* WE DONT DO ANYTHING */
	    }else{

	      /* we update the subrequestSatus*/
	      this->newSubrequestStatus = SUBREQUEST_READY;
	      if((this->currentSubrequestStatus) != (this->newSubrequestStatus)){
		stgRequestHelper->subrequest->setStatus(this->newSubrequestStatus);
		/* since newSubrequestStatus == SUBREQUEST_READY, we have to setGetNextStatus */
		stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);

		/* we are gonna replyToClient so we dont  updateRep on DB explicitly */
	      }

	      /*  replyToClient  */
	      stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);
	      if(stgReplyHelper == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
		throw(ex);
	      }
	      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0,"No Error");
	      stgReplyHelper->endReplyToClient(stgRequestHelper);
	      delete stgReplyHelper->ioResponse;
	      delete stgReplyHelper; 
	      
	    }
	    
	  }else{/* we schedule, huge flow */
	    
	    /* depending on the value returned by getDiskCopiesForJob */
	    /* if needed, we update the subrequestStatus internally */
	    switchDiskCopiesForJob();
	    
	    stgReplyHelper = new StagerReplyHelper(newSubrequestStatus);
	    if(stgReplyHelper == NULL){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerRepackHandler handle) Impossible to get the StagerReplyHelper"<<std::endl;
	      throw(ex);
	    }
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0,"No Error");
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	    delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper; 
	  
	  }  
	
	}catch(castor::exception::Exception e){

	  if(stgReplyHelper != NULL){
	    if(stgReplyHelper->ioResponse) delete stgReplyHelper->ioResponse;
	    delete stgReplyHelper;
	  }
	 
	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerPrepareToUpdateHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	}
      }


      


      StagerPrepareToUpdateHandler::~StagerPrepareToUpdateHandler()throw(){
	
      }
      
      

      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

