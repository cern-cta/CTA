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

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

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
        
      }

      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerPrepareToUpdateHandler::handlerSettings() throw(castor::exception::Exception)
      {	
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
      }

      
      /*******************************************/	
      /*     switch(getDiskCopyForJob):         */  
      /*        case 0: (staged) nothing to be done */                                   
      /*        case 1: (staged) waitD2DCopy  */
      /*        case 2: (waitRecall) createTapeCopyForRecall */
      void StagerPrepareToUpdateHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        
        switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,typeRequest,this->sources)){
	case -2:
	  {
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	  }break;

   case -1:
	    {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 6, params, &(stgCnsHelper->cnsFileid));
	    }break;
      
	case 0:
	  {
	  /* nothing to be done */
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_NOTHING_TOBEDONE, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	  }break;
	  
	case 1:
	  {
	    if(replicaSwitch())
	      processReplica();
	    
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_DISKTODISK_COPY, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	    /* build the rmjob struct and submit the job */
	    jobManagerPart();
	    
	    stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
	    stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); 
	    stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	    
	    /* and we have to notify the jobManager */
	    m_notifyJobManager = true;
	  }break;
	    
	case 2:
	  {
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	    stgRequestHelper->stagerService->createRecallCandidate(
								   stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	  }break;
        }
        
      }
  
  
  
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerPrepareToUpdateHandler::preHandle() throw(castor::exception::Exception)
      {
	
	/* set the username and groupname needed to print them on the log */
	stgRequestHelper->setUsernameAndGroupname();

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
	this->fileExist = stgCnsHelper->checkAndSetFileOnNameServer(stgRequestHelper->subrequest->fileName(), this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
	
	/* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	stgRequestHelper->checkFilePermission();
    
	this->toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
   
    
      }
  
  
  
  /*********************************************/
  /* handler for the PrepareToUpdate request  */
  /*******************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
	StagerReplyHelper* stgReplyHelper=NULL;
	try{
      
	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/
	  
	  handlerSettings();

	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  }; 
	 
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, 5 ,params, &(stgCnsHelper->cnsFileid));
	  
	  jobOriented();
	  
	  
	  
	  /*************************/
	  /* huge or small flow?? */
	  /***********************/
	  
	  if( toRecreateCastorFile){/* we recreate, small flow*/
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_WARNING, STAGER_RECREATION_IMPOSSIBLE, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
	      
	    }else{
	       castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToUpdate"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, 6 ,params, &(stgCnsHelper->cnsFileid));
            
	      stgRequestHelper->subrequest->setStatus(SUBREQUEST_READY);
	      /* since newSubrequestStatus == SUBREQUEST_READY, we have to setGetNextStatus */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
	      
	      /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
	      stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
	      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0,"No Error");
	      stgReplyHelper->endReplyToClient(stgRequestHelper);
	      
	      delete stgReplyHelper; 
	      
	    }
	    
	  }else{/* we schedule, huge flow */
	    
	    /* depending on the value returned by getDiskCopiesForJob */
	    /* if needed, we update the subrequestStatus internally */
	    switchDiskCopiesForJob();
	    
	    stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
	    stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0,"No Error");
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	   
	    delete stgReplyHelper; 
	  }
	  
	}catch(castor::exception::Exception e){
	  
	  if(stgReplyHelper != NULL) delete stgReplyHelper;
	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOUPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
	  
	  throw(e);
	}
  }
  



    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

