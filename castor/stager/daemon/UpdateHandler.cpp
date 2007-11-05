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

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
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
      
      /* constructor */
      StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StageUpdateRequest;     
          
      }
      
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerUpdateHandler::handlerSettings() throw(castor::exception::Exception)
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
      }


      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the fileExist */
      void StagerUpdateHandler::preHandle() throw(castor::exception::Exception)
      {
        
	/* get the uuid request string version and check if it is valid */
	stgRequestHelper->setRequestUuid();

	/* we create the StagerCnsHelper inside and we pass the requestUuid needed for logging */
	this->stgCnsHelper = new StagerCnsHelper(stgRequestHelper->requestUuid);

	/* set the username and groupname needed to print them on the log */
	stgRequestHelper->setUsernameAndGroupname();

        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* get the associated client and set the iClientAsString variable */
        stgRequestHelper->getIClient();
        
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
       
        
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

      /********************************************/	
      /* for Get, Update                         */
      /*     switch(getDiskCopyForJob):         */                                     
      /*        case 0,1: (staged) jobManager  */
      /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
      void StagerUpdateHandler::switchDiskCopiesForJob() throw(castor::exception::Exception)
      {
        
        switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,typeRequest,this->sources)){
	case -2:
	  {
	    
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, 6, params, &(stgCnsHelper->cnsFileid));
	  }break;
	  
	  case -1:
	    {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 6, params, &(stgCnsHelper->cnsFileid));
	    }break;

          case 0: // DISKCOPY_STAGED, schedule job
	    {
	      bool isToReplicate= replicaSwitch();
	      if(isToReplicate){
		processReplica();
	      }
	      
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_SCHEDULINGJOB, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
	      /* build the rmjob struct and submit the job */
	      jobManagerPart();
	      
	     
	      stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      /* we have to setGetNextStatus since the newSub...== SUBREQUEST_READYFORSCHED */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); /* 126 */	      
	      
	      /* and we have to notify the jobManager */
	      m_notifyJobManager = true;
	    }break;
      
	case 1:   // DISK2DISKCOPY - will disappear soon
	    {
	      bool isToReplicate= replicaSwitch();
	      if(isToReplicate){
		processReplica();
	      }
	      
	       castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
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
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); /* 126 */
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      
	      /* and we have to notify the jobManager */
	      m_notifyJobManager = true;
	    }break;
          
	case 2: /* create a tape copy and corresponding segment objects on stager catalogue */
          {
            stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	     castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 6 ,params, &(stgCnsHelper->cnsFileid));
            
          }break;
          
        }//end switch
        
      }




      /************************************/
      /* handler for the update request  */
      /**********************************/
      void StagerUpdateHandler::handle() throw(castor::exception::Exception)
      {
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
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_UPDATE, 5 ,params, &(stgCnsHelper->cnsFileid));
	  
	
	  jobOriented();
	  
	  
	  /* huge or small flow?? */
	  if(toRecreateCastorFile){/* we skip the processReplica part :) */    
	    /* use the stagerService to recreate castor file */
	    castor::stager::DiskCopyForRecall* diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
	    
	    if(diskCopyForRecall == NULL){
	      /* in this case we have to archiveSubrequest */
	      /* updateStatus to FAILED_FINISHED and replyToClient (both to be dones on StagerDBService, catch exception) */
	      this->stgRequestHelper->stagerService->archiveSubReq(this->stgRequestHelper->subrequest->id());

	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_RECREATION_IMPOSSIBLE, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
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
		  castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					       castor::dlf::Param(stgRequestHelper->subrequestUuid),
					       castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					       castor::dlf::Param("UserName",stgRequestHelper->username),
					       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
		  };
		  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_INVALID_FILESYSTEM, 6 ,params, &(stgCnsHelper->cnsFileid));
		  castor::exception::Exception ex(SENAMETOOLONG);
		  ex.getMessage()<<"Not enough space for filesystem constraint"<<std::endl;
		  throw ex;
		}else{
		  /* update the subrequest table (coming from the latest stager_db_service.c) */
		  stgRequestHelper->subrequest->setRequestedFileSystems(this->rfs);
		  stgRequestHelper->subrequest->setXsize(this->xsize);
		  
		} 
	      }

	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Update"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_CASTORFILE_RECREATION, 6 ,params, &(stgCnsHelper->cnsFileid));
            
	      /* updateSubrequestStatus Part: */
	      stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      
	      /* and we have to notify the jobManager */
	      m_notifyJobManager = true;
	      
	    }/* notSchedule && diskCopyForRecall != NULL  */

	  }else{/*if notToRecreateCastorFile */
	   
	    /* for Get, Update                         */
	    /*     switch(getDiskCopyForJob):         */                                     
	    /*        case 0,1: (staged) jobManager  */
	    /*        case 2: (waitRecall) createTapeCopyForRecall */
	    /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
	    switchDiskCopiesForJob(); 
	  }
	  
	 
	}catch(castor::exception::Exception e){
	 
	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_UPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw(e);
	 
	}

      }
      
      
      
      
      /* destructor */
      StagerUpdateHandler::~StagerUpdateHandler() throw()
      {
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
