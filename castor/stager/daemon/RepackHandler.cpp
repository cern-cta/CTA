/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerRepackHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

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
     
      StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper) throw (castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_StageRepackRequest;	
      }

      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerRepackHandler::handlerSettings() throw(castor::exception::Exception)
      {	
	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();


      	
	/* get the request's size required on disk */
	/* depending if the file exist, we ll need to update this variable */
	this->xsize = stgRequestHelper->subrequest->xsize();

	if( xsize > 0){
	  if( xsize < (stgCnsHelper->cnsFilestat.filesize)){
	    /* warning, user is requesting less bytes than the real size */
	    /* just print a message. we don't update xsize!!! */	 
	  }

	}else{
	    xsize = stgCnsHelper->cnsFilestat.filesize;
	    /* before enter the job, we ll need to print a message */
	}

	this->default_protocol = "rfio";
	
      }




      /*******************************************/	
      /*     switch(getDiskCopyForJob):         */  
      /*        case 0: (staged) call startRepackMigration (once implemented) */                                   
      /*        case 1: (staged) waitD2DCopy  */
      /*        case 2: (waitRecall) createRecallCandidate */
      bool StagerRepackHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
	switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,typeRequest,this->sources)){
	case -2:
	  {
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Repack"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, 6 ,params, &(stgCnsHelper->cnsFileid));
	    return false;
	  }break;

   case -1:
	    {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Repack"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 6, params, &(stgCnsHelper->cnsFileid));
	    return false;
	    }break;
      	case 0:
	  {
	  
	    /* to be done */
	    /* stgRequestHelper->stagerService->startRepackMigration */
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Repack"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };

	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_REPACK_MIGRATION, 6 ,params, &(stgCnsHelper->cnsFileid));
      return true;
	  }break;

	case 1:
	  {
	    if(replicaSwitch()) {    // XXX to be checked: a Repack request should be granted anyway
	      processReplica();
	    }
	    
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Repack"),
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
	    return true;
	  }break;
	case 2:
	  {
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Repack"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 6 ,params, &(stgCnsHelper->cnsFileid));
	    stgRequestHelper->stagerService->createRecallCandidate(
								   stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	    return true;
	    }break;

	}//end switch

      }
      


      void StagerRepackHandler::handle() throw(castor::exception::Exception)
      {
	/**/
	StagerReplyHelper* stgReplyHelper= NULL;
	try{

	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/
	  
	  handlerSettings();

	  castor::dlf::Param params[]={ castor::dlf::Param(stgRequestHelper->subrequestUuid),
					castor::dlf::Param("Subrequest fileName",stgRequestHelper->subrequest->fileName()),
					castor::dlf::Param("UserName",stgRequestHelper->username),
					castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_REPACK, 5 ,params, &(stgCnsHelper->cnsFileid));
	      

	  /* job oriented part */
	  jobOriented();
	  
	  /* depending on the value returned by getDiskCopiesForJob */
	  /* if needed, we update the subrequestStatus internally */
	  if(switchDiskCopiesForJob()) {
	 
      stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
      stgReplyHelper->endReplyToClient(stgRequestHelper);
     
      delete stgReplyHelper;
	  }

	}catch(castor::exception::Exception e){
	 
	  if(stgReplyHelper) delete stgReplyHelper;
	 
	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_REPACK, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw e;
	}
      }


      /***********************************************************************/
      /*    destructor                                                      */
      StagerRepackHandler::~StagerRepackHandler() throw()
      {
      }

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
