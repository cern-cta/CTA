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

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "osdep.h"

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
      
      StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {

	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StageGetRequest;
      }
      

      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerGetHandler::handlerSettings() throw(castor::exception::Exception)
      {	
	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();	
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
	
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


	this->default_protocol = "rfio";

      }


      

      /********************************************/	
      /* for Get, Update                         */
      /*     switch(getDiskCopyForJob):         */                                     
      /*        case 0,1: (staged) jobManager  */
      /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
      void StagerGetHandler::switchDiskCopiesForJob() throw(castor::exception::Exception)
      {
	  
	  switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,typeRequest,this->sources)){
	case -2:
	  {
	    
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Get"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, 6, params, &(stgCnsHelper->cnsFileid));
	  }break;
	  
   case -1:
	    {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Get"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
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
	      
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Get"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
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
	      
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Get"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_DISKTODISK_COPY, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
	      /* build the rmjob struct and submit the job */
	      jobManagerPart();
	      
	     
	      stgRequestHelper->subrequest->setStatus(SUBREQUEST_READYFORSCHED);
	      stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      /* we have to setGetNextStatus since the newSub...== SUBREQUEST_READYFORSCHED */
	      stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED); /* 126 */	      
	      
	      /* and we have to notify the jobManager */
	      m_notifyJobManager = true;
	    }break;
          
	case 2: /* create a tape copy and corresponding segment objects on stager catalogue */
          {
	     castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Get"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
            stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
	    
          }break;
          
        }//end switch

        
      }




      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void StagerGetHandler::handle() throw (castor::exception::Exception)
      {
	try{
	  
	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/

	  handlerSettings();

	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_GET, 5 ,params, &(stgCnsHelper->cnsFileid));
	  

	  jobOriented();
	  
	  /* for Get, Update                         */
	  /*     switch(getDiskCopyForJob):         */                                     
	  /*        case 0,1: (staged) jobManager  */
	  /*        case 2: (waitRecall) createTapeCopyForRecall */
	  /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
	  switchDiskCopiesForJob(); 

	 

	}catch(castor::exception::Exception e){

	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_GET, 2, params, &(stgCnsHelper->cnsFileid));
	  throw(e);
	}  
      }

      StagerGetHandler::~StagerGetHandler()throw(){
	
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
