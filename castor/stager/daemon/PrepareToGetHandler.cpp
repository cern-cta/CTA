/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"

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
#include "castor/exception/Internal.hpp"

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
      
      StagerPrepareToGetHandler::StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePrepareToGetRequest;
        
       
      }

      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void StagerPrepareToGetHandler::handlerSettings() throw(castor::exception::Exception)
      {	
	this->maxReplicaNb= this->stgRequestHelper->svcClass->maxReplicaNb();
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
      }
 



      
      /*******************************************/	
      /*     switch(getDiskCopyForJob):         */  
      /*        case 0: (staged) archiveSubrequest */                                   
      /*        case 1: (staged) waitD2DCopy  */
      /*        case 2: (waitRecall) createRecallCandidate */
      void StagerPrepareToGetHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {

        switch(stgRequestHelper->stagerService->getDiskCopiesForJob(stgRequestHelper->subrequest,typeRequest,this->sources)){
	case -2:
	  {
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToGet"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	  }break;

   case -1:
	    {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToGet"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 6, params, &(stgCnsHelper->cnsFileid));
	    }break;
      
      
    case 0:   // DiskCopy STAGED
	  {
	    stgRequestHelper->subrequest->setStatus(SUBREQUEST_ARCHIVED);
	    stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
	    /* we are gona replyToClient so we dont updateRep on DB explicitly */
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToGet"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_ARCHIVE_SUBREQ, 6 ,params, &(stgCnsHelper->cnsFileid));
	    

	    /* we archive the subrequest */
	    stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
	  } break;
          
	case 1:    // DISK2DISKCOPY - will disappear soon
	  {
	      castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToGet"),
					   castor::dlf::Param(stgRequestHelper->subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					   castor::dlf::Param("UserName",stgRequestHelper->username),
					   castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					   castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	      };
	      castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_DISKTODISK_COPY, 6 ,params, &(stgCnsHelper->cnsFileid));
	      
      if(replicaSwitch()) {
	      processReplica();
	    }

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
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "PrepareToGet"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 6 ,params, &(stgCnsHelper->cnsFileid));
	    
	    stgRequestHelper->stagerService->createRecallCandidate(
								   stgRequestHelper->subrequest,stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), stgRequestHelper->svcClass);
          }break;
          
        }//end switch
        

      }//END switchDiskCopyForJob
      
      
      
      void StagerPrepareToGetHandler::handle() throw(castor::exception::Exception)
      {

	StagerReplyHelper* stgReplyHelper=NULL;
	try{

	  

	  preHandle();
	 

	  /* handlerSettings();*/

	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  }; 
	 
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_PREPARETOGET, 5 ,params, &(stgCnsHelper->cnsFileid));
	  

	  jobOriented();	  

	  /* depending on the value returned by getDiskCopiesForJob */
	  /* if needed, we update the subrequestStatus internally  */
	  switchDiskCopiesForJob();
	  
	 
	  stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);
	  stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
	  stgReplyHelper->endReplyToClient(stgRequestHelper);
       
	  delete stgReplyHelper;
    stgReplyHelper = 0;
	  

	}catch(castor::exception::Exception e){

	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/
	  if(stgReplyHelper != NULL) delete stgReplyHelper;
	  
	  castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOGET, 2 ,params, &(stgCnsHelper->cnsFileid));
	  
	  throw(e);
	}  


      }
      
      
      /***********************************************************************/
      /*    destructor                                                      */
      StagerPrepareToGetHandler::~StagerPrepareToGetHandler() throw()
      {
        
      }
      
    }//end namespace dbservice
  }//end namespace stager
}//end namespace castor
