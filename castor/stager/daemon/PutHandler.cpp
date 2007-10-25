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
      
      StagerPutHandler::StagerPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePutRequest;
        
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
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          preHandle();
          /**********/
	  castor::dlf::Param params[]={castor::dlf::Param(stgRequestHelper->subrequestUuid),
				       castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
				       castor::dlf::Param("UserName",stgRequestHelper->username),
				       castor::dlf::Param("GroupName", stgRequestHelper->groupname),
				       castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)				     
	  };
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_DEBUG, STAGER_PUT, 5 ,params, &(stgCnsHelper->cnsFileid));
	  
	
          jobOriented();
          
          /* use the stagerService to recreate castor file */
          castor::stager::DiskCopyForRecall* diskCopyForRecall = 
            stgRequestHelper->stagerService->recreateCastorFile(stgRequestHelper->castorFile,stgRequestHelper->subrequest);
          
          if(diskCopyForRecall){  	  
            /* we never replicate... we make by hand the rfs (and we don't fill the hostlist) */
            if(!diskCopyForRecall->mountPoint().empty()) {
              stgRequestHelper->subrequest->setRequestedFileSystems(diskCopyForRecall->diskServer() + ":" + diskCopyForRecall->mountPoint());
            }
            stgRequestHelper->subrequest->setXsize(this->xsize);
            
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Put"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
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
          } else{/* diskCopyForRecall != NULL */
	    castor::dlf::Param params[]={castor::dlf::Param("Request type:", "Put"),
					 castor::dlf::Param(stgRequestHelper->subrequestUuid),
					 castor::dlf::Param("Subrequest fileName",stgCnsHelper->subrequestFileName),
					 castor::dlf::Param("UserName",stgRequestHelper->username),
					 castor::dlf::Param("GroupName", stgRequestHelper->groupname),
					 castor::dlf::Param("SvcClassName",stgRequestHelper->svcClassName)					 
	    };
	    castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_USER_ERROR, STAGER_RECREATION_IMPOSSIBLE, 6 ,params, &(stgCnsHelper->cnsFileid));
            

	  }
          
        }catch(castor::exception::Exception e){
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
				       castor::dlf::Param("Error Message",e.getMessage().str())
	  };
	  
	  castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PUT, 2 ,params, &(stgCnsHelper->cnsFileid));
	  throw(e);
	 
        }
        
      }/* end StagerPutHandler::handle()*/
      
      
      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

