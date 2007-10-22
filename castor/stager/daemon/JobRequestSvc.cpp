/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/JobRequestSvc.hpp"

#include "castor/stager/dbService/StagerGetHandler.hpp"
#include "castor/stager/dbService/StagerPutHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"

#include "castor/stager/dbService/RequestSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "serrno.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "osdep.h"
#include "Cnetdb.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "stager_uuid.h"
#include "Cuuid.h"
#include "u64subr.h"
#include "marshall.h"
#include "net.h"

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

     
      /****************/
      /* constructor */
      /**************/
      JobRequestSvc::JobRequestSvc(std::string jobManagerHost, int jobManagerPort) throw() :
        m_jobManagerHost(jobManagerHost), m_jobManagerPort(jobManagerPort)
      {
	this->nameRequestSvc = "JobReqSvc";
      }

     
      /*************************************************************/
      /* Method to get a subrequest to do using the StagerService */
      /***********************************************************/
      castor::IObject* JobRequestSvc::select() throw(castor::exception::Exception){
	castor::stager::IStagerSvc* stgService;

	castor::IService* svc =
	  castor::BaseObject::services()->
	  service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
	if (0 == svc) {
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(JobRequestSvc) Impossible to get the stgService"<<std::endl;
	  throw ex;
	}
	stgService = dynamic_cast<castor::stager::IStagerSvc*>(svc);
	if (0 == stgService) {
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(JobRequestSvc) Got a bad stgService"<<std::endl;
	  throw ex;
	}
	
	castor::stager::SubRequest* subrequestToProcess = stgService->subRequestToDo(this->nameRequestSvc);
	
	return(subrequestToProcess);
      }


      /*********************************************************/
      /* Thread calling the specific request's handler        */
      /***************************************************** */
      void JobRequestSvc::process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception){
	StagerCnsHelper* stgCnsHelper= NULL;
	StagerRequestHelper* stgRequestHelper= NULL;
	StagerJobRequestHandler* stgRequestHandler = NULL;

	try {


	  /*******************************************/
	  /* We create the helpers at the beginning */
	  /*****************************************/
	  
	  stgCnsHelper = new StagerCnsHelper();
	  if(stgCnsHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(JobRequestSvc process) Impossible to create the StagerCnsHelper"<<std::endl;
	    throw ex;
	  }

	  
	  int typeRequest=0;
	 stgRequestHelper = new StagerRequestHelper(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess), typeRequest);
	  if(stgRequestHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<< "(JobRequestSvc process) Impossible to create the StagerRequestHelper"<<std::endl;
	    throw ex;
	  }
	  

	  
	  stgRequestHandler;

	  switch(typeRequest){
	 
	  case OBJ_StageGetRequest:
	    {
	      stgRequestHandler = new StagerGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerGetHandler"<<std::endl;
		throw ex;
	      }	      	     
	    }
	    break;
	    
	  case OBJ_StagePutRequest:
	    {
	      stgRequestHandler = new StagerPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerPutHandler"<<std::endl;
		throw ex;
	      }
	    }
	    break;

	  case OBJ_StageUpdateRequest:
	    {
	      /* bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0)); */
	      stgRequestHandler = new StagerUpdateHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerUpdateHandler"<<std::endl;
		throw ex;
	      }
	    }
	    break;

	  }// end switch(typeRequest)

	  /**********************************************/
	  /* inside the handle(), call to preHandle() */

	  stgRequestHandler->handle();

    if (stgRequestHandler->notifyJobManager()) {
   	  castor::server::BaseServer::sendNotification(m_jobManagerHost, m_jobManagerPort, 1);
    }

	  /******************************************/
	
	  delete stgRequestHandler;
	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper) delete stgCnsHelper;
	 
	  
	}catch(castor::exception::Exception ex){ /* process the exception an replyToClient */

	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  if((stgRequestHelper != NULL)&&(stgCnsHelper != NULL)){
	    handleException(stgRequestHelper,stgCnsHelper, ex.code(), ex.getMessage().str());
	  }else{
	    std::cerr<<"(JobRequestSvc handleException)"<<ex.code()<<ex.getMessage().str()<<std::endl;
	  }
	  
	  /* we delete our objects */
	  if(stgRequestHandler) delete stgRequestHandler;	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }	  
	  if(stgCnsHelper) delete stgCnsHelper;

 	}catch (...){
	  
	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in JobRequestSvc")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);	  
	  if((stgRequestHelper != NULL)&&(stgCnsHelper != NULL)){
	    handleException(stgRequestHelper,stgCnsHelper, SEINTERNAL, "General Exception");
	  }else{
	    std::cerr<<"(JobRequestSvc handleException) Caught general Exception"<<std::endl;
	  }

	  /* we delete our objects */
	  if(stgRequestHandler) delete stgRequestHandler;	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }	  
	  if(stgCnsHelper) delete stgCnsHelper;
	   
	}


      }/* end JobRequestSvc::process */



      /*********************/
      /* empty destructor */
      /*******************/
      JobRequestSvc::~JobRequestSvc() throw(){

      }

   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
