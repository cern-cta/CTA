/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/RequestSvc.hpp"
#include "castor/stager/dbService/PreRequestSvc.hpp"

#include "castor/stager/dbService/StagerGetHandler.hpp"
#include "castor/stager/dbService/StagerRepackHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"
#include "castor/stager/dbService/StagerPutHandler.hpp"
#include "castor/stager/dbService/StagerPutDoneHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"
#include "castor/stager/dbService/StagerRmHandler.hpp"
#include "castor/stager/dbService/StagerSetGCHandler.hpp"




#include "castor/server/SelectProcessThread.hpp"
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

#define PREREQ_HANDLERS 4



namespace castor{
  namespace stager{
    namespace dbService{

     
      /****************/
      /* constructor */
      /**************/
      PreRequestSvc::PreRequestSvc(std::string jobManagerHost, int jobManagerPort) throw() :
        m_jobManagerHost(jobManagerHost), m_jobManagerPort(jobManagerPort)
      {
	
	this->nameRequestSvc = "PrepReqSvc";
	
	
	/* Initializes the DLF logging */
	/*	castor::dlf::Messages messages[]={{1, "Starting PreRequestSvc Thread"},{2, "StagerRequestHelper"},{3, "StagerCnsHelper"},{4, "StagerReplyHelper"},{5, "StagerRequestHelper failed"},{6, "StagerCnsHelper failed"},{7, "StagerReplyHelper failed"},{8, "StagerHandler"}, {9, "StagerHandler successfully finished"},{10,"StagerHandler failed finished"},{11, "PreRequestSvc Thread successfully finished"},{12, "PreRequestSvc Thread failed finished"}};
		castor::dlf::dlf_init("PreRequestSvc", messages);*/
      }


      /*********************************************************/
      /* Thread calling the specific request's handler        */
      /***************************************************** */
      void PreRequestSvc::process(castor::IObject* subRequestToProcess) throw() {
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
	 

	  
	  

	  switch(typeRequest){
	 
	    
	  case OBJ_StagePrepareToGetRequest:
	    {	      
	      stgRequestHandler = new StagerPrepareToGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(PreRequestSvc) Impossible to execute the StagerPrepareToGetHandler"<<std::endl;
		throw ex;
	      }
	    }
	    break;


	  case OBJ_StageRepackRequest:
	    {
	      stgRequestHandler = new StagerRepackHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(PreRequestSvc) Impossible to execute the StagerRepackHandler"<<std::endl;
		throw ex;
	      }
	    }
	    break;

	  case OBJ_StagePrepareToPutRequest:
	    {
	      stgRequestHandler = new StagerPrepareToPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(PreRequestSvc) Impossible to execute the StagerPrepareToPutHandler"<<std::endl;
		throw ex;
	      }
	    }
	    break;

	  case OBJ_StagePrepareToUpdateRequest:
	    {
	      /* bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));*/
	      stgRequestHandler = new StagerPrepareToUpdateHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRequestHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(PreRequestSvc) Impossible to execute the StagerPrepareToUpdateHandler"<<std::endl;
		throw ex;
	      }	      
	    }
	    break;
  
	      
	  }//end switch(typeRequest)

	  /**********************************************/
	  /* inside the handle(), call to preHandle() */

	  stgRequestHandler->handle();
    
    if (stgRequestHandler->notifyJobManager()) {
   	  castor::server::BaseServer::sendNotification(m_jobManagerHost, m_jobManagerPort, 1);
    }


	  /********************************************/
	 
	  delete stgRequestHandler;

	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper) delete stgCnsHelper;
	  
	  /* we have to process the exception and reply to the client in case of error  */
	}catch(castor::exception::Exception ex){

	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  if((stgRequestHelper != NULL) &&(stgCnsHelper != NULL)){
	    handleException(stgRequestHelper, stgCnsHelper, ex.code(), ex.getMessage().str());
	  }else{
	    std::cerr<<"(PreRequestSvc handleException)"<<strerror(ex.code())<<ex.getMessage().str()<<std::endl;
	  }
	  
	  /* we delete our objects */
	  if(stgRequestHandler) delete stgRequestHandler;	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }	  
	  if(stgCnsHelper) delete stgCnsHelper;
	 
	}catch (...){
	  
	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in PreRequestSvc")}; 
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  if((stgRequestHelper != NULL) &&(stgCnsHelper != NULL)){
	    handleException(stgRequestHelper,stgCnsHelper,SEINTERNAL, "General Exception");
	  }else{
	    std::cerr<<"(PreRequestSvc handleException)Caught general Exception"<<std::endl;
	  }

	  /* we delete our objects */
	  if(stgRequestHandler) delete stgRequestHandler;	  
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }	  
	  if(stgCnsHelper) delete stgCnsHelper;

	}


      }/* end PreRequestSvc::process */


       /****************/
      /* destructor  */
      /**************/
      PreRequestSvc::~PreRequestSvc() throw(){


      }
   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
