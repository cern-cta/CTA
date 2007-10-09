/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StgRequestSvc.hpp"

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
#include "castor/stager/SubRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#define STGREQ_HANDLERS 3



namespace castor{
  namespace stager{
    namespace dbService{

     
      /****************/
      /* constructor */
      /**************/
      StgRequestSvc::StgRequestSvc() throw()
      {
	
	this->types.resize(STGREQ_HANDLERS);
	ObjectsIds auxTypes[] = { OBJ_StageRmRequest,
				  OBJ_SetFileGCWeight,
				  OBJ_StagePutDoneRequest};
	
	for(int i= 0; i< STGREQ_HANDLERS; i++){
	  this->types.at(i) = auxTypes[i];
	}
	
	
	/* Initializes the DLF logging */
	/*	castor::dlf::Messages messages[]={{1, "Starting StgRequestSvc Thread"},{2, "StagerRequestHelper"},{3, "StagerCnsHelper"},{4, "StagerReplyHelper"},{5, "StagerRequestHelper failed"},{6, "StagerCnsHelper failed"},{7, "StagerReplyHelper failed"},{8, "StagerHandler"}, {9, "StagerHandler successfully finished"},{10,"StagerHandler failed finished"},{11, "StgRequestSvc Thread successfully finished"},{12, "StgRequestSvc Thread failed finished"}};
		castor::dlf::dlf_init("StgRequestSvc", messages);*/
      }

     


      /*************************************************************/
      /* Method to get a subrequest to do using the StagerService */
      /***********************************************************/
      castor::IObject* StgRequestSvc::select() throw(castor::exception::Exception){
	castor::stager::IStagerSvc* stgService;

	castor::IService* svc =
	  castor::BaseObject::services()->
	  service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
	if (0 == svc) {
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StgRequestSvc) Impossible to get the stgService"<<std::endl;
	  throw ex;
	}
	stgService = dynamic_cast<castor::stager::IStagerSvc*>(svc);
	if (0 == stgService) {
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StgRequestSvc) Got a bad stgService"<<std::endl;
	  throw ex;
	}
	
	castor::stager::SubRequest* subrequestToProcess = stgService->subRequestToDo(this->types);
	
	return(subrequestToProcess);
      }


      /*********************************************************/
      /* Thread calling the specific request's handler        */
      /***************************************************** */
      void StgRequestSvc::process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception){
	try {

	  /******************************************************************/
	  /* to perform the common part for all the kind of subrequest type*/
	  /* helpers creation, check file permissions/existence...        */ 
	  /***************************************************************/
	  preprocess(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess));
	  
	  switch(typeRequest){
	    
	  case OBJ_StagePutDoneRequest:
	    {
	      StagerPutDoneHandler *stgPutDoneHandler = new StagerPutDoneHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutDoneHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StgRequestSvc) Impossible to execute the StagerPutDoneHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPutDoneHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgPutDoneHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutDone successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPutDoneHandler;
	      }catch(castor::exception::Exception ex){
		delete stgPutDoneHandler;
		throw ex;
	      }
	    }
	    break;
	      
	  case OBJ_StageRmRequest:
	    {
	      StagerRmHandler *stgRmHandler = new StagerRmHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRmHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StgRequestSvc) Impossible to execute the StagerRmHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerRmHandler starting")};/* 414 */
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);     
		stgRmHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerRmHandler successfully finished")};/* 417 */
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgRmHandler;
	      }catch(castor::exception::Exception ex){
		delete stgRmHandler;
		throw ex;
	      }
	    }
	    break;

	  case OBJ_SetFileGCWeight:
	    {
	      StagerSetGCHandler *stgSetGCHandler = new StagerSetGCHandler(stgRequestHelper, stgCnsHelper);
	      if(stgSetGCHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StgRequestSvc) Impossible to execute the StagerSetGCHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerSetGCHandler starting")};/* 436 */
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgSetGCHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerSetGCHandler successfully finished")};/* 439*/
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgSetGCHandler;
	      }catch(castor::exception::Exception ex){
		delete stgSetGCHandler;
		throw (ex);
	      }
		
	    }
	    break;
	      
	  }


	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper) delete stgCnsHelper;
	 
	  
	  /* we have to process the exception and reply to the client in case of error  */
	}catch(castor::exception::Exception ex){

	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  handleException(ex.code(), ex.getMessage().str());  
 	}catch (...){
	  
	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in StgRequestSvc")}; 
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  handleException(SEINTERNAL, "General Exception");	  
	}


      }/* end StgRequestSvc::process */


       /****************/
      /* destructor  */
      /**************/
      StgRequestSvc::~StgRequestSvc() throw(){

      }
      

   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
