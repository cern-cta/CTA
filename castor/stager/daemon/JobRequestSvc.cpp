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
#include "castor/stager/SubRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#define JOBREQ_HANDLERS 3



namespace castor{
  namespace stager{
    namespace dbService{

     
      /****************/
      /* constructor */
      /**************/
      JobRequestSvc::JobRequestSvc() throw()
      {
	
	this->types.resize(JOBREQ_HANDLERS);
	ObjectsIds auxTypes[] = {OBJ_StageGetRequest,
				 OBJ_StagePutRequest,
				 OBJ_StageUpdateRequest};
	
	for(int i= 0; i< JOBREQ_HANDLERS; i++){
	  this->types.at(i) = auxTypes[i];
	}
	
	
	/* Initializes the DLF logging */
	/*	castor::dlf::Messages messages[]={{1, "Starting JobRequestSvc Thread"},{2, "StagerRequestHelper"},{3, "StagerCnsHelper"},{4, "StagerReplyHelper"},{5, "StagerRequestHelper failed"},{6, "StagerCnsHelper failed"},{7, "StagerReplyHelper failed"},{8, "StagerHandler"}, {9, "StagerHandler successfully finished"},{10,"StagerHandler failed finished"},{11, "JobRequestSvc Thread successfully finished"},{12, "JobRequestSvc Thread failed finished"}};
		castor::dlf::dlf_init("JobRequestSvc", messages);*/
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
	
	castor::stager::SubRequest* subrequestToProcess = stgService->subRequestToDo(this->types);
	
	return(subrequestToProcess);
      }


      /*********************************************************/
      /* Thread calling the specific request's handler        */
      /***************************************************** */
      void JobRequestSvc::process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception){
	try {

	  /******************************************************************/
	  /* to perform the common part for all the kind of subrequest type*/
	  /* helpers creation, check file permissions/existence...        */ 
	  /***************************************************************/
	  preprocess(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess));
	 
	  switch(typeRequest){
	 
	  case OBJ_StageGetRequest:
	    {
	      StagerGetHandler *stgGetHandler = new StagerGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgGetHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerGetHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","StagerGetHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgGetHandler->handle();/* we notify internally the jobManager */
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerGetHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);		
		delete stgGetHandler;
	      }catch(castor::exception::Exception ex){
		delete stgGetHandler;
		throw ex;
	      }
	     
	    }
	    break;
	    
	  case OBJ_StagePutRequest:
	    {
	      StagerPutHandler *stgPutHandler = new StagerPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerPutHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPutHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgPutHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPutHandler;
	      }catch(castor::exception::Exception ex){
		delete stgPutHandler;
		throw ex;
	      }
	      
	    }
	    break;

	  case OBJ_StageUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	      StagerUpdateHandler *stgUpdateHandler = new StagerUpdateHandler(stgRequestHelper, stgCnsHelper, toRecreateCastorFile);
	      if(stgUpdateHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(JobRequestSvc) Impossible to execute the StagerUpdateHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerUpdateHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgUpdateHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerUpdateHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgUpdateHandler;
	      }catch(castor::exception::Exception ex){
		delete stgUpdateHandler;
		throw ex;
	      }
	    }
	    break;


	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper) delete stgCnsHelper;
	 
	  }
	}catch(castor::exception::Exception ex){ /* process the exception an replyToClient */

	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  handleException(ex.code(), ex.getMessage.str());
 	}catch (...){
	  
	  castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in JobRequestSvc")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);	  
	  handleException(SEINTERNAL, "General Exception");
	}


      }/* end JobRequestSvc::process */



   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
