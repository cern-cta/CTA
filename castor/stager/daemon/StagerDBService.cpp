/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerDBService.hpp"

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

#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "serrno.h"
#include "dlf_api.h"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "rm_api.h"
#include "osdep.h"
#include "Cnetdb.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "stager_uuid.h"
#include "Cuuid.h"
#include "u64subr.h"


#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"


#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include <iostream>
#include <string>

#define STAGER_OPTIONS 10

namespace castor{
  namespace stager{
    namespace dbService{

      /****************/
      /* constructor */
      /**************/
      StagerDBService::StagerDBService() throw()
      {
	
	this->types.resize(STAGER_OPTIONS);
	ObjectsIds auxTypes[] = {OBJ_StageGetRequest,
				 OBJ_StagePrepareToGetRequest,
				 OBJ_StageRepackRequest,
				 OBJ_StagePutRequest,
				 OBJ_StagePrepareToPutRequest,
				 OBJ_StageUpdateRequest,
				 OBJ_StagePrepareToUpdateRequest,
				 OBJ_StageRmRequest,
				 OBJ_SetFileGCWeight,
				 OBJ_StagePutDoneRequest};
	
	for(int i= 0; i< STAGER_OPTIONS; i++){
	  this->types.at(i) = auxTypes[i];
	}
	
	
	castor::BaseObject::initLog("StagerDBService", castor::SVC_STDMSG);
	/* Initializes the DLF logging */
	/*	castor::dlf::Messages messages[]={{1, "Starting StagerDBService Thread"},{2, "StagerRequestHelper"},{3, "StagerCnsHelper"},{4, "StagerReplyHelper"},{5, "StagerRequestHelper failed"},{6, "StagerCnsHelper failed"},{7, "StagerReplyHelper failed"},{8, "StagerHandler"}, {9, "StagerHandler successfully finished"},{10,"StagerHandler failed finished"},{11, "StagerDBService Thread successfully finished"},{12, "StagerDBService Thread failed finished"}};
		castor::dlf::dlf_init("StagerDBService", messages);*/
      }

      /****************/
      /* destructor  */
      /**************/
      StagerDBService::~StagerDBService() throw(){};
      

      /*********************************************************/
      /* Thread calling the specific request's handler        */
      /***************************************************** */
      void StagerDBService::run(void* param) throw(){
	try {


	  /*******************************************/
	  /* We create the helpers at the beginning */
	  /*****************************************/
	   
	  stgCnsHelper = new StagerCnsHelper();
	  if(stgCnsHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerDBService) Impossible to create the StagerCnsHelper"<<std::endl;
	    throw ex;
	  }
	  
	  stgRequestHelper = new StagerRequestHelper();
	  if(stgRequestHelper == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<< "(StagerDBService) Impossible to create the StagerRequestHelper"<<std::endl;
	    throw ex;
	  }


	  /* we get the Cuuid_t fileid (needed to logging in dl)  */
	  castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","Helpers successfully created")};
	  castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 1, param);/*   */
	  
	 

	  /* get the subrequest: if there isn' t subrequest: we stop the program here */
	  stgRequestHelper->getSubrequest();
	 

	  /* settings BaseAddress */
	  stgRequestHelper->settingBaseAddress();
	 	 
	  /* get the uuid subrequest string version and check if it is valid */
	  /* we can create one !*/
	  stgRequestHelper->setSubrequestUuid();
	  
	  /* obtain all the fileRequest associated to the subrequest using the dbService to get the foreign representation */
	  stgRequestHelper->getFileRequest();/* get from subrequest */
	  int type = stgRequestHelper->fileRequest->type();

	  /* get the associated client and set the iClientAsString variable */
	  stgRequestHelper->getIClient();
	 

	  /* get the stgCnsHelper->fileid needed for the logging in dlf */
	  stgCnsHelper->getFileid();


	  /* get the uuid request string version and check if it is valid */
	  stgRequestHelper->setRequestUuid();

	 
	  /* get the svcClass */
	  stgRequestHelper->getSvcClass();
	  
	 

	  /* create and fill request->svcClass link on DB */
	  stgRequestHelper->linkRequestToSvcClassOnDB();
	  	
	  
	  /* ask sebastien about this!:  */
	  /* #ifdef HAVE_REQUEST_UUID */
	  /* requestUuid = stgRequestHelper->getOrCreateRequest()->uuid();*/
	  /* #endif */ 
	  /* otherwise, requestUuid = (from string to Cuuid_t )request->reqId(); */
	  

	  mode_t mask = (mode_t) stgRequestHelper->fileRequest->mask();
	  stgCnsHelper->cnsSettings(stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), mask);//to do!!!
	  stgRequestHelper->setUsernameAndGroupname();

	  /* get the castorFile of the subrequest */
	  stgRequestHelper->getCastorFile();
	 
	  
	  /* for the required file: check existence (and create if necessary), and permission */
	  std::string filename = stgRequestHelper->subrequest->fileName();
	  bool fileExist = stgCnsHelper->createCnsFileIdAndStat_setFileExist(filename.c_str());
	  if(!fileExist){

	    /* depending on fileExist and type, check if the file needed is to be created or throw exception */
	    if(stgRequestHelper->isFileToCreateOrException(fileExist)){

	      mode_t mode = (mode_t) stgRequestHelper->subrequest->modeBits();
	      /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
	      stgCnsHelper->createFileAndUpdateCns(filename.c_str(), mode);
	    }
	    
	  }

	  /* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
	  stgRequestHelper->checkFilePermission();


                    
	  castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","Starting specific subrequestHandler")};
	  castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 1, parameter);
	  

	  /**************************************************/
	  /* "from now on we can log the invariants in DLF"*/
	  /*  fileidPointer = &cnsFileid  */
	  /*  fileid_ts = cnsFileid "and we remember these settings in the thread-specific version of the fileid" */ 
	 
	  switch(type){
	 
	  case OBJ_StageGetRequest:
	    {
	      StagerGetHandler *stgGetHandler = new StagerGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgGetHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerGetHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","StagerGetHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgGetHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerGetHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgGetHandler;
	      }catch(castor::exception::Exception ex){
		delete stgGetHandler;
		throw ex;
	      }
	     
	    }
	    break;

	  
	  case OBJ_StagePrepareToGetRequest:
	    {
	      StagerPrepareToGetHandler *stgPrepareToGetHandler = new StagerPrepareToGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPrepareToGetHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToGetHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToGetHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgPrepareToGetHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPrepareToGetHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPrepareToGetHandler;
	      }catch(castor::exception::Exception ex){
		delete stgPrepareToGetHandler;
		throw ex;
	      }
	     
	    }
	    break;


	  case OBJ_StageRepackRequest:
	    {
	      StagerRepackHandler *stgRepackHandler = new StagerRepackHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRepackHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerRepackHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerRepackHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgRepackHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerRepackHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgRepackHandler;
	      }catch(castor::exception::Exception ex){
		delete stgRepackHandler;
		throw ex;
	      }
	      
 
	    }
	    break;

	  case OBJ_StagePrepareToPutRequest:
	    {
	      StagerPrepareToPutHandler *stgPrepareToPutHandler = new StagerPrepareToPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPrepareToPutHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToPutHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToPutHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);      
		stgPrepareToPutHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPrepareToPutHandler;
	      }catch(castor::exception::Exception ex){
		delete stgPrepareToPutHandler;
		throw ex;
	      }
	     
	    }
	    break;

	  case OBJ_StagePrepareToUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));

	      StagerPrepareToUpdateHandler *stgPrepareToUpdateHandler = new StagerPrepareToUpdateHandler(stgRequestHelper, stgCnsHelper, toRecreateCastorFile);
	      if(stgPrepareToUpdateHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToUpdateHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToUpdateHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param); 
		stgPrepareToUpdateHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPrepareToUpdate successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPrepareToUpdateHandler;
	      }catch(castor::exception::Exception ex){
		delete stgPrepareToUpdateHandler;
		throw ex;
	      }
	      
	    }
	    break;
	    
	  case OBJ_StagePutRequest:
	    {
	      StagerPutHandler *stgPutHandler = new StagerPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPutHandler"<<std::endl;
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
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerUpdateHandler"<<std::endl;
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

	  case OBJ_StagePutDoneRequest:
	    {
	      StagerPutDoneHandler *stgPutDoneHandler = new StagerPutDoneHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutDoneHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPutDoneHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPutDoneHandler starting")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgPutDoneHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutDone successfully finished")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
		delete stgPutDoneHandler;
	      // stgPutDoneHandler.jobOriented;
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
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerRmHandler"<<std::endl;
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
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerSetFileGCWeightHandler"<<std::endl;
		throw ex;
	      }
	      try{
		castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerSetFileGCWeightHandler starting")};/* 436 */
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
		stgSetGCHandler->handle();/**/
		castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerSetFileGCWeightHandler successfully finished")};/* 439*/
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

	  /* if the error happened before we had gotten the fileId needed for the dlf */
	  if((stgCnsHelper == NULL) || (stgCnsHelper->fileid == NULL)){
	    std::cerr<<ex.getMessage()<<std::endl;
	  }else{
	    castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};/* 459 */
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  }

	  /* we have to set the subrequest status to SUBREQUEST_FAILED_FINISHED */
	  /* but not setGetNextSubrequestStatus!!  */
	  if(stgRequestHelper!=NULL){
	    /* update subrequest status */
	    if(stgRequestHelper->subrequest != NULL){
	      stgRequestHelper->updateSubrequestStatus(SUBREQUEST_FAILED_FINISHED);
	    }
	    /* reply to the client in case of error*/
	    if(stgRequestHelper->iClient != NULL){
	      StagerReplyHelper *stgReplyHelper = new StagerReplyHelper;
	      if(stgReplyHelper == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService catch exception) Impossible to get the stgReplyHelper"<<std::endl;
		throw ex;
	      }
	      stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid,ex.code(),ex.getMessage().str());
	      stgReplyHelper->endReplyToClient(stgRequestHelper);
	      delete stgReplyHelper->ioResponse;
	      delete stgReplyHelper;
	    }else{
	      if((stgRequestHelper->dbService)&&(stgRequestHelper->subrequest)){
		stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      }
	    }

	    if(stgRequestHelper != NULL){
	      if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	      delete stgRequestHelper;
	    }
	    
	    if(stgCnsHelper != NULL){
	      delete stgCnsHelper;/* 567 */
	    }
	  }  
 
	}catch (...){
	  if((stgCnsHelper == NULL)||(stgCnsHelper->fileid == NULL)){
	    std::cerr<<"Caught general exception"<<std::endl;
	  }else{
	    castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in StagerDBService")}; /* 485 */
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  }
	  /* we have to set the subrequest status to SUBREQUEST_FAILED */
	  /* but not setGetNextSubrequestStatus!!  */
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->subrequest != NULL){
	      stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED_FINISHED);
	    }
	    /* reply to the client in case of error*/
	    if(stgRequestHelper->iClient != NULL){
	      StagerReplyHelper *stgReplyHelper = new StagerReplyHelper;	  
	      if(stgReplyHelper != NULL){
		stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->fileid, SEINTERNAL, "General Exception");	      
		stgReplyHelper->endReplyToClient(stgRequestHelper);
		delete stgReplyHelper->ioResponse;
		delete stgReplyHelper;
	      }
	    }else{
	      if((stgRequestHelper->dbService)&&(stgRequestHelper->subrequest)){
		stgRequestHelper->dbService->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
	      }
	    }
	  }
	 
	  if(stgRequestHelper != NULL){
	    if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
	    delete stgRequestHelper;
	  }
	  
	  if(stgCnsHelper != NULL){
	    delete stgCnsHelper;
	  }
	}


      }/* end StagerDBService::run */

      
     
   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
