/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/
#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"
#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"
#include "StagerDBService.hpp"

#include "StagerGetHandler.hpp"
#include "StagerRepackHandler.hpp"
#include "StagerPrepareToGetHandler.hpp"
#include "StagerPrepareToPutHandler.hpp"
#include "StagerPutHandler.hpp"
#include "StagerPutDoneHandler.hpp"
#include "StagerPrepareToUpdateHandler.hpp"
#include "StagerUpdateHandler.hpp"
#include "StagerRmHandler.hpp"
#include "StagerSetGCHandler.hpp"


#include "../../server/SelectProcessThread.hpp"
#include "../../BaseObject.hpp"

#include "../../../h/stager_constants.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/serrno.h"
#include "../../../h/dlf_api.h"
#include "../../dlf/Dlf.hpp"
#include "../../dlf/Param.hpp"
#include "../../../h/rm_api.h"
#include "../../../h/osdep.h"
#include "../../../h/Cnetdb.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/stager_uuid.h"
#include "../../../h/Cuuid.h"
#include "../../../h/u64subr.h"

#include "../../exception/Exception.hpp"
#include "../../exception/Internal.hpp"


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
	//maybe print a nice message or login in rm
	
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
      StagerDBService::~StagerDBService() throw(){
      }
      

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

	  /*!! at this point in the c version, we get all the subrequest properties: mode, protocol, xsize...*/
	  
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

	    /* depending on fileExist and type, check the file needed is to be created or throw exception */
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
	      StagerGetHandler stgGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgGetHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerGetHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","StagerGetHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgGetHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerGetHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	     
	    }
	    break;

	  
	  case OBJ_StagePrepareToGetRequest:
	    {
	      StagerPrepareToGetHandler stgPrepareToGetHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPrepareToGetHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToGetHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToGetHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgPrepareToGetHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPrepareToGetHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	     
	    }
	    break;


	  case OBJ_StageRepackRequest:
	    {
	      StagerRepackHandler stgRepackHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRepackHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerRepackHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerRepackHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgRepackHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerRepackHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      
	    }
	    break;

	  case OBJ_StagePrepareToPutRequest:
	    {
	      StagerPrepareToPutHandler stgPrepareToPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPrepareToPutHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToPutHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToPutHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgPrepareToPutHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	     
	    }
	    break;

	  case OBJ_StagePrepareToUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	      StagerPrepareToUpdateHandler stgPrepareToUpdateHandler(stgRequestHelper, stgCnsHelper, toRecreateCastorFile);
	      if(stgPrepareToUpdateHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPrepareToUpdateHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPrepareToUpdateHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	      stgPrepareToUpdateHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPrepareToUpdate successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      
	    }
	    break;
	    
	  case OBJ_StagePutRequest:
	    {
	      StagerPutHandler stgPutHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPutHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPutHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      stgPutHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      
	    }
	    break;

	  case OBJ_StageUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	      StagerUpdateHandler stgUpdateHandler(stgCnsHelper, stgCnsHelper, toRecreateCastorFile);
	      if(stgUpdateHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerUpdateHandler"<<std::endl;
	      throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerUpdateHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	 
	      stgUpdateHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerUpdateHandler successfully created")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      
	    }
	    break;

	  case OBJ_StagePutDoneRequest:
	    {
	      StagerPutDoneHandler stgPutDoneHandler(stgRequestHelper, stgCnsHelper);
	      if(stgPutDoneHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerPutDoneHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerPutDoneHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgPutDoneHandler.handle();
	      castor::dlf::param2[]={castor::dlf::Param("Standard Message","StagerPutDone successfully created")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      // stgPutDoneHandler.jobOriented;
	    }
	    break;
	      
	  case OBJ_StageRmRequest:
	    {
	      StagerRmHandler stgRmHandler(stgRequestHelper, stgCnsHelper);
	      if(stgRmHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerRmHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param[]={castor::dlf::Param("Standard Message","StagerRmHandler starting")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgRmHandler.handle();
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerRmHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	      
	    }
	    break;

	  case OBJ_SetFileGCWeight:
	    {
	      StagerSetGCHandler stgSetGCHandler(stgRequestHelper, stgCnsHelper);
	      if(stgSetGCHandler == NULL){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerDBService) Impossible to execute the StagerSetFileGCWeightHandler"<<std::endl;
		throw ex;
	      }
	      castor::dlf::Param param("Standard Message","StagerSetFileGCWeightHandler starting");
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param);
	      
	      
	      stgSetGCHandler.handle();/* 401 */
	      castor::dlf::Param param2[]={castor::dlf::Param("Standard Message","StagerSetFileGCWeightHandler successfully finished")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, param2);
	     
	    }
	    break;
	      
	  }

	  /* we have to process the exception and reply to the client in case of error  */
	}catch(castor::exception::Exception ex){

	  /* if the error happened before we had gotten the fileId needed for the dlf */
	  if((stgCnsHelper == NULL)||(stgCnsHelper->fileid == NULL)){
	    std::cerr<<ex.getMessage()<<std::endl;
	  }else{

	    castor::dlf::Param params[] = {castor::dlf::Param("Standard Message",sstrerror(ex.code())),castor::dlf::Param("Precise Message",ex.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
	  }
	  /* we have to set the subrequest status to SUBREQUEST_FAILED */
	  /* but not setGetNextSubrequestStatus!!  */
	  if((stgRequestHelper != NULL) && (stgRequestHelper->subrequest != NULL)){
	    stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED);
	  }
	  /* reply to the client in case of error*/
	  if(stgRequestHelper->iClient != NULL){
	    StagerReplyHelper *stgReplyHelper = new StagerReplyHelper;
	    stgReplyHelper->setAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid,ex.code(),ex.getMessage().str());//429
	    stgReplyHelper->endReplyToClient(stgRequestHelper);
	  }
	  

	}catch (...){
	  if((stgCnsHelper == NULL)||(stgCnsHelper->fileid == NULL)){
	    std::cerr<<"Caught general exception"<<std::endl;
	  }else{
	    castor::dlf::Param params[] = {castor::dlf::Param("Standard Message","Caught general exception in StagerDBService")};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);//439
	  }
	  /* we have to set the subrequest status to SUBREQUEST_FAILED */
	  /* but not setGetNextSubrequestStatus!!  */
	  if((stgRequestHelper != NULL) &&(stgRequestHelper->subrequest != NULL)){
	    stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED);
	  }
	  /* reply to the client in case of error*/
	  if(stgRequestHelper->iClient != NULL){
	    StagerReplyHelper *stgReplyHelper = new StagerReplyHelper;
	    
	    stgReplyHelper->setAndSendIoResponse(*stgRequestHelper,stgCnsHelper->fileid, SEINTERNAL, "General Exception");	      
	    stgReplyHelper->endReplyToClient(stgRequestHelper);//451
	  }
	 
	}

	
	  
	  
      }

      /***********************/
      /* stop thread method */ 
      /*********************/
      void StagerDBService::stop() throw(){
	delete stgRequestHelper;
	delete stgCnsHelper;
      }
     
      
     
   
      
     
   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
