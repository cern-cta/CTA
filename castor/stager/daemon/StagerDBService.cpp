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
#include "StagerSetFileGCWeightHandler.hpp"
#include "../../exception/Internal.hpp"
#include "../../../server/SelectProcessThread.hpp"
#include "../../../h/stager_constants.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/serrno.h"
#include "../../../h/dlf_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/osdep.h"
#include "../../../h/Cnetdb.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/stager_uuid.h"
#include "../../../h/Cuuid.h"
#include "../../../h/u64subr.h"



#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      /****************/
      /* constructor */
      /**************/
      StagerDBService::StagerDBService() throw()
      {
	//maybe print a nice message or login in rm
	this->types = {OBJ_StageGetRequest,OBJ_StagePrepareToGetRequest,OBJ_StageRepackRequest,OBJ_StagePutRequest,OBJ_StagePrepareToPutRequest,OBJ_StageUpdateRequest,OBJ_StagePrepareToUpdateRequest,OBJ_StageRmRequest,OBJ_SetFileGCWeight,OBJ_StagePutDoneRequest};
	


	castor::BaseObject::initLog("StagerDBService", castor::SVC_STDMSG);
	/* Initializes the DLF logging */
	castor::dlf::Messages messages[]={{1, "Starting StagerDBService Thread"},{2, "StagerRequestHelper"},{3, "StagerCnsHelper"},{4, "StagerReplyHelper"},{5, "StagerRequestHelper failed"},{6, "StagerCnsHelper failed"},{7, "StagerReplyHelper failed"},{8, "StagerHandler"}, {9, "StagerHandler successfully finished"},{10,"StagerHandler failed finished"},{11, "StagerDBService Thread successfully finished"},{12, "StagerDBService Thread failed finished"}};
	castor::dlf::dlf_init("StagerDBService", messages);
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
	  castor::dlf::Param initParam[]= {castor::dlf::Param("Standard Message","Creating the Helpers objects")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 1, initParam);
	  
	  stgCnsHelper = new StagerCnsHelper();
	  if(stgCnsHelper == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) Impossible to create the StagerCnsHelper"<<std::endl;
	    throw ex;
	  }
	  
	  stgRequestHelper = new StagerRequestHelper();
	  if(stgRequestHelper == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<< "(StagerDBService) Impossible to create the StagerRequestHelper"<<std::endl;
	    throw ex;
	  }
	  castor::stager::Param param[]= {castor::dlf::Param("Standard Message","Helpers successfully created")};
	  castor::dlf::dlf_writep(stgRequestHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 

	  /* get the subrequest: if there isn' t subrequest: we stop the program here */
	  stgRequestHelper->getSubrequest();
	  if((stgRequestHelper->subrequest) == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) There is not subrequest to process"<<std::endl;
	    throw ex;
	  }

	  /* settings BaseAddress */
	  stgRequestHelper->settingBaseAddress();
	 
	 
	  /* get the uuid subrequest string version and check if it is valid */
	  /* we can create one !*/
	  stgRequestHelper->setSubrequestUuid();
	  
	  /* obtain all the fileRequest associated to the subrequest using the dbService to get the foreign representation */
	  stgRequestHelper->getFileRequest();/* get from subrequest */
	  if((stgRequestHelper->filerequest) == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) Impossible to get the FileRequest object"<<std::endl;
	    throw ex;
	  }

	  /* get the associated client and set the iClientAsString variable */
	  stgRequestHelper->getIClient();
	  if((stgRequestHelper->iClient) == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) Impossible to get the IClient object"<<std::endl;
	    throw ex;
	  }


	  /* get the stgCnsHelper->fileid needed for the logging in dlf */
	  stgCnsHelper->getFileid();


	  /* get the uuid request string version and check if it is valid */
	  stgRequestHelper->setRequestUuid();

	  /*!! at this point in the c version, we get all the subrequest properties: mode, protocol, xsize...*/
	  
	  /* get the svcClass */
	  stgRequestHelper->getSvcClass();
	  if((stgRequestHelper->svcClass) == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) Impossible to get the SvcClass object (from FileRequest/defaultSvcClass)"<<std::endl;
	    throw ex;
	  }

	 

	  /* create and fill request->svcClass link on DB */
	  stgRequestHelper->linkRequestToSvcClassOnDB();
	  
	 

	 
	
	  
	  /* ask sebastien about this!:  */
	  /* #ifdef HAVE_REQUEST_UUID */
	  /* requestUuid = stgRequestHelper->getOrCreateRequest()->uuid();*/
	  /* #endif */ 
	  /* otherwise, requestUuid = (from string to Cuuid_t )request->reqId(); */
	  

	  std::string mask = stgRequestHelper->fileRequest->mask();
	  stgCnsHelper->cnsSettings(stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), mask.c_str());//to do!!!
	  stgRequestHelper->setUsernameAndGroupname();



	  /* get the castorFile of the subrequest and its iObject version */
	  stgRequestHelper->getCastorFile();
	  if((stgRequestHelper->castorFile) == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerDBService) Impossible to get the castorFile (from subrequest)"<<std::endl;
	    throw ex;
	  }
	  
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


                    
	  param[]= {castor::dlf::Param("Standard Message","Starting specific subrequestHandler")};
	  castor::dlf::dlf_writep(stgRequestHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 


	  /**************************************************/
	  /* "from now on we can log the invariants in DLF"*/
	  /*  fileidPointer = &cnsFileid  */
	  /*  fileid_ts = cnsFileid "and we remember these settings in the thread-specific version of the fileid" */ 
	 
	  switch(type){
	 
	  case OBJ_StageGetRequest:
	    StagerGetHandler *stgGetHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgGetHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerGetHandler"<<std::endl;
	      throw ex;
	    }
	    param[]= {castor::dlf::Param("Standard Message","StagerGetHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgGetHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerGetHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgGetHandler.jobOriented();
	    // int caseToSchedule=stagerService->isSubrequestToBeSchedule();
	    // stgGetHandler.switchScheduling(caseToSchedule)
	    // stgRequestHelper.buildRmJobHelperPart
	    // stgGetHandler.buildRmJobHandlerPart
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary 
	    // ->replyToClient just in case of error
	    break;

	  
	  case OBJ_StagePrepareToGetRequest:
	    StagerPrepareToGetHandler *stgPrepareToGetHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgPrepareToGetHandler == NULL){
	       castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerPrepareToGetHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerPrepareToGetHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgPrepareToGetHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerPrepareToGetHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgPrepareToGetHandler.jobOriented();
	    // int caseToSchedule=stagerService.isSubrequestToBeSchedule();
	    // (SPECIAL FOR PREPARETOGET) 
	    // switch(caseToSchedule)
	    //    case 0: stagerService->archiveSubrequest(subrequest->id())
	    //    default:stgPrepareToGetHandler.switchScheduling(caseToSchedule)
	    //            stgRequestHelper.buildRmJobHelperPart(this->rmjob)
	    //            stgPrepareToGetHandler.buildRmJobRequestPart()
	    //            rm_enterjob()
	    //            update subrequest status and getNextStatus if it is necessary 
	    // stgPrepareToGetHandler.replyToClient();
	    break;

	  case OBJ_StageRepackRequest:
	    StagerRepackHandler *stgRepackHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgRepackHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerRepackHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerRepackHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgRepackHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerRepackHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgRepackHandler.jobOriented();
	    // int caseToSchedule=stagerService.isSubrequestToBeSchedule();
	    // stgRepackHandler.switchScheduling(caseToSchedule);
	    // stgRequestHelper.buildRmJobHelperPart
	    // stgGetHandler.buildRmJobHandlerPart
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary 
	    // stgRepackHandler.replyToClient();
	    break;

	  case OBJ_StagePrepareToPutRequest:
	    StagerPrepareToPutHandler *stgPrepareToPutHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgPrepareToPutHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerPrepareToPutHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerPrepareToPutHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgPrepareToPutHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgPrepareToPutHandler.jobOriented;
	    // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	    // stgPrepareToPutHandler.buildRmJobHandlerPart()
	    // stgRequestHelper.buildRmJobHelperPart()
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary
	    // stgPrepareToPutHandler.replyToClient()	    
	    break;

	  case OBJ_StagePrepareToUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	      StagerPrepareToUpdateHandler *stgPrepareToUpdateHandler(stgRequestHelper, stgCnsHelper, message, toRecreateCastorFile);
	      if(stgPrepareToUpdateHandler == NULL){
		castor::exception::Internal ex;
		ex.getMessage()<<"(StagerDBService) Impossible to get the StagerPrepareToUpdateHandler"<<std::endl;
		throw ex;
	      }
	      castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerPrepareToUpdateHandler starting")};
	      castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	      stgPrepareToUpdateHandler->handle();
	      param[]= {castor::dlf::Param("Standard Message","StagerPrepareToUpdate successfully finished")};
	      castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	      // stgPutHandler.jobOriented;
	      // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	      // stgPutHandler.buildRmJobHandlerPart()
	      // stgRequestHelper.buildRmJobHelperPart()
	      // rm_enterjob()
	      // update subrequest status and getNextStatus if it is necessary
	      // stgPrepareToUpdateHandler.replyToClient();
	      break;
	    }
	  case OBJ_StagePutRequest:
	    StagerPutHandler *stgPutHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgPutHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerPutHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerPutHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	    stgPutHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerPutHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgPutHandler.jobOriented;
	    // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	    // stgPutHandler.buildRmJobHandlerPart()
	    // stgRequestHelper.buildRmJobHelperPart()
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary
	    // ->reply to client just in case of error
	    break;

	  case OBJ_StageUpdateRequest:
	    {
	      bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	      StagerUpdateHandler *stgUpdateHandler(stgCnsHelper, stgCnsHelper, message, toRecreateCastorFile);
	      if(stgUpdateHandler == NULL){
		castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerUpdateHandler"<<std::endl;
	      throw ex;
	      }
	      castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerUpdateHandler starting")};
	      castor::dlf::dlf_writep(stgRequestHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	      stgUpdateHandler->handle();
	      param[]= {castor::dlf::Param("Standard Message","StagerUpdateHandler successfully created")};
	      castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	      // stgUpdateHandler.jobOriented();
	      // if(fileExist && ((SubrequestFlags & O_TRUNC)==0))
	      //	 int caseToSchedule=stagerService.isSubrequestToBeSchedule();
	      //	 stgUpdateHandler.switchScheduling(caseToSchedule);
	      //   stgRequestHelper.buildRmJobHelperPart()
	      //   stgUpdateHandler.buildRmJobHandlerPart()
	      // else
	      //   stgRequestHelper->stagerService->recreateCastorFile()
	      //   stgUpdateHandler.buildRmJobHelperPar()
	      // rm_enterjob()
	      // update subrequest status and getNextStatus if it is necessary
	      //->reply_to_client just in case of error
	    }
	    break;

	  case OBJ_StagePutDoneRequest:
	    StagerPutDoneHandler *stgPutDoneHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgPutDoneHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerPutDoneHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerPutDoneHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgPutDoneHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerPutDone successfully created")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgPutDoneHandler.jobOriented;
	    break;

	  case OBJ_StageRmRequest:
	    StagerRmHandler *stgRmHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgRmHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerRmHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerRmHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgRmHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerRmHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgRequestHelper->stagerService->stageRm()				  
	    // update subrequest status and getNextStatus if it is needed!
	    // stgRmHandler.replyToClient;
	    break;

	  case OBJ_SetFileGCWeight:
	    StagerSetFileGCWeightHandler *stgSeFiletGCWeightHandler(stgRequestHelper, stgCnsHelper, message);
	    if(stgSetFileGCWeightHandler == NULL){
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerDBService) Impossible to get the StagerSetFileGCWeightHandler"<<std::endl;
	      throw ex;
	    }
	    castor::stager::Param param[]= {castor::dlf::Param("Standard Message","StagerSetFileGCWeightHandler starting")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	  
	 
	    stgSetFileGCWeightHandler->handle();
	    param[]= {castor::dlf::Param("Standard Message","StagerSetFileGCWeightHandler successfully finished")};
	    castor::dlf::dlf_writep(stgCnsHelper->fileid, DLF_LVL_USAGE, 1, 1, param);
	    // stgRequestHelper->stagerService->stageSetFileGCWeight()
 	    // update subrequest status and getNextStatus if it is necessary
	    // stgSetGCWeightHandler.replyToClient;
	    break;
	      
	  }

	  /* the run method exits without exception */
	  
	    delete stgRequestHelper;
	    delete stgCnsHelper;
	 
	  
	}catch(castor::exception::Internal ex){
	 
	  castor::dlf::Param params[]={castor::dlf::Param("Standard Message",sstrerror(e.code())),castor::dlf::Param("Precise Message",e.getMessage().str())};
	  /* since the exception can be throwed before we have stgCnsHelper->fileid, we must have an auxiliar fileid for dlf */
	  struct Cns_fileid *auxFileid = NULL;
	  if((stgCnsHelper->fileid) == NULL){
	    auxFileid = nullCuuid;
	  }else{
	    auxFileid = stgCnsHelper->fileid;
	  }
	  
	  castor::dlf::dlf_writep(auxFileid, DLF_LVL_ERROR, 2, 1, params);
	  delete stgRequestHelper;
	  delete stgCnsHelper;
	  return -1;

	}catch (...){
	  std::cerr<<"caught general exeption"<<std::endl;

	  castor::dlf::Param params2[]= {castor::dlf::Param("Standard Message","Caught general exception in StagerDBService")};
	  /* since the exception can be throwed before we have stgCnsHelper->fileid, we must have an auxiliar fileid for dlf */
	  struct Cns_fileid *auxFileid = NULL;
	  if((stgCnsHelper->fileid) == NULL){
	    auxFileid = nullCuuid;
	  }else{
	    auxFileid = stgCnsFileid->fileid;
	  }

	  castor::dlf::dlf_writep(auxFileid, DLF_LVL_ERROR, 2, 1, params2);
	  delete stgRequestHelper;
	  delete stgCnsHelper;
	  return -1;
	}

	return(0);
	  
	  
      }

      /***********************/
      /* stop thread method */ 
      /*********************/
      void StagerDBService::stop() throw(){

      }
     
      
     
   
      
     
   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
