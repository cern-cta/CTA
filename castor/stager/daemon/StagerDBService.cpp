/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/
#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
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
#include "castor/stager/dbService/StagerSetFileGCWeightHandler.hpp"


#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      /* constructor */
      StagerDBService::StagerDBService()
      {
	//maybe print a nice message or login in rm
	types = {OBJ_StageGetRequest,OBJ_StagePrepareToGetRequest,OBJ_StageRepackRequest,OBJ_StagePutRequest,OBJ_StagePrepareToPutRequest,OBJ_StageUpdateRequest,OBJ_StagePrepareToUpdateRequest,OBJ_StageRmRequest,OBJ_SetFileGCWeight,OBJ_StagePutDoneRequest};
      }


      
      /* build the collection to pass to the selected request */
      /* stgRequestHelper->getOrCreate____(): first call, we also create it */
      int main(int argc, char* argv[]){
	try {

	  /* log in rm: extern C function: rm_setlog */

	  /* we create services neededs at the begining */
	  this->stgRequestHelper = new castor::stager::StagerRequestHelper*;
	  this->stgCnsHelper = new castor::stager::StagerCnsHelper*;

	  
	 
	  /* get the subrequest: if there isn' t subrequest: we stop the program here */
	  this->stgRequestHelper->getSubrequest();
	 

	  /* settings BaseAddress */
	  this->stgRequestHelper->settingBaseAddress();
	  
	 
	  /* get the uuid subrequest string version and check if it is valid */
	  /* we can create one !*/
	  this->stgRequestHelper->setSubrequestUuid();
	  
	  /* obtain all the fileRequest associated to the subrequest using the dbService to get the foreign representation */
	  this->stgRequestHelper->getFileRequest();/* get from subrequest */
	 
	  /* get the associated client and set the iClientAsString variable */
	  this->stgRequestHelper->getIClient();
	  

	  /* get the uuid request string version and check if it is valid */
	  this->stgRequestHelper->setRequestUuid();

	  /*!! at this point in the c version, we get all the subrequest properties: mode, protocol, xsize...*/
	  
	  /* get the svcClass */
	  this->stgRequestHelper->getSvcClass();

	  /*!! at this point in the c version, we get the svcClass id */

	  /* create and fill request->svcClass link on DB*/
	  this->stgRequestHelper->linkRequestToSvcClassOnDB();
	  
	 

	 
	
	  
	  /* ask sebastien about this!:  */
	  /* #ifdef HAVE_REQUEST_UUID */
	  /* requestUuid = stgRequestHelper->getOrCreateRequest()->uuid();*/
	  /* #endif */ 
	  /* otherwise, requestUuid = (from string to Cuuid_t )request->reqId(); */
	  

	  std::string mask = stgRequestHelper->fileRequest->mask();
	  stgCnsHelper->cnsSettings(stgRequestHelper->fileRequest->euid(), stgRequestHelper->fileRequest->egid(), mask.c_str());//to do!!!
	  this->stgRequestHelper->setUsernameAndGroupname();



	  /* get the castorFile of the subrequest and its iObject version */
	  this->stgRequestHelper->getCastorFile();
	  
	  
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
	  this->stgRequestHelper->checkFilePermission();






	  /**************************************************/
	  /* "from now on we can log the invariants in DLF"*/
	  /*  fileidPointer = &cnsFileid  */
	  /*  fileid_ts = cnsFileid "and we remember these settings in the thread-specific version of the fileid" */ 
	 
	  switch(type){
	 
	  case OBJ_StageGetRequest:
	    StagerGetHandler stgGetHandler(stgRequestHelper, stgCnsHelper, message);
	    stgGetHandler.handle();
	    // stgGetHandler.jobOriented();
	    // int caseToSchedule=stagerService.isSubrequestToBeSchedule();
	    // stgGetHandler.switchScheduling(caseToSchedule)
	    // stgRequestHelper.buildRmJobHelperPart
	    // stgGetHandler.buildRmJobHandlerPart
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary 
	    // ->replyToClient just in case of error
	    break;
	  
	  case OBJ_StagePrepareToGetRequest:
	    StagerPrepareToGetHandler stgPrepareToGetHandler(stgRequestHelper, stgCnsHelper, message);
	    stgPrepareToGetHandler.handle();
	    // stgRepackHandler.jobOriented();
	    // int caseToSchedule=stagerService.isSubrequestToBeSchedule();
	    // stgRepackHandler.switchScheduling(caseToSchedule);
	    // stgRequestHelper.buildRmJobHelperPart
	    // stgGetHandler.buildRmJobHandlerPart
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary 
	    // stgPrepareToGetHandler.replyToClient();
	    break;

	  case OBJ_StageRepackRequest:
	    StagerRepackHandler stgRepackHandler(stgRequestHelper, stgCnsHelper, message);
	    stgRepackHandler.handle();
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
	    StagerPrepareToPutHandler stgPrepareToPutHandler(stgRequestHelper, stgCnsHelper, message);
	    stgPrepareToPutHandler.handle();
	    // stgPutHandler.jobOriented;
	    // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	    // stgPutHandler.buildRmJobHandlerPart()
	    // stgRequestHelper.buildRmJobHelperPart()
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary
	    // stgPrepareToPutHandler.replyToClient()	    
	    break;

	  case OBJ_StagePrepareToUpdateRequest:
	    bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	    StagerPrepareToUpdateHandler stgPrepareToUpdateHandler(stgRequestHelper, stgCnsHelper, message, toRecreateCastorFile);
	    stgPrepareToUpdateHandler.handle();
	    // stgPutHandler.jobOriented;
	    // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	    // stgPutHandler.buildRmJobHandlerPart()
	    // stgRequestHelper.buildRmJobHelperPart()
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary
	    // stgPrepareToUpdateHandler.replyToClient();
	    break;

	  case OBJ_StagePutRequest:
	    StagerPutHandler stgPutHandler(stgRequestHelper, stgCnsHelper, message);
	    stgPutHandler.handle();
	    // stgPutHandler.jobOriented;
	    // diskCopyForRecall = stgRequestHelper->stagerService->recreateCastorFile
	    // stgPutHandler.buildRmJobHandlerPart()
	    // stgRequestHelper.buildRmJobHelperPart()
	    // rm_enterjob()
	    // update subrequest status and getNextStatus if it is necessary
	    // ->reply to client just in case of error
	    break;

	  case OBJ_StageUpdateRequest:
	    bool toRecreateCastorFile = !(fileExist && (((stgRequestHelper->subrequest->flags()) & O_TRUNC) == 0));
	    StagerUpdateHandler stgUpdateHandler(stgRequestHelper, stgCnsHelper, message, toRecreateCastorFile);
	    stgUpdateHandler.handle();
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
	    
	    break;

	  case OBJ_StagePutDoneRequest:
	    StagerPutDoneHandler stgPutDoneHandler(stgRequestHelper, stgCnsHelper, message);
	    stgPutDoneHandler.handle();
	    // stgPutDoneHandler.jobOriented;
	    break;

	  case OBJ_StageRmRequest:
	    StagerRmHandler stgRmHandler(stgRequestHelper, stgCnsHelper, message);
	    stgRmHandler.handle();
	    // stgRequestHelper->stagerService->stageRm()				  
	    // update subrequest status and getNextStatus if it is needed!
	    // stgRmHandler.replyToClient;
	    break;

	  case OBJ_SetFileGCWeight:
	    StagerSetFileGCWeightHandler stgSeFiletGCWeightHandler(stgRequestHelper, stgCnsHelper, message);
	    stgSetFileGCWeightHandler.handle();
	    // stgRequestHelper->stagerService->stageSetFileGCWeight()
 	    // update subrequest status and getNextStatus if it is necessary
	    // stgSetGCWeightHandler.replyToClient;
	    break;
	  
    
	  }
	}catch(){
	} 

	return(0);
	  
	  
      }

      

     
      
     
   
      
      void StagerDBService::~StagerDBService()
      {
	delete stgRequestHelper;
	delete stgCnsHelper;
      }
   
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
      
    





    







   
