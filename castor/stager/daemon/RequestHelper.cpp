/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "StagerRequestHelper.hpp"

#include "../../IStagerSvc.hpp"
#include "../../Services.hpp"
#include "../../BaseAddress.hpp"
#include "../../Subrequest.hpp"
#include "../../FileRequest.hpp"
#include "../../IClient.hpp"
#include "../../SvcClass.hpp"
#include "../../CastorFile.hpp"
#include "../../FileClass.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/u64subr.h"
#include "../../../IClientFactory.h"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      /* contructor */
      StagerRequestHelper::StagerRequestHelper() throw(castor::exception::Internal){
	
	try{
	  this->stagerService =  new castor::stager::IStagerSvc*; 
	  if(this->stagerService == NULL){
	    
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the stagerService"<<std::endl;
	    throw ex;
	  }
	  
	  this->dbService = new castor::Services*;
	  if(this->dbService == NULL){
	    
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the dbService"<<std::endl;
	    throw ex;
	  }

	  this->baseAddr =  new castor::BaseAddress*; /* the settings ll be done afterwards */
	  if(this->baseAddr == NULL){
	    
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the baseAddress"<<std::endl;
	    throw ex;
	  }
	  
	  this->partitionMask[0] = '\0';  
	  types = {OBJ_StageGetRequest,OBJ_StagePrepareToGetRequest,OBJ_StageRepackRequest,OBJ_StagePutRequest,OBJ_StagePrepareToPutRequest,OBJ_StageUpdateRequest,OBJ_StagePrepareToUpdateRequest,OBJ_StageRmRequest,OBJ_SetFileGCWeight,OBJ_StagePutDoneRequest};

	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);

	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	} 
      }



      /* destructor */
      StagerRequestHelper::~StagerRequestHelper()
      {
	delete stagerService;
	delete dbService;
	delete baseAddr;
      }





      /**********************/
      /* baseAddr settings */ 
      /********************/
      void StagerRequestHelper::settingBaseAddress()
      {
	  baseAddr->setCnvSvcName("DbCnvSvc");
	  baseAddr->setCnvSvcType("SVC_DB_CNV");
      }
     


      /**************************************************************************************/
      /* get/create subrequest->  fileRequest, and many subrequest's attributes            */
      /************************************************************************************/
      
      /* get subrequest using stagerService (and also types) */
      void StagerRequestHelper::getSubrequest() throw(castor::exception::Internal)
      {
	try{
	  this->subrequest=stagerService->subRequestToDo(&types);
	  if(this->subrequest == NULL){

	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getSubrequest) There is not subrequest to process"<<std::endl;
	    throw ex;
	  }
	}catch(castor::exception::Internal ex){

	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_USAGE, 2, 1, param);

	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	
      }
      

      /* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
      /* using dbService, and get the fileRequest */ 
      void StagerRequestHelper::getFileRequest() throw(castor::exception::Internal)
      {
	try{
	  dbService->fillObj(baseAddr, subrequest, OBJ_FileRequest, 0);
	  this->fileRequest=subrequest->request();
	  if(this->fileRequest == NULL){
	    
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getFileRequest) Impossible to get the fileRequest"<<std::endl;
	    throw ex;
	  }
	
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
      }
    
    


      /***********************************************************************************/
      /* get the link (fillObject~SELECT) between fileRequest and its associated client */
      /* using dbService, and get the client                                           */
      /********************************************************************************/
      void StagerRequestHelper::getIClient() throw(castor::exception::Internal)
      {
	try{
	  dbService->fillObj(baseAddr, fileRequest,OBJ_IClient, 0);
	  this->iClient=fileRequest->client();
	  if(this->iClient == NULL){
	    
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getIClient) Impossible to get the iClient"<<std::endl;
	    throw ex;
	  }
	  this->iClientAsString=IClientFactory.client2string(iClient);/* IClientFactory singleton */
	  
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}

      }
      
	


      
      
      /****************************************************************************************/
      /* get svClass by selecting with stagerService                                         */
      /* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
      /*************************************************************************************/
      void StagerRequestHelper::getSvcClass() throw(castor::exception::Internal)
      {
	try{
	  std::string className=fileRequest->svcClassName(); 
	  
	  if((className==NULL)||(className.empty())){  /* we set the default className */
	    className="default";
	    fileRequest->setSvcClassName(className);
	    
	    castor::dlf::Param param[] = {castor::dlf::Param("Message", "(StagerRequestHelper getSvcClass) No className on fileRequest. Using the default_className")};
	    castor::dlf::dlf_writep(, DLF_LVL_USAGE, 2, 1,param);
	    
	    className=fileRequest->svcClassName(); /* we retrieve it to know if it has been correctly updated */
	    if((className == NULL)||(className.empty())){
	       
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerRequestHelper getSvcClass) Impossible to set properly the svcClassName on fileRequest"<<std::endl;
	      throw ex;
	    }
	  }

	  svcClass=stagerService->selectSvcClass(className);//check if it is NULL
	  if(this->svcClass == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getSvcClass) Impossible to get the svcClass"<<std::endl;
	    throw ex; 
	  }


	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	 
      }
      
      
      
      
      /*******************************************************************************/
      /* update request in DB, create and fill request->svcClass link on DB         */ 
      /*****************************************************************************/
      void StagerRequestHelper::linkRequestToSvcClassOnDB() throw(castor::exception::Internal)
      {
	try{
	  /* update request on DB */
	  dbService->updateRep(baseAddr, fileRequest, STAGER_AUTOCOMMIT_TRUE);//EXCEPTION!

	}catch(castor::exception::Exception ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper linkRequestToSvcClassOnDB) dbService->updateRep throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
 
	fileRequest->setSvcClass(svcClass);
	
	try{
	  /* fill the svcClass object using the request as a key  */
	  dbService->fillRep(baseAddr, fileRequest,OBJ_SvcClass,STAGER_AUTOCOMMIT_TRUE);
	}catch(castor::exception::Exception ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper linkRequestToSvcClassOnDB) dbService->fillRep throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	   
      }


      
      
      
      


      /*******************************************************************************/
      /* get the castoFile associated to the subrequest                             */ 
      /*****************************************************************************/
      void StagerRequestHelper::getCastorFile() throw(castor::exception::Internal)
      {
	try{
	  this->castorFile=subrequest->castorFile();
	  if(this->castorFile == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getCastorFile) Impossible to get the castorFile"<<std::endl;
	    throw ex; 
	  }
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
      }
      

      
      
      /****************************************************************************************/
      /* get fileClass by selecting with stagerService                                       */
      /* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
      /*************************************************************************************/
      void StagerRequestHelper::getFileClass(char* nameClass) throw(castor::exception::Internal)
      {
	try{
	  std::string fileClassName(nameClass);
	  fileClass=stagerService->selectFileClass(fileClassName);//throw exception if it is null
	   if(this->fileClass == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getFileClass) Impossible to get the fileClass"<<std::endl;
	    throw ex; 
	   }
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
      }



      
      /******************************************************************************************/
      /* get and (create or initialize) Cuuid_t subrequest and request                         */
      /* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
      /***************************************************************************************/

      /* get or create subrequest uuid */
      void StagerRequestHelper::setSubrequestUuid() throw(castor::exception::Internal)
      {
	Cuuid_t subrequest_uuid=NOT_VALID;//if we manage to get the cuuid version, we update 
	char subrequest_uuid_as_string[CUUID_STRING_LEN+1];	

	try{
	  if (subrequest->subreqId().empty()){/* we create a new Cuuid_t, copy to thread-safe variable, and update it on subrequest...*/
	    Cuuid_create(&subrequest_uuid);
	    this->subrequestUuid = subrequest_uuid;/* COPY TO THREAD-SAFE VARIABLE*/
	    
	    /* convert to the string version, needed to update the subrequest and fill it on DB*/ 
	    if(Cuuid2string(subrequest_uuid_as_string, CUUID_STRING_LEN+1, &subrequest_uuid) != 0){
	      
	      castor::dlf::Param param[] = { castor::dlf::Param("Message", "(StagerRequestHelper setSubrequestUuid) error (but no exception) on Cuuid2string")};
	      castor::dlf::dlf_writep(, DLF_LVL_WARNING, 2, 1, param);
	    }else{
	      subrequest_uuid_as_string[CUUID_STRING_LEN] = '\0';
	      /* update on subrequest */
	      subrequest->setSubreqId(subrequest_uuid_as_string);
	      /* update it in DB*/
	      
	      dbService->updateRep(baseAddr, subrequest, STAGER_AUTOCOMMIT_TRUE);		
	    }
	  }else{ /* translate to the Cuuid_t version and copy to our thread-safe variable */
	    if( string2Cuuid(&subrequest_uuid, (char *) subrequest_uuid_as_string)!=0){
	      
	      castor::dlf::Param param[] = { castor::dlf::Param("Message", "(StagerRequestHelper setSubrequestUuid) error (but no exception) on string2Cuuid")};
	      castor::dlf::dlf_writep(, DLF_LVL_WARNING, 2, 1, param);
	    }else{
	      this->subrequestUuid = subrequest_uuid; /* COPY TO THREAD-SAFE VARIABLE*/ 
	    }
	  }
	}catch(castor::exception::Exception exep){
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", exep.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper setSubrequestUuid) dbService->updateRep  throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	
      }
      
      /* get request uuid (we cannon' t create it!) */ 
      void StagerRequestHelper::setRequestUuid()
      {
	Cuuid_t request_uuid;

	if(fileRequest->reqId().empty()){/* we cannon' t generate one!*/
	  
	  castor::dlf::Param param[] = { castor::dlf::Param("Message", "(StagerRequestHelper setRequestUuid) Request has no uuid")};
	  castor::dlf::dlf_writep(, DLF_LVL_WARNING, 2, 1, param);
	  
	}else{/*convert to the Cuuid_t version and copy in our thread safe variable */
	   
	  char* uuid_as_string = fileRequest->reqId();
	  if (string2Cuuid(&request_uuid,(char *) uuid_as_string) != 0) {
	    
	    castor::dlf::Param param[] = { castor::dlf::Param("Message", "(StagerRequestHelper setRequestUuid) error (but no exception) on string2Cuuid")};
	    castor::dlf::dlf_writep(, DLF_LVL_WARNING, 2, 1, param);

	  }else{
	    this->requestUuid = request_uuid;
	  }
	}

      }





      /*******************************************************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
      /*****************************************************************************************************************************************/
      
      void StagerRequestHelper::linkCastorFileToServiceClass(StagerCnsHelper stgCnsHelper) throw(castor::exception::Internal)
      {
	unsigned64 fileClassId = fileClass->id();
	unsigned64 svcClassId = svcClass->id();
	
	/* get the castorFile from the stagerService and fill it on the subrequest */
	
	/* we use the stagerService.selectCastorFile to get it from DB */
	try{
	  castorFile = stagerService->selectCastorFile(stgCnsHelper.cnsFileid.fileid, stgCnsHelper.cnsFileid.server,svcClassId, fileClassId, stgCnsHelper.cnsFilestat.filesize,subrequest->fileName());
	  if(this->castorFile == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper linkCastorFileToServiceClass) Impossible to get the castorFile"<<std::endl;
	    throw ex; 
	  }

	}catch(castor::exception::Internal ex){/* castorFile == NULL */
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;

	}catch(castor::exception::Exception exep){/* stagerService throw exception */
	   castor::dlf::Param param[] = {castor::dlf::Param("Message", exep.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper linkCastorFileToSvcClass) stagerService->selectCastorFile throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}

	subrequest->setCastorFile(castorFile);
	try{
	  /* create the subrequest-> castorFile on DB*/
	  dbService->fillRep(baseAddr, subrequest, OBJ_CastorFile, STAGER_AUTOCOMMIT_TRUE);//throw exception
	  /* create the castorFile-> svcClass link on DB */
	  dbService->fillObj(baseAddr, castorFile, OBJ_SvcClass, 0);//throw exception	

	}catch(castor::exception::Exception exep){/* stagerService throw exception */
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", exep.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper linkCastorFileToSvcClass) dbService->fill___  throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}


      }



      /****************************************************************************************************/
      /*  check if the castorFile is linked to the service Class: called in StagerRequest.jobOriented()*/
      /**************************************************************************************************/
      void StagerRequestHelper::isCastorFileLinkedToSvcClass() throw(castor::exception::Internal)
      {
	castor::stager::SvcClass* svcClass_from_castorFile = castorFile->svcClass();

	try{
	  if(svcClass_from_castorFile == NULL){
	    castorFile->setSvcClass(svcClass);
	    dbService->fillRep(baseAddr, castorFile, OBJ_SvcClass, STAGER_AUTOCOMMIT_TRUE);//THROW EXCEPTION!
	    
	  }else{
	    if(svcClass == NULL){
	      delete svcClass; /***** just a call to the delete cpp (including the destructor) ***/
	    }
	    svcClass = castorFile->svcClass();
	  }
	}catch(castor::exception::Exception exep){/* stagerService throw exception */
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", exep.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper isCastorFileLinkedToSvcClass) dbService->fill___  throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
      }



      
      /****************************************************************************************************/
      /*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
      /**************************************************************************************************/
      std::string StagerRequestHelper::getPartitionMask() throw(castor::exception::Internal)
      {
	try{
	  if(svcClass->name().empty()){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper getPartitionMask) svcClassName is empty"<<std::endl;
	    throw ex;
	  }else{
	    std::string svcClassName =  svcClass->name();
	    strncpy(partitionMask, svcClassName.c_str(),RM_MAXPARTITIONLEN+1);
	  }
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	return(this->partitionMask);
      }
      



      /****************************************************************************************************/
      /*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
      /**************************************************************************************************/
      void StagerRequestHelper::setFileClassOnCastorFile() throw(castor::exception::Internal)
      {
	try{
	  if(this->castorFile == NULL){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper setFileClassOnCastorFile) Impossible to get the castorFile"<<std::endl;
	    throw ex;
	  }else{
	    castorFile->setFileClass(fileClass);
	    dbService->fillRep(baseAddr, castorFile, OBJ_FileClass, STAGER_AUTOCOMMIT_TRUE);
	  }
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}catch(castor::exception::Exception exep){/* stagerService throw exception */
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", exep.getMessage().str()), castor::dlf::Param("Message", "(StagerRequestHelper setFileClassOnCastorFile) dbService->fillRep  throws an exception")};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 2, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	
      }


     



      /************************************************************************************/
      /* set the username and groupname string versions using id2name c function  */
      /**********************************************************************************/
      void StagerRequestHelper::setUsernameAndGroupname() throw(castor::exception::Internal)
      {
	struct passwd *this_passwd;
	struct group *this_gr;
	
	uid_t euid = fileRequest->euid();
	uid_t egid = fileRequest->egid();

	try{
	  if((this_passw=Cgetpwuid(euid)) == NULL){
	    serrno=SEURSERUNKN; 
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Username"<<std::endl;
	    throw ex;
	  }
	  
	  if(egid != this_passwd->pw_gid){
	    serrno=EINVAL;
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Username"<<std::endl;
	    throw ex;
	  }
	  
	  if((this_gr=Cgetgrgid(egid))==NULL){
	    serrno=SEUSERUNKN;
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Groupname"<<std::endl;
	    throw ex;
	  }
	  
	  if(username!=NULL){
	    username.copy(this_passwd->pw_name,RM_MAXUSRNAMELEN);
	  }
	  
	  if(groupname!=NULL){
	    groupname.copy(this_gr->gr_name,RM_MAXGRPNAMELEN);
	  }
	}catch(castor::exception::Internal ex){
	  
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
       
      }


     


      /****************************************************************************************************/
      /* depending on fileExist and type, check the file needed is to be created or throw exception    */
      /********************************************************************************************/
      bool StagerRequestHelper::isFileToCreateOrException(bool fileExist) throw(castor::exception::Internal)
      {

	bool toCreate=FALSE;
	int type = this->subrequest.type();
	int subrequestFlags = this->subrequest.flags();

	try{
	  if(!fileExist){
	    if ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type)||((O_CREAT == (subRequestFlags & O_CREAT))&&((OBJ_StageUpdateRequest == type) ||(OBJ_StagePrepareToUpdateRequest == type)))) {
	      if (serrno == ENOENT) {
		toCreate=TRUE;
	      } else {
		castor::exception::Internal ex;
		ex.getMessage()<<"(StagerRequestHelper isFileToCreateOrException) Impossible to create the required file"<<std::endl;
		throw ex;
		castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
		castor::dlf::dlf_writep(, DLF_LVL_SYSTEM, 2, 1, param);
	      }
	    }else if(OBJ_StageGetRequest == type) || (OBJ_StagePrepareToGetRequest == type) ||(OBJ_StageRepackRequest == type) ||(OBJ_StageUpdateRequest == type) ||                          (OBJ_StagePrepareToUpdateRequest == type)||(OBJ_StageRmRequest == type) ||(OBJ_SetFileGCWeight == type) ||(OBJ_StagePutDoneRequest == type)){
	    castor::exception::Internal ex;
	    ex.getMessage()<<"(StagerRequestHelper isFileToCreateOrException) Asking for a file which doesn't exist"<<std::endl;
	    throw ex;
	    castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	    castor::dlf::dlf_writep(, DLF_LVL_USAGE, 2, 1, param);
	  }
	}catch(castor::exception::Internal ex){ 
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	
	return(toCreate);
      }



      /*****************************************************************************************************/
      /* check if the user (euid,egid) has the ritght permission for the request's type                   */
      /* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
      /**************************************************************************************************/
      bool StagerRequestHelper::checkFilePermission() throw(castor::exception::Internal)
      {
	bool filePermission = true;
	int type =  this->subrequest.type();
	std::string filename = this->subrequest.filename();
	uid_t euid = this->fileRequest.euid();
	uid_t egid = this->fileRequest.egid();

	try{
	  switch(type) {
	  case OBJ_StageGetRequest:
	  case OBJ_StagePrepareToGetRequest:
	  case OBJ_StageRepackRequest:
	    filePermission=R_OK;
	    if ( Cns_accessUser(filename,R_OK,euid,egid) == -1 ) {
	      filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerRequestHelper checkFilePermission) Access denied: The user doesn't have the read permission"<<std::endl;
	      throw ex;
	    }
	    break;
	  case OBJ_StagePrepareToPutRequest:
	  case OBJ_StagePrepareToUpdateRequest:
	  case OBJ_StagePutRequest:
	  case OBJ_StageRmRequest:
	  case OBJ_StageUpdateRequest:
	    filePermission=W_OK;
	    if ( Cns_accessUser(filename,W_OK,euid,egid) == -1 ) {
	      filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	      castor::exception::Internal ex;
	      ex.getMessage()<<"(StagerRequestHelper checkFilePermission) Access denied: The user doesn't have the write permission"<<std::endl;
	      throw ex;
	      
	    }
	    break;
	  default:
	    break;
	  }
	}catch(castor::exception::Internal ex){ 
	  castor::dlf::Param param[] = {castor::dlf::Param("Message", ex.getMessage().str())};
	  castor::dlf::dlf_writep(, DLF_LVL_ERROR, 2, 1, param);
	  
	  /* we throw an exception for the next level (StagerDBService) */
	  castor::exception::Internal e;
	  throw e;
	}
	return(filePermission);
      }


      /*****************************************************************************************************/
      /* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
      /* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
      /**************************************************************************************************/
      struct rmjob StagerRequestHelper::buildRmJobHelperPart(struct rmjob &rmjob)//after processReplica (if it is necessary)
      {


	rmjob.uid = (uid_t) fileRequest->euid();
	rmjob.gid = (uid_t) fileRequest->egid();
	
	strncpy(rmjob.uname, username.c_str(),RM_MAXUSRNAMELEN);
	rmjob.uname[RM_MAXUSRNAMELEN] = '\0';
	strncpy(rmjob.gname, groupname.c_str(),RM_MAXGRPNAMELEN);
	rmjob.gname[RM_MAXGRPNAMELEN] = '\0';

	
	strncpy(rmjob.partitionmask,getOrInitiatePartitionMask(),RM_MAXPARTITIONLEN);//partitionMask: char *
	rmjob.partitionmask[RM_MAXPARTITIONLEN] = '\0';

	std::string features;
	features.rsize(RM_MAXFEATURELEN+1);

	char* className = fileRequest->svcClassName().c_str(); 
	if((className == NULL)||(className[0] == '\0')){
	  className = "default" ;
	}
	features.copy(svcClass.className(), RM_MAXFEATURELEN, 0);
	std::string protocol = subrequest->protocol();
	if(!protocol().empty()){
	  if((features.size()+1+protocol.size())<RM_MAXFEATURELEN){
	    features+= ":";
	    features+= protocol;
	    
	  }
	}else{
	  if((features.size()+1+default_protocol.size())<RM_MAXFEATURELEN){
	    features+=":"+default_protocol;
	  }
	}
	
	strncpy(rmjob.rfeatures, features.c_str(), RM_MAXFEATURELEN);
	               

	
	u64tostr(subrequest.id(),rmjob.stageid,0);
	strcat(rmjob.stageid,"@");
	
	//adding request and subrequest uuid:
	{
	  Cuuid_t thisuuid=requestUuid;
	  Cuuid2string(rmjob.requestid, CUUID_STRING_LEN+1,&thisuuid);
	  thisuuid=subrequestUuid;
	  Cuuid2string(rmjob.subrequestid, CUUID_STRING-LEN+1,&thisuuid);
	}


	std::string client_as_string = IClientFactory.client2String(iClient);
	rmjob.clientStruct = client_as_string.c_str();//what is the max len for client?
	strcpy(rmjob.exec,"/usr/bin/stagerJob.sh");
	strncpy(rmjob.account,fileRequest->svcClassName().c_str(),RM_MAXACCOUNTNAMELEN);
	
	return(rmjob);
      }



     

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
