/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#ifndef STAGER_REQUEST_HELPER_HPP
#define STAGER_REQUEST_HELPER_HPP 1

#include "castor/stager/dbService/StagerCnsHelper.hpp"

#include "castor/IObject.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/ObjectSet.hpp"
#include "castor/Constants.hpp"

#include <vector>
#include <iostream>
#include <string>
#include <string.h>


namespace castor{
  namespace stager{
    namespace dbService{

      
      class castor::stager::IStagerSvc;
      class castor::db::DbCnvSvc;
      class castor::BaseAddress;
      class castor::stager::SubRequest;
      class castor::stager::FileRequest;
      class castor::stager::IClient;
      class castor::stager::SvcClass;
      class castor::stager::CastorFile;
      class castor::stager::FileClass;
      class castor::stager::dbService::StagerCnsHelper;
      class castor::IClientFactory;
    
      

      class StagerRequestHelper : public virtual castor::BaseObject{



      public:
	  	
	/* services needed: database and stager services*/
	castor::stager::IStagerSvc* stagerService;
	castor::db::DbCnvSvc* dbService;
    

	/* BaseAddress */
	castor::BaseAddress* baseAddr;


	/* subrequest and fileRequest  */
	castor::stager::SubRequest* subrequest;
	
	castor::stager::FileRequest* fileRequest;

	/* client associated to the request */
	castor::IClient* iClient;
	std::string iClientAsString;
	
	/* service class */
	std::string svcClassName;
	castor::stager::SvcClass* svcClass;
	
	char partitionMask[RM_MAXPARTITIONLEN+1];/* initialized to svcClass.name on stagerRequest.jobOriented ()*/
	
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	/* get from the stagerService using as key Cnsfileclass.name (JOB ORIENTED)*/
	castor::stager::FileClass* fileClass;
       
	char username[RM_MAXUSRNAMELEN+1];
	char groupname[RM_MAXGRPNAMELEN+1];

	/* Cuuid_t thread safe variables */ 
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;
	std::vector<ObjectsIds> types;

	std::string default_protocol;
	/****************/
	/* constructor */
	/* destructor */
	StagerRequestHelper() throw(castor::exception::Exception);
	~StagerRequestHelper() throw();
	


	
	/**********************/
	/* baseAddr settings */ 
	/********************/
	inline void settingBaseAddress(){
	  baseAddr->setCnvSvcName("DbCnvSvc");
	  baseAddr->setCnvSvcType(SVC_DBCNV);
	}

	/**************************************************************************************/
	/* get/create subrequest->  fileRequest, and many subrequest's attributes            */
	/************************************************************************************/
	
	/* get subrequest using stagerService (and also types) */
	inline void setSubrequest(castor::stager::SubRequest* subRequestToProcess) throw(castor::exception::Exception){
	  this->subrequest=subRequestToProcess;
	  if(this->subrequest == NULL){
	    castor::exception::Exception ex(SEENTRYNFND);
	    ex.getMessage()<<"(StagerRequestHelper setSubrequest) Got a NULL subrequest"<<std::endl;
	    throw ex;
	  }
	}
       
	/* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
	/* using dbService, and get the fileRequest */ 
	inline void getFileRequest() throw(castor::exception::Exception){
	  try{
	    dbService->fillObj(baseAddr, subrequest, castor::OBJ_FileRequest, false); /* 155 */ 
	  }catch(castor::exception::Exception e){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper getFileRequest) Exception throwed by the dbService->fillObj"<<std::endl;
	    throw ex;
	  }
	  this->fileRequest=subrequest->request();
	  if(this->fileRequest == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper getFileRequest) Impossible to get the fileRequest"<<std::endl;
	    throw ex;
	  }
	}
      
    


	/***********************************************************************************/
	/* get the link (fillObject~SELECT) between fileRequest and its associated client */
	/* using dbService, and get the client                                           */
	/********************************************************************************/
	inline void getIClient() throw(castor::exception::Exception){
	  try{
	    dbService->fillObj(baseAddr, fileRequest,castor::OBJ_IClient, false);//196
	  }catch(castor::exception::Exception e){
	    e.getMessage()<<"(StagerRequestHelper getIClient) Exception throwed by the dbService->fillObj"<<std::endl;
	    throw e;
	  }
	  this->iClient=fileRequest->client();
	  if(this->iClient == NULL){
	    
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper getIClient) Impossible to get the iClient"<<std::endl;
	    throw ex;
	  }
	  //const castor::IClient* iAuxClient = this->iClient;
	  this->iClientAsString = IClientFactory::client2String(*(this->iClient));/* IClientFactory singleton */
	}
	
	
	
	/****************************************************************************************/
	/* get svClass by selecting with stagerService                                         */
	/* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
	/*************************************************************************************/
	inline void getSvcClass() throw(castor::exception::Exception){
	  this->svcClassName=fileRequest->svcClassName(); 
	  
	  if(this->svcClassName.empty()){  /* we set the default svcClassName */
	    this->svcClassName="default";
	    fileRequest->setSvcClassName(this->svcClassName);

	    /* it isnt really necessary and it causes and segFault on SvcClass.hpp */
	    /*  svcClass->setName(this->svcClassName);*/

	    this->svcClassName=fileRequest->svcClassName(); /* we retrieve it to know if it has been correctly updated */
	    if(this->svcClassName.empty()){      
	      castor::exception::Exception ex(SESVCCLASSNFND);
	      ex.getMessage()<<"(StagerRequestHelper getSvcClass) Impossible to set properly the svcClassName on fileRequest"<<std::endl;
	      throw ex;
	    }
	  }
	  
	  svcClass=stagerService->selectSvcClass(this->svcClassName);//check if it is NULL
	  if(this->svcClass == NULL){
	    castor::exception::Exception ex(SESVCCLASSNFND);
	    ex.getMessage()<<"(StagerRequestHelper getSvcClass) Impossible to get the svcClass"<<std::endl;
	    throw ex; 
	  }
	}
     
      
      
	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	inline void linkRequestToSvcClassOnDB() throw(castor::exception::Exception){
	  try{
	    /* update request on DB */
	    dbService->updateRep(baseAddr, fileRequest, true);//EXCEPTION!
	    
	  }catch(castor::exception::Exception e){	  
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<< "(StagerRequestHelper linkRequestToSvcClassOnDB) dbService->updateRep throws an exception"<<std::endl;	
	    throw ex;
	  }
	  
	  fileRequest->setSvcClass(svcClass);
	  
	  try{
	    /* fill the svcClass object using the request as a key  */
	    dbService->fillRep(baseAddr, fileRequest,castor::OBJ_SvcClass,true);
	  }catch(castor::exception::Exception ex){
	    castor::exception::Exception e(SEINTERNAL);
	    e.getMessage()<< "(StagerRequestHelper linkRequestToSvcClassOnDB) dbService->fillRep throws an exception"<<std::endl;
	    throw e;
	  }
	}
     
      

	/*******************************************************************************/
	/* get the castoFile associated to the subrequest                             */ 
	/*****************************************************************************/
	/* inline void getCastorFile() throw(castor::exception::Exception){
	  this->castorFile=subrequest->castorFile();
	  if(this->castorFile == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper getCastorFile) Impossible to get the castorFile"<<std::endl;
	    throw ex; 
	  }
	  } */
      
      
      
	/****************************************************************************************/
	/* get fileClass by selecting with stagerService                                       */
	/* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
	/*************************************************************************************/
	inline void getFileClass(char* nameClass) throw(castor::exception::Exception){
	  std::string fileClassName(nameClass);
	  fileClass=stagerService->selectFileClass(fileClassName);//throw exception if it is null
	  if(this->fileClass == NULL){
	    castor::exception::Exception ex(SEENTRYNFND);
	    ex.getMessage()<<"(StagerRequestHelper getFileClass) Impossible to get the fileClass"<<std::endl;
	    throw ex; 
	  }
	}
      
      
	/******************************************************************************************/
	/* get and (create or initialize) Cuuid_t subrequest and request                         */
	/* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
	/***************************************************************************************/

	/* get or create subrequest uuid */
	void setSubrequestUuid() throw(castor::exception::Exception);
	
      
	/* get request uuid (we cannon' t create it!) */ 
	void setRequestUuid() throw(castor::exception::Exception);
      




	/*******************************************************************************************************************************************/
	/*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
	/*****************************************************************************************************************************************/
       	void getCastorFileFromSvcClass(StagerCnsHelper stgCnsHelper) throw(castor::exception::Exception);
     




     


      
	/*********************************************************************************************************/
	/*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
	/*******************************************************************************************************/
	inline std::string getPartitionMask() throw(castor::exception::Exception){
	  if(svcClassName.empty()){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper getPartitionMask) svcClassName is empty"<<std::endl;
	    throw ex;
	  }else{
	    strncpy(partitionMask, svcClassName.c_str(),RM_MAXPARTITIONLEN+1);
	  }
	  
	  return(this->partitionMask);
	}
     

	/****************************************************************************************************/
	/*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	/**************************************************************************************************/
	inline void setFileClassOnCastorFile() throw(castor::exception::Exception){
	  if(this->castorFile == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper setFileClassOnCastorFile) Impossible to get the castorFile"<<std::endl;
	    throw ex;
	  }else{
	    castorFile->setFileClass(fileClass);
	    dbService->fillRep(baseAddr, castorFile, castor::OBJ_FileClass, true);
	  }
	  
	}
     


	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	inline void setUsernameAndGroupname() throw(castor::exception::Exception){
	  struct passwd *this_passwd;
	  struct group *this_gr;
	  
	  uid_t euid = fileRequest->euid();
	  uid_t egid = fileRequest->egid();
	  
	  if((this_passwd = Cgetpwuid(euid)) == NULL){
	    castor::exception::Exception ex(SEUSERUNKN);
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Username"<<std::endl;
	    throw ex;
	  }
	  
	  if(egid != this_passwd->pw_gid){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Username"<<std::endl;
	    throw ex;
	  }
	  
	  if((this_gr=Cgetgrgid(egid))==NULL){
	    castor::exception::Exception ex(SEUSERUNKN);
	    ex.getMessage()<<"(StagerRequestHelper setUsernameAndGroupname) Impossible to get the Groupname"<<std::endl;
	    throw ex;
	  }
   
	  if((this->username) != NULL){
	    strncpy(username,this_passwd->pw_name,RM_MAXUSRNAMELEN);
	    username[RM_MAXUSRNAMELEN]='\0';
	  }
	  if((this->groupname) != NULL){
	    strncpy(groupname,this_gr->gr_name,RM_MAXGRPNAMELEN);
	    groupname[RM_MAXUSRNAMELEN]='\0';
	  }
	}
      


     

	
	/****************************************************************************************************/
	/* depending on fileExist and type, check the file needed is to be created or throw exception    */
	/********************************************************************************************/
	inline bool isFileToCreateOrException(bool fileExist) throw(castor::exception::Exception){
	  bool toCreate = false;
	  int type = this->fileRequest->type();
	  int subRequestFlags = this->subrequest->flags();
	  
	  if(!fileExist){
	    if ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type)||((O_CREAT == (subRequestFlags & O_CREAT))&&((OBJ_StageUpdateRequest == type) ||(OBJ_StagePrepareToUpdateRequest == type)))) {
	      /* here, we must check if serrno or its new equivalent is === ENOENT !!!!*/
	      toCreate = true;
	    }else if((OBJ_StageGetRequest == type) || (OBJ_StagePrepareToGetRequest == type) ||(OBJ_StageRepackRequest == type) ||(OBJ_StageUpdateRequest == type) ||                          (OBJ_StagePrepareToUpdateRequest == type)||(OBJ_StageRmRequest == type) ||(OBJ_SetFileGCWeight == type) ||(OBJ_StagePutDoneRequest == type)){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerRequestHelper isFileToCreateOrException) Asking for a file which doesn't exist"<<std::endl;
	      throw ex;
	    }
	  }	
	  return(toCreate);
	  
	}
     


	/*****************************************************************************************************/
	/* check if the user (euid,egid) has the ritght permission for the request's type                   */
	/* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
	/**************************************************************************************************/
	inline bool checkFilePermission() throw(castor::exception::Exception){
	  bool filePermission = true;
	  int type =  this->fileRequest->type();
	  std::string filename = this->subrequest->fileName();
	  uid_t euid = this->fileRequest->euid();
	  uid_t egid = this->fileRequest->egid();
	  
	
	  switch(type) {
	  case OBJ_StageGetRequest:
	  case OBJ_StagePrepareToGetRequest:
	  case OBJ_StageRepackRequest:
	    filePermission=R_OK;
	    if ( Cns_accessUser(filename.c_str(),R_OK,euid,egid) == -1 ) {
	      filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	      castor::exception::Exception ex(SEINTERNAL);
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
	    if ( Cns_accessUser(filename.c_str(),W_OK,euid,egid) == -1 ) {
	      filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerRequestHelper checkFilePermission) Access denied: The user doesn't have the write permission"<<std::endl;
	      throw ex;
	      
	    }
	    break;
	  default:
	    break;
	  }
	  return(filePermission);
	};
     


	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	void buildRmJobHelperPart(struct rmjob* rmjob) throw(castor::exception::Exception);//after processReplica (if it is necessary)
	
	
	/* function to update the subrequestStatus */
	/* called within the normal flow and also the exception */
	inline void updateSubrequestStatus(SubRequestStatusCodes newSubrequestStatus) throw(castor::exception::Exception){
	  /* updateSubrequestStatus Part: */
	  SubRequestStatusCodes currentSubrequestStatus = this->subrequest->status();	  
	  if(newSubrequestStatus != currentSubrequestStatus){
	    this->subrequest->setStatus(newSubrequestStatus);
	    this->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
	    /* the status will be update on DB on each handler */
	  }
	}

     

	
	

      }; //end StagerRequestHelper class
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
