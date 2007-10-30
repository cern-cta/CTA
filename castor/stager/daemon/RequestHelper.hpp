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

#include "Cns_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/ObjectSet.hpp"
#include "castor/Constants.hpp"

#include "serrno.h"
#include <errno.h>

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

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
      class castor::IClient;
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
		
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	/* get from the stagerService using as key Cnsfileclass.name (JOB ORIENTED)*/
	castor::stager::FileClass* fileClass;
       
	char username[CA_MAXLINELEN+1];
	char groupname[CA_MAXLINELEN+1];

	/* Cuuid_t thread safe variables */ 
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;
	
	std::string default_protocol;




	/****************************************************/
	/*  called on the different thread (job, pre, stg) */
	StagerRequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest) throw(castor::exception::Exception);

	/* destructor */
	~StagerRequestHelper() throw();

	/***********************************************************************************/
	/* get the link (fillObject~SELECT) between fileRequest and its associated client */
	/* using dbService, and get the client                                           */
	/********************************************************************************/
	inline void getIClient() throw(castor::exception::Exception){

	  dbService->fillObj(baseAddr, fileRequest,castor::OBJ_IClient, false);//196
	  this->iClient=fileRequest->client();
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

	  }

	 
	  svcClass=stagerService->selectSvcClass(this->svcClassName);//check if it is NULL
	  if(this->svcClass == NULL){
	    castor::dlf::Param params[]={ castor::dlf::Param(subrequestUuid),
					  castor::dlf::Param("Subrequest fileName",subrequest->fileName()),
					  castor::dlf::Param("UserName",username),
					  castor::dlf::Param("GroupName", groupname),
					  castor::dlf::Param("SvcClassName",svcClassName)				     
	    };
	    castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_SVCCLASS_EXCEPTION, 5 ,params);
	    
	    castor::exception::Exception ex(SESVCCLASSNFND);
	    ex.getMessage()<<"(StagerRequestHelper getSvcClass)"<<std::endl;
	   
	  }
	   
	  
	}
     
      
      
	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	inline void linkRequestToSvcClassOnDB() throw(castor::exception::Exception){
	 
	  /* update request on DB */
	  dbService->updateRep(baseAddr, fileRequest, true);//EXCEPTION!    
	  fileRequest->setSvcClass(svcClass);
	  
	  /* fill the svcClass object using the request as a key  */
	  dbService->fillRep(baseAddr, fileRequest,castor::OBJ_SvcClass,true);
	  
	}
     
      


      
      
	/****************************************************************************************/
	/* get fileClass by selecting with stagerService                                       */
	/* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
	/*************************************************************************************/
	inline void getFileClass(char* nameClass) throw(castor::exception::Exception){
	  std::string fileClassName(nameClass);
	  fileClass=stagerService->selectFileClass(fileClassName);//throw exception if it is null
	  
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
       	void getCastorFileFromSvcClass(castor::stager::dbService::StagerCnsHelper stgCnsHelper) throw(castor::exception::Exception);
       


      
     

	/****************************************************************************************************/
	/*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	/**************************************************************************************************/
	inline void setFileClassOnCastorFile() throw(castor::exception::Exception){
	  
	  castorFile->setFileClass(fileClass);
	  dbService->fillRep(baseAddr, castorFile, castor::OBJ_FileClass, true);
	  
	}
     


	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	inline void setUsernameAndGroupname() throw(castor::exception::Exception){
	  struct passwd *this_passwd;
	  struct group *this_gr;
	  try{
	    uid_t euid = fileRequest->euid();
	    uid_t egid = fileRequest->egid();
	    
	    if((this_passwd = Cgetpwuid(euid)) == NULL){
	      castor::exception::Exception ex(SEUSERUNKN);
	      throw ex;
	    }
	    
	    if(egid != this_passwd->pw_gid){
	      castor::exception::Exception ex(SEINTERNAL);     
	      throw ex;
	    }
	    
	    if((this_gr=Cgetgrgid(egid))==NULL){
	      castor::exception::Exception ex(SEUSERUNKN);
	      throw ex;
	    
	    }
	    
	    if((this->username) != NULL){
	      strncpy(username,this_passwd->pw_name,CA_MAXLINELEN);
	      username[CA_MAXLINELEN]='\0';
	    }
	    if((this->groupname) != NULL){
	      strncpy(groupname,this_gr->gr_name,CA_MAXLINELEN);
	      groupname[CA_MAXLINELEN]='\0';
	    }
	  }catch(castor::exception::Exception e){
	     castor::dlf::Param params[]={ castor::dlf::Param(subrequestUuid),
					   castor::dlf::Param("Subrequest fileName",subrequest->fileName()),
					   castor::dlf::Param("SvcClassName",svcClassName)				     
	     };
	     castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_INVALID, 3 ,params);	    
	    
	     e.getMessage()<< "(StagerRequestHelper setUsernameAndGroupname)"<<std::endl;
	     throw e;    
	  }
	 
	    
	}
      


	/**
	 * check if the user (euid,egid) has the right permission for the request's type
	 * note that we were not checking the permissions for SetFileGCWeight and PutDone request (true),
   * now you need write permissions
	 */
	bool checkFilePermission() throw(castor::exception::Exception);
     
	

      }; //end StagerRequestHelper class
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
