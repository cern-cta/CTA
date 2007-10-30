/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"


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
#include "osdep.h"
/* to get a instance of the services */
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"



#include "castor/IClientFactory.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <vector>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>




namespace castor{
  namespace stager{
    namespace dbService{
      
     
      /* constructor-> return the request type, called on the different thread (job, pre, stg) */
      StagerRequestHelper::StagerRequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest) throw(castor::exception::Exception){	

	try{
	  castor::IService* svc =
	    castor::BaseObject::services()->
	    service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
	  if (0 == svc) {
	    castor::exception::Exception e(SEINTERNAL);
	    throw e;
	  }
	  this->stagerService = dynamic_cast<castor::stager::IStagerSvc*>(svc);
	  if (0 == this->stagerService) {
	    castor::exception::Exception e(SEINTERNAL);
	    throw e;
	  }
	  
	  castor::IService* svcDB =
	    castor::BaseObject::services()->
	    service("DbCnvSvc", castor::SVC_DBCNV);
	  if (0 == svcDB) {
	    castor::exception::Exception e(SEINTERNAL);
	    throw e;
	  }
	  this->dbService = dynamic_cast<castor::db::DbCnvSvc*>(svcDB);
	  if (0 == this->dbService) {
	    castor::exception::Exception e(SEINTERNAL);
	    throw e;
	  }
	  
	  this->baseAddr =  new castor::BaseAddress; /* the settings ll be done afterwards */
	  
	  baseAddr->setCnvSvcName("DbCnvSvc");
	  baseAddr->setCnvSvcType(SVC_DBCNV);
	  
	  this->subrequest=subRequestToProcess;
	  this->default_protocol = "rfio";
	  
	  dbService->fillObj(baseAddr, subrequest, castor::OBJ_FileRequest, false); 
	  this->fileRequest=subrequest->request();
	  
	  typeRequest = fileRequest->type();
	}catch(castor::exception::Exception e){
	  castor::dlf::Param params[]={	castor::dlf::Param("Error while getting the services","Handler level")};

	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_SERVICES_EXCEPTION, 1 ,params);	    	  
	  e.getMessage()<< "(StagerRequestHelper constructor)"<<std::endl;
	  throw e;  


	}
      }



      /* destructor */
      StagerRequestHelper::~StagerRequestHelper() throw()
      {
      }





   


      /**************************************************************************************/
      /* get/create subrequest->  fileRequest, and many subrequest's attributes            */
      /************************************************************************************/
      
      /* get subrequest using stagerService (and also types) */
      /***  inline void StagerRequestHelper::getSubrequest() throw(castor::exception::Exception)  ***/
     
      

      /* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
      /* using dbService, and get the fileRequest */ 
      /***  inline void StagerRequestHelper::getFileRequest() throw(castor::exception::Exception) ***/
     


      /***********************************************************************************/
      /* get the link (fillObject~SELECT) between fileRequest and its associated client */
      /* using dbService, and get the client                                           */
      /********************************************************************************/
      /*** inline void StagerRequestHelper::getIClient() throw(castor::exception::Exception)  ***/
      
      
	


      
      
      /****************************************************************************************/
      /* get svClass by selecting with stagerService                                         */
      /* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
      /*************************************************************************************/
      /*** inline void StagerRequestHelper::getSvcClass() throw(castor::exception::Exception) ***/
      
      
      
      
      /*******************************************************************************/
      /* update request in DB, create and fill request->svcClass link on DB         */ 
      /*****************************************************************************/
      /*** inline void StagerRequestHelper::linkRequestToSvcClassOnDB() throw(castor::exception::Exception) ***/
     

     
      /*******************************************************************************/
      /* get the castoFile associated to the subrequest                             */ 
      /*****************************************************************************/
      /*** inline void StagerRequestHelper::getCastorFile() throw(castor::exception::Exception) ***/
           

      
      
      /****************************************************************************************/
      /* get fileClass by selecting with stagerService                                       */
      /* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
      /*************************************************************************************/
      /***  inline void StagerRequestHelper::getFileClass(char* nameClass) throw(castor::exception::Exception) ***/
     



      
      /******************************************************************************************/
      /* get and (create or initialize) Cuuid_t subrequest and request                         */
      /* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
      /***************************************************************************************/
      /* get or create subrequest uuid */
      void StagerRequestHelper::setSubrequestUuid() throw(castor::exception::Exception)
      {

	try{
	  char subrequest_uuid_as_string[CUUID_STRING_LEN+1];	
	  
	  if (subrequest->subreqId().empty()){/* we create a new Cuuid_t, copy to thread-safe variable, and update it on subrequest...*/
	    Cuuid_create(&(this->subrequestUuid));
	    
	  
	    /* convert to the string version, needed to update the subrequest and fill it on DB*/ 
	    if(Cuuid2string(subrequest_uuid_as_string, CUUID_STRING_LEN+1, &(this->subrequestUuid)) != 0){
	      castor::exception::Exception ex(SEINTERNAL);
	      throw ex;
	      
	    }else{
	      subrequest_uuid_as_string[CUUID_STRING_LEN] = '\0';
	      /* update on subrequest */
	      subrequest->setSubreqId(subrequest_uuid_as_string);
	      /* update it in DB*/
	      dbService->updateRep(baseAddr, subrequest, true);	
	    
	    }

	  }else{ /* translate to the Cuuid_t version and copy to our thread-safe variable */
	  
	    if( string2Cuuid(&(this->subrequestUuid), (char *) subrequest_uuid_as_string)!=0){
	      castor::exception::Exception ex(SEINTERNAL);
	      throw ex;
	    }

	  }
	}catch(castor::exception::Exception e){
	  castor::dlf::Param params[]={ castor::dlf::Param("Impossible to get the subrequestUuid"," ")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_SUBREQUESTUUID_EXCEPTION, 1 ,params);	    
	    
	  e.getMessage()<< "(StagerRequestHelper setSubrequestUuid)"<<std::endl;
	  throw e;  

	}
      }
      

      /* get request uuid (we cannon' t create it!) */ 
      void StagerRequestHelper::setRequestUuid() throw(castor::exception::Exception)
      {
	try{
	  
	  if(fileRequest->reqId().empty()){/* we cannon' t generate one!*/
	    castor::exception::Exception ex(SEINTERNAL); 
	    throw ex;
	  }else{/*convert to the Cuuid_t version and copy in our thread safe variable */
	    castor::dlf::Param param[]= {castor::dlf::Param("Standard Message","(StagerRequestHelper RequestUuid) RequestUuid:"),
					 castor::dlf::Param("Standard Message",fileRequest->reqId())};
	    castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 2, param);/*   */
	    
	    if (string2Cuuid(&(this->requestUuid),(char*) fileRequest->reqId().c_str()) != 0) {
	      castor::exception::Exception ex(SEINTERNAL);	      
	      throw ex;
	    }
	  }
	}catch(castor::exception::Exception e){
	  
	  castor::dlf::Param params[]={ castor::dlf::Param("Impossible to get the RequestUuid"," ")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_REQUESTUUID_EXCEPTION, 1 ,params);	    
	  
	  e.getMessage()<< "(StagerRequestHelper setRequestUuid)"<<std::endl;
	  throw e; 
	}

      }





      /*******************************************************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
      /*****************************************************************************************************************************************/      
      void StagerRequestHelper::getCastorFileFromSvcClass(StagerCnsHelper stgCnsHelper) throw(castor::exception::Exception)
      {
	u_signed64 fileClassId = fileClass->id();
	u_signed64 svcClassId = svcClass->id();
	
	/* get the castorFile from the stagerService and fill it on the subrequest */
	/* we use the stagerService.selectCastorFile to get it from DB */

	try{
	  castorFile = stagerService->selectCastorFile(stgCnsHelper.cnsFileid.fileid, stgCnsHelper.cnsFileid.server,svcClassId, fileClassId, stgCnsHelper.cnsFilestat.filesize,subrequest->fileName());

	  subrequest->setCastorFile(castorFile);
	  
	  /* create the subrequest-> castorFile on DB*/
	  dbService->fillRep(baseAddr, subrequest, castor::OBJ_CastorFile, true);//throw exception
	  
	  /* create the castorFile-> svcClass link on DB */
	  dbService->fillObj(baseAddr, castorFile, castor::OBJ_SvcClass, 0);//throw exception	

	}catch(castor::exception::Exception e){
	  e.getMessage()<<"(StagerRequestHelper getCastorFileFromSvcClass)"<<std::endl;
	  throw(e);
	}

      }


     

      /****************************************************************************************************/
      /*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
      /**************************************************************************************************/
      /***  void StagerRequestHelper::setFileClassOnCastorFile() throw(castor::exception::Exception)  ***/
      



      /************************************************************************************/
      /* set the username and groupname string versions using id2name c function  */
      /**********************************************************************************/
      /***  inline void StagerRequestHelper::setUsernameAndGroupname() throw(castor::exception::Exception)  ***/
      

      /****************************************************************************************************/
      /* depending on fileExist and type, check the file needed is to be created or throw exception    */
      /********************************************************************************************/
      /*** inline bool StagerRequestHelper::isFileToCreateOrException(bool fileExist) throw(castor::exception::Exception)  ***/
      

      /*****************************************************************************************************/
      /* check if the user (euid,egid) has the ritght permission for the request's type                   */
      /* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
      /**************************************************************************************************/
      bool StagerRequestHelper::checkFilePermission() throw(castor::exception::Exception)
      {
     
	  bool filePermission = true;
	  try{
	  
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
		throw ex;
	
	      }	   
	      break;
	      
	    case OBJ_StagePrepareToPutRequest:
	    case OBJ_StagePrepareToUpdateRequest:
	    case OBJ_StagePutRequest:
	    case OBJ_StageRmRequest:
	    case OBJ_StageUpdateRequest:
      case OBJ_StagePutDoneRequest:
      case OBJ_SetFileGCWeight:
	      filePermission=W_OK;
	      if ( Cns_accessUser(filename.c_str(),W_OK,euid,egid) == -1 ) {
		filePermission=false; // even if we treat internally the exception, lets gonna use it as a flag
		castor::exception::Exception ex(SEINTERNAL);
		throw ex;
			      
	      }
	      break;
	      
	    default:
	      break;
	    }
	  }catch(castor::exception::Exception e){
	     castor::dlf::Param params[]={ castor::dlf::Param(subrequestUuid),
					  castor::dlf::Param("Subrequest fileName",subrequest->fileName()),
					  castor::dlf::Param("SvcClassName",svcClassName)				     
	    };
	    castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION, 3 ,params);	    
	    
	    e.getMessage()<< "Access denied"<<std::endl;
	    throw e;  
	  }
	  return(filePermission);
	}



     

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
