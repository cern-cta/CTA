/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "StagerRequestHelper.hpp"

#include "../IStagerSvc.hpp"
#include "../../Services.hpp"
#include "../../BaseAddress.hpp"
#include "../SubRequest.hpp"
#include "../FileRequest.hpp"
#include "../../IClient.hpp"
#include "../SvcClass.hpp"
#include "../CastorFile.hpp"
#include "../FileClass.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/rm_struct.h"

#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/u64subr.h"
#include "../../../h/osdep.h"
#include "../../IClientFactory.hpp"
#include "../../exception/Exception.hpp"
#include "../../exception/Internal.hpp"
#include "../../../h/serrno.h"
#include "../../Constants.hpp"

#include <vector>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define STAGER_OPTIONS 10


namespace castor{
  namespace stager{
    namespace dbService{
      
      /* contructor */
      StagerRequestHelper::StagerRequestHelper() throw(castor::exception::Exception){
	
	this->stagerService =  new castor::stager::IStagerSvc; 
	if(this->stagerService == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the stagerService"<<std::endl;
	  throw ex;
	}
	
	this->dbService = new castor::Services;
	if(this->dbService == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the dbService"<<std::endl;
	  throw ex;
	}
	
	this->baseAddr =  new castor::BaseAddress; /* the settings ll be done afterwards */
	if(this->baseAddr == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper constructor) Impossible to get the baseAddress"<<std::endl;
	  throw ex;
	}
	
	this->partitionMask[0] = '\0';  
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
	
	for(int i=0; i< STAGER_OPTIONS; i++){
	  this->types.at(i) = auxTypes[i]; 
	}

	this->default_protocol = "rfio";
      
      }



      /* destructor */
      StagerRequestHelper::~StagerRequestHelper() throw()
      {
	delete stagerService;
	delete dbService;
	delete baseAddr;
      }





      /**********************/
      /* baseAddr settings */ 
      /********************/
      /*** inline void StagerRequestHelper::settingBaseAddress() ***/
      
     


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
	Cuuid_t subrequest_uuid;//if we manage to get the cuuid version, we update 
	char subrequest_uuid_as_string[CUUID_STRING_LEN+1];	

	if (subrequest->subreqId().empty()){/* we create a new Cuuid_t, copy to thread-safe variable, and update it on subrequest...*/
	  Cuuid_create(&subrequest_uuid);
	  this->subrequestUuid = subrequest_uuid;/* COPY TO THREAD-SAFE VARIABLE*/
	  
	  /* convert to the string version, needed to update the subrequest and fill it on DB*/ 
	  if(Cuuid2string(subrequest_uuid_as_string, CUUID_STRING_LEN+1, &subrequest_uuid) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper setSubrequestUuid) error  on Cuuid2string"<<std::endl;
	    throw ex;
	  }else{
	    subrequest_uuid_as_string[CUUID_STRING_LEN] = '\0';
	    /* update on subrequest */
	    subrequest->setSubreqId(subrequest_uuid_as_string);
	    /* update it in DB*/
	    try{
	      dbService->updateRep(baseAddr, subrequest, true);	/*207*/	
 	    }catch(castor::exception::Exception ex){
	      castor::exception::Exception e(SEINTERNAL);
	      e.getMessage()<< "(StagerRequestHelper setSubrequestUuid) dbService->updateRep throws an exception"<<std::endl;
	      throw e;
	    }
	  }
	}else{ /* translate to the Cuuid_t version and copy to our thread-safe variable */
	  if( string2Cuuid(&subrequest_uuid, (char *) subrequest_uuid_as_string)!=0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper setSubrequestUuid) error  on string2Cuuid"<<std::endl;
	    throw ex;
	  }else{
	    this->subrequestUuid = subrequest_uuid; /* COPY TO THREAD-SAFE VARIABLE*/ 
	  }
	}
      }
      
      /* get request uuid (we cannon' t create it!) */ 
      void StagerRequestHelper::setRequestUuid() throw(castor::exception::Exception)
      {
	Cuuid_t request_uuid;

	if(fileRequest->reqId().empty()){/* we cannon' t generate one!*/
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper setRequestUuid) Request has no uuid"<<std::endl;
	  throw ex;	  
	}else{/*convert to the Cuuid_t version and copy in our thread safe variable */
	   
	  //char* uuid_as_string = fileRequest->reqId().c_str();
	  if (string2Cuuid(&request_uuid,(char*) fileRequest->reqId().c_str()) != 0) {
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerRequestHelper setRequestUuid) error (but no exception) on string2Cuuid"<<std::endl;
	    throw ex;
	  }else{
	    this->requestUuid = request_uuid;
	  }
	}

      }





      /*******************************************************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
      /*****************************************************************************************************************************************/
      
      void StagerRequestHelper::linkCastorFileToServiceClass(StagerCnsHelper stgCnsHelper) throw(castor::exception::Exception)
      {
	u_signed64 fileClassId = fileClass->id();
	u_signed64 svcClassId = svcClass->id();
	
	/* get the castorFile from the stagerService and fill it on the subrequest */
	/* we use the stagerService.selectCastorFile to get it from DB */

	try{
	  castorFile = stagerService->selectCastorFile(stgCnsHelper.cnsFileid.fileid, stgCnsHelper.cnsFileid.server,svcClassId, fileClassId, stgCnsHelper.cnsFilestat.filesize,subrequest->fileName());

	}catch(castor::exception::Exception ex){/* stagerService throw exception */
	  castor::exception::Exception e(SEINTERNAL);
	  e.getMessage()<<"(StagerRequestHelper linkCastorFileToSvcClass) stagerService->selectCastorFile throws an exception"<<std::endl;
	  throw e;
	}

	if(this->castorFile == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper linkCastorFileToServiceClass) Impossible to get the castorFile"<<std::endl;
	  throw ex; 
	}

	subrequest->setCastorFile(castorFile);
	try{
	  /* create the subrequest-> castorFile on DB*/
	  dbService->fillRep(baseAddr, subrequest, OBJ_CastorFile, true);//throw exception
	  /* create the castorFile-> svcClass link on DB */
	  dbService->fillObj(baseAddr, castorFile, OBJ_SvcClass, 0);//throw exception	

	}catch(castor::exception::Exception ex){/* stagerService throw exception */
	  castor::exception::Exception e(SEINTERNAL);
	  e.getMessage()<<"(StagerRequestHelper linkCastorFileToSvcClass) dbService->fill___  throws an exception"<<std::endl;
	  throw e;
	}


      }


      /****************************************************************************************************/
      /*  check if the castorFile is linked to the service Class: called in StagerRequest.jobOriented()*/
      /**************************************************************************************************/
      /*** inline void StagerRequestHelper::isCastorFileLinkedToSvcClass() throw(castor::exception::Exception) ***/
      



      
      /****************************************************************************************************/
      /*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
      /**************************************************************************************************/
      /***  inline std::string StagerRequestHelper::getPartitionMask() throw(castor::exception::Exception)  ***/
      



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
      /***  inline bool StagerRequestHelper::checkFilePermission() throw(castor::exception::Exception);  ***/
     


      /*****************************************************************************************************/
      /* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
      /* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
      /**************************************************************************************************/
      void StagerRequestHelper::buildRmJobHelperPart(struct rmjob* rmjob) throw(castor::exception::Exception)//after processReplica (if it is necessary)
      {


	rmjob->uid = (uid_t) fileRequest->euid();
	rmjob->gid = (uid_t) fileRequest->egid();
	
	strncpy(rmjob->uname, username.c_str(),RM_MAXUSRNAMELEN);
	rmjob->uname[RM_MAXUSRNAMELEN] = '\0';
	strncpy(rmjob->gname, groupname.c_str(),RM_MAXGRPNAMELEN);
	rmjob->gname[RM_MAXGRPNAMELEN] = '\0';

	
	strncpy(rmjob->partitionmask, getPartitionMask().c_str(),RM_MAXPARTITIONLEN);
	rmjob->partitionmask[RM_MAXPARTITIONLEN] = '\0';

	std::string features;

	std::string className("default");
	if((fileRequest->svcClassName().empty()) == false){
	  className = fileRequest->svcClassName();//620
	}
	features = className;//622
	std::string protocol = this->subrequest->protocol();
	if((protocol.empty()) == false){
	  if((features.size()+1+protocol.size())<RM_MAXFEATURELEN){
	    features+= ":";
	    features+= protocol;
	    
	  }
	}else{
	  if((features.size()+1+ this->default_protocol.size())<RM_MAXFEATURELEN){
	    features+=":"+this->default_protocol;
	  }
	}

	if(features.size() > RM_MAXFEATURELEN){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerRequestHelper buildRmJobRequestPart) String features exceeds the max lenght"<<std::endl;
	  throw ex;
	}
	strncpy(rmjob->rfeatures, features.c_str(), RM_MAXFEATURELEN);
	               	
	u64tostr(subrequest->id(),rmjob->stageid,0);
	strcat(rmjob->stageid,"@");
	
	//adding request and subrequest uuid:
	{
	  Cuuid_t thisuuid=requestUuid;
	  Cuuid2string(rmjob->requestid, CUUID_STRING_LEN+1,&thisuuid);
	  thisuuid=subrequestUuid;
	  Cuuid2string(rmjob->subrequestid, CUUID_STRING_LEN+1,&thisuuid);
	}
	
	strcpy(rmjob->clientStruct,iClientAsString.c_str());//what is the max len for client? 663
	strcpy(rmjob->exec,"/usr/bin/stagerJob.sh");
	
	strncpy(rmjob->account,className.c_str(),RM_MAXACCOUNTNAMELEN);
	

      }



     

      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
