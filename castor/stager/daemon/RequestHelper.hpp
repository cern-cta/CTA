/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/Subrequest.hpp"
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
#include "castor/IClientFactory.h"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

    

      class StagerRequestHelper :public::castor::IObject{



      public:
	  	
	/* services needed: database and stager services*/
	castor::stager::IStagerSvc* stagerService;
	castor::Services* dbService;
    

	/* BaseAddress */
	castor::BaseAddress* baseAddr;
	

	/* subrequest and fileRequest  */
	castor::stager::Subrequest* subrequest;
	
	castor::stager::FileRequest* fileRequest;

	/* client associated to the request */
	castor::IClient* iClient;
	std::string iClientAsString;
	
	/* service class */
	castor::stager::SvcClass* svcClass;
	bool svcClass_to_free;
	char partitionMask[RM_MAXPARTITIONLEN+1];/* initialized to svcClass.name on stagerRequest.jobOriented ()*/
	
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	/* get from the stagerService using as key Cnsfileclass.name (JOB ORIENTED)*/
	castor::stager::FileClass* fileclass;
       
	std::string username;
	//[RM_MAXUSRNAMELEN+1];
	std::string groupname;
	//[RM_MAXGRPNAMELEN+1];

	/* Cuuid_t thread safe variables */ 
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;
	std::vector<ObjectsIds> types;

	/* list of flags for the object creation */
	


	/****************/
	/* constructor */
	/* destructor */
	StagerRequestHelper();
	~StagerRequestHelper();
	

	
	/**********************/
	/* baseAddr settings */ 
	/********************/
	inline void StagerRequestHelper::settingBaseAddress();
	


	/**************************************************************************************/
	/* get/create subrequest->  fileRequest, and many subrequest's attributes            */
	/************************************************************************************/
	
	/* get subrequest using stagerService (and also types) */
	inline void StagerRequestHelper::getSubrequest() throw();
       
	/* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
	/* using dbService, and get the fileRequest */ 
	inline void StagerRequestHelper::getFileRequest() throw();
      
    


	/***********************************************************************************/
	/* get the link (fillObject~SELECT) between fileRequest and its associated client */
	/* using dbService, and get the client                                           */
	/********************************************************************************/
	inline void StagerRequestHelper::getIClient() throw();
	
	
	
	/****************************************************************************************/
	/* get svClass by selecting with stagerService                                         */
	/* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
	/*************************************************************************************/
	inline void StagerRequestHelper::getSvcClass() throw();
     
      
      
	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	inline void StagerRequestHelper::linkRequestToSvcClassOnDB() throw();
     
      

	/*******************************************************************************/
	/* get the castoFile associated to the subrequest                             */ 
	/*****************************************************************************/
	inline void StagerRequestHelper::getCastorFile() throw();
      
      
      
	/****************************************************************************************/
	/* get fileClass by selecting with stagerService                                       */
	/* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
	/*************************************************************************************/
	inline void StagerRequestHelper::getFileClass(char* nameClass) throw();
      
      
	/******************************************************************************************/
	/* get and (create or initialize) Cuuid_t subrequest and request                         */
	/* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
	/***************************************************************************************/

	/* get or create subrequest uuid */
	void StagerRequestHelper::setSubrequestUuid() throw();
	
      
	/* get request uuid (we cannon' t create it!) */ 
	void StagerRequestHelper::setRequestUuid() throw();
      




	/*******************************************************************************************************************************************/
	/*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
	/*****************************************************************************************************************************************/
       	inline void StagerRequestHelper::linkCastorFileToServiceClass(StagerCnsHelper stgCnsHelper) throw();
     



	/****************************************************************************************************/
	/*  check if the castorFile is linked to the service Class: called in StagerRequest.jobOriented()*/
	/**************************************************************************************************/
	inline void StagerRequestHelper::isCastorFileLinkedToSvcClass() throw();
     


      
	/****************************************************************************************************/
	/*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
	/**************************************************************************************************/
	inline std::string StagerRequestHelper::getPartitionMask() throw();
     

	/****************************************************************************************************/
	/*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	/**************************************************************************************************/
	inline void StagerRequestHelper::setFileClassOnCastorFile() throw();
     


	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	inline void StagerRequestHelper::setUsernameAndGroupname() throw();
      


     

	
	/****************************************************************************************************/
	/* depending on fileExist and type, check the file needed is to be created or throw exception    */
	/********************************************************************************************/
	inline bool StagerRequestHelper::isFileToCreateOrException(bool fileExist) throw();
     


	/*****************************************************************************************************/
	/* check if the user (euid,egid) has the ritght permission for the request's type                   */
	/* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
	/**************************************************************************************************/
	inline bool StagerRequestHelper::checkFilePermission() throw();
     


	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	struct rmjob StagerRequestHelper::buildRmJobHelperPart(struct rmjob &rmjob) throw();//after processReplica (if it is necessary)
	
	
	
     

	
	

      }//end StagerRequestHelper class
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
