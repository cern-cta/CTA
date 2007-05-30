/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (StagerDBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#ifndef STAGER_REQUEST_HELPER_HPP
#define STAGER_REQUEST_HELPER_HPP 1


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
#include "../../../h/Cpwd.h"
#include "../../..//h/Cgrp.h"
#include "../../../h/u64subr.h"

#include "../../IClientFactory.h"

#include "../../exception/Exception.hpp"
#include "../../exception/Internal.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      
      class castor::stager::IStagerSvc;
      class castor::Services;
      class castor::BaseAddress;
      class castor::stager::SubRequest;
      class castor::stager::FileRequest;
      class castor::stager::IClient;
      class castor::stager::SvcClass;
      class castor::stager::CastorFile;
      class castor::stager::FileClass;
      

      class StagerRequestHelper : public::castor::IObject{



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
	
	char partitionMask[RM_MAXPARTITIONLEN+1];/* initialized to svcClass.name on stagerRequest.jobOriented ()*/
	
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	/* get from the stagerService using as key Cnsfileclass.name (JOB ORIENTED)*/
	castor::stager::FileClass* fileClass;
       
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
	StagerRequestHelper() throw(castor::exception::Internal);
	~StagerRequestHelper() throw();
	

	
	/**********************/
	/* baseAddr settings */ 
	/********************/
	inline void StagerRequestHelper::settingBaseAddress();
	


	/**************************************************************************************/
	/* get/create subrequest->  fileRequest, and many subrequest's attributes            */
	/************************************************************************************/
	
	/* get subrequest using stagerService (and also types) */
	inline void StagerRequestHelper::getSubrequest() throw(castor::exception::Internal);
       
	/* get the link (fillObject~SELECT) between the subrequest and its associated fileRequest  */ 
	/* using dbService, and get the fileRequest */ 
	inline void StagerRequestHelper::getFileRequest() throw(castor::exception::Internal);
      
    


	/***********************************************************************************/
	/* get the link (fillObject~SELECT) between fileRequest and its associated client */
	/* using dbService, and get the client                                           */
	/********************************************************************************/
	inline void StagerRequestHelper::getIClient() throw(castor::exception::Internal);
	
	
	
	/****************************************************************************************/
	/* get svClass by selecting with stagerService                                         */
	/* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
	/*************************************************************************************/
	inline void StagerRequestHelper::getSvcClass() throw(castor::exception::Internal);
     
      
      
	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	inline void StagerRequestHelper::linkRequestToSvcClassOnDB() throw(castor::exception::Internal);
     
      

	/*******************************************************************************/
	/* get the castoFile associated to the subrequest                             */ 
	/*****************************************************************************/
	inline void StagerRequestHelper::getCastorFile() throw(castor::exception::Internal);
      
      
      
	/****************************************************************************************/
	/* get fileClass by selecting with stagerService                                       */
	/* using the CnsfileClass.name as key      (in StagerRequest.JobOriented)             */
	/*************************************************************************************/
	inline void StagerRequestHelper::getFileClass(char* nameClass) throw(castor::exception::Internal);
      
      
	/******************************************************************************************/
	/* get and (create or initialize) Cuuid_t subrequest and request                         */
	/* and copy to the thread-safe variables (subrequestUuid and requestUuid)               */
	/***************************************************************************************/

	/* get or create subrequest uuid */
	void StagerRequestHelper::setSubrequestUuid() throw(castor::exception::Internal);
	
      
	/* get request uuid (we cannon' t create it!) */ 
	void StagerRequestHelper::setRequestUuid();
      




	/*******************************************************************************************************************************************/
	/*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in StagerRequest.jobOriented()*/
	/*****************************************************************************************************************************************/
       	inline void StagerRequestHelper::linkCastorFileToServiceClass(StagerCnsHelper stgCnsHelper) throw(castor::exception::Internal);
     



	/****************************************************************************************************/
	/*  check if the castorFile is linked to the service Class: called in StagerRequest.jobOriented()*/
	/**************************************************************************************************/
	inline void StagerRequestHelper::isCastorFileLinkedToSvcClass() throw(castor::exception::Internal);
     


      
	/****************************************************************************************************/
	/*  initialize the partition mask with svcClass.name()  or get it:called in StagerRequest.jobOriented() */
	/**************************************************************************************************/
	inline std::string StagerRequestHelper::getPartitionMask() throw(castor::exception::Internal);
     

	/****************************************************************************************************/
	/*  fill castorFile's FileClass: called in StagerRequest.jobOriented()                             */
	/**************************************************************************************************/
	inline void StagerRequestHelper::setFileClassOnCastorFile() throw(castor::exception::Internal);
     


	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	inline void StagerRequestHelper::setUsernameAndGroupname() throw(castor::exception::Internal);
      


     

	
	/****************************************************************************************************/
	/* depending on fileExist and type, check the file needed is to be created or throw exception    */
	/********************************************************************************************/
	inline bool StagerRequestHelper::isFileToCreateOrException(bool fileExist) throw(castor::exception::Internal);
     


	/*****************************************************************************************************/
	/* check if the user (euid,egid) has the ritght permission for the request's type                   */
	/* note that we don' t check the permissions for SetFileGCWeight and PutDone request (true)        */
	/**************************************************************************************************/
	inline bool StagerRequestHelper::checkFilePermission() throw(castor::exception::Internal);
     


	/*****************************************************************************************************/
	/* build the struct rmjob necessary to submit the job on rm : rm_enterjob                           */
	/* called on each request thread (not including PrepareToPut,PrepareToUpdate,Rm,SetFileGCWeight)   */
	/**************************************************************************************************/
	struct rmjob StagerRequestHelper::buildRmJobHelperPart(struct rmjob &rmjob) throw(castor::exception::Internal);//after processReplica (if it is necessary)
	
	
	
     

	
	

      }//end StagerRequestHelper class
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
