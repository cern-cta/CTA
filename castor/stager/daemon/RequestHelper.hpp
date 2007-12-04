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
#include <sys/time.h>

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

	/* service class */
	std::string svcClassName;
	castor::stager::SvcClass* svcClass;
		
	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;
	
	std::string username;
	std::string groupname;

	/* Cuuid_t thread safe variables */ 
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;
	
	std::string default_protocol;

  timeval tvStart;

	/****************************************************/
	/*  called on the different thread (job, pre, stg) */
	StagerRequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest) throw(castor::exception::Exception);

	/* destructor */
	~StagerRequestHelper() throw();


	/****************************************************************************************/
	/* get svClass by selecting with stagerService                                         */
	/* (using the svcClassName:getting from request OR defaultName (!!update on request)) */
	/*************************************************************************************/
	void getSvcClass() throw(castor::exception::Exception);
	
   
	/*******************************************************************************/
	/* update request in DB, create and fill request->svcClass link on DB         */ 
	/*****************************************************************************/
	void linkRequestToSvcClassOnDB() throw(castor::exception::Exception);
      

     
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
  void getCastorFileFromSvcClass(castor::stager::dbService::StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
       

	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	void setUsernameAndGroupname() throw(castor::exception::Exception);


	/* check if the user (euid,egid) has the right permission for the request's type
	   note that we were not checking the permissions for SetFileGCWeight and PutDone request (true),
	   now you need write permissions
	*/
	bool checkFilePermission() throw(castor::exception::Exception);
     
  /**
   * Logs a standard message to DLF including all needed info (e.g. filename, svcClass, etc.)
   * @param level the DLF logging level
   * @param messageNb the message number as defined in StagerDlfMessages.hpp
   * @param fid the fileId structure if needed
   */
  void logToDlf(int level, int messageNb, Cns_fileid* fid = 0) throw();
	

      }; //end StagerRequestHelper class
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
