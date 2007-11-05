/************************************************************************/
/* container for the c methods and structures related with the cns_api */
/**********************************************************************/

#ifndef STAGER_CNS_HELPER_HPP
#define STAGER_CNS_HELPER_HPP 1

#include <sys/types.h>
#include <sys/stat.h>
#include "Cns_api.h"
#include "Cns_struct.h"
#include "Cglobals.h"
#include "serrno.h"

#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/FileRequest.hpp"



#include "castor/Constants.hpp"

#include "castor/ObjectSet.hpp"


#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include <fcntl.h>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerCnsHelper : public virtual castor::BaseObject{
	
      public:
	
	struct Cns_fileid cnsFileid;
	struct Cns_filestat cnsFilestat;
	struct Cns_fileclass cnsFileclass;
	
	uid_t euid;/* stgRequestHelper->fileRequest->euid() */ 
	uid_t egid;/* stgRequestHelper->fileRequest->egid() */
	int fileExist;	


	
	
	StagerCnsHelper() throw(castor::exception::Exception);
	~StagerCnsHelper() throw();

	



	/*******************/
	/* Cns structures */
	/*****************/ 

	/****************************************************************************/
	/* set the subrequestFileName from stgRequestHelper->subrequest->fileName()*/
	void setSubrequestFileName(std::string subReqFileName);
	
	
	/****************************************************************************************/
	/* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
	void getCnsFileclass() throw(castor::exception::Exception);
      
	/***************************************************************/
	/* set the user and group id needed to call the Cns functions */
	/*************************************************************/
	void cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest) throw(castor::exception::Exception);
      

	/****************************************************************************************************************/
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	/**************************************************************************************************************/
	bool checkAndSetFileOnNameServer(std::string fileName, int type, int subrequestFlags, mode_t modeBits, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception);


	/******************************************************************************/
	/* return toCreate= true when type = put/prepareToPut/update/prepareToUpdate */
	/****************************************************************************/
	bool isFileToCreateOrException(int type, int subRequestFlags) throw(castor::exception::Exception);
	


      }; // end StagerCnsHelper class
    }//end namespace dbService
  }// end namespace stager
}// end namespace castor


#endif

