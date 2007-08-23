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
	
	struct Cns_fileid *fileid;
	struct Cns_fileid cnsFileid;
	struct Cns_filestat cnsFilestat;
	struct Cns_fileclass cnsFileclass;
	
	std::string subrequestFileName;
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
	inline void setSubrequestFileName(std::string subReqFileName){
	  this->subrequestFileName = subReqFileName;
	}
	
	

	/* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
	inline void getCnsFileclass() throw(castor::exception::Exception){
	  memset(&(this->cnsFileclass),0, sizeof(cnsFileclass));
	  if( Cns_queryclass((this->cnsFileid.server),(this->cnsFilestat.fileclass), NULL, &(this->cnsFileclass)) != 0 ){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper getFileclass) Error on Cns_setid"<<std::endl;
	    throw ex;
	  }
	}
      
	/***************************************************************/
	/* set the user and group id needed to call the Cns functions */
	/*************************************************************/
	inline void cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest){
	  this->euid = fileRequest->euid();
	  this->egid = fileRequest->egid();
	}

	/************************************************************************************/
	/* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
	/**********************************************************************************/
	inline void cnsSettings(mode_t mask) throw(castor::exception::Exception){
	  if (Cns_setid(euid,egid) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper cnsSettings) Error on Cns_setid"<<std::endl;
	    throw ex;
	  }
	  
	  if (Cns_umask(mask) < 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper cnsSettings) Error on Cns_umask"<<std::endl;
	    throw ex;
	  }
	}

	
	/****************************************************************************************************************/
	/* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
	/* create the file if it is needed/possible */
	/**************************************************************************************************************/
	bool checkAndSetFileOnNameServer(int type, int subrequestFlags, int modeBits, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception);


	/******************************************************************************/
	/* return toCreate= true when type = put/prepareToPut/update/prepareToUpdate */
	/****************************************************************************/
	inline bool isFileToCreateOrException(int type, int subRequestFlags) throw(castor::exception::Exception){
	  bool toCreate = false;
	  
	  
	  if ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type)||((O_CREAT == (subRequestFlags & O_CREAT))&&((OBJ_StageUpdateRequest == type) ||(OBJ_StagePrepareToUpdateRequest == type)))) {
	    
	    toCreate = true;
	  }else if((OBJ_StageGetRequest == type) || (OBJ_StagePrepareToGetRequest == type) ||(OBJ_StageRepackRequest == type) ||(OBJ_StageUpdateRequest == type) ||                          (OBJ_StagePrepareToUpdateRequest == type)||(OBJ_StageRmRequest == type) ||(OBJ_SetFileGCWeight == type) ||(OBJ_StagePutDoneRequest == type)){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper isFileToCreateOrException) Asking for a file which doesn't exist"<<std::endl;
	    throw ex;
	  }
	  
	  return(toCreate);

	}
	


      }; // end StagerCnsHelper class
      




    }//end namespace dbService
  }// end namespace stager
}// end namespace castor


#endif

