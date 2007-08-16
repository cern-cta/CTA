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

#include "castor/ObjectSet.hpp"

#include <iostream>
#include <string>
#include <errno.h>
#include "serrno.h"
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
	
	/*since we are gonna use dlf: we won' t probably need it*/
	/* get the fileid pointer to print (since we are gonna use dlf: we won' t probably need it  */
	inline void getFileid(){
	  /* static variables needed to get the fileid for logging */
	  static int fileid_ts_key = -1;
	  static struct Cns_fileid fileid_ts_static;
	  Cns_fileid *var;
	  Cglobals_get(&fileid_ts_key,(void**) &var, sizeof(struct Cns_fileid));
	  if(var == NULL){
	    this->fileid =&(fileid_ts_static);
	  }else{
	    this->fileid = var;
	  }
	}
	
	/* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
	/* for a subrequest.filename *//* update fileExist*/
	inline int createCnsFileIdAndStat_setFileExist(){

	  memset(&(this->cnsFileid), '\0', sizeof(this->cnsFileid)); /* reset cnsFileid structure  */
	  this->fileExist = (0 == Cns_statx(this->subrequestFileName.c_str(),&(this->cnsFileid),&(this->cnsFilestat)));
	  
	  return(this->fileExist);
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

	/***************************************************************************************************************/
	/* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
	/*************************************************************************************************************/
	inline void createFileAndUpdateCns( mode_t mode, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception){
	  if(serrno == ENOENT){
	    memset(&(this->cnsFileid),0, sizeof(cnsFileid));
	    if (Cns_creatx(this->subrequestFileName.c_str(), mode, &(this->cnsFileid)) != 0) {
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_creatx"<<std::endl;
	      throw ex;
	    }
	    
	    /* in case of Disk1 pool, we want to force the fileClass of the file */
	    if(svcClass->hasDiskOnlyBehavior() == true){
	      std::string forcedFileClassName = svcClass->forcedFileClass();

	      if(Cns_unsetid() != 0){ /* coming from the latest stager_db_service.c */
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_unsetid"<<std::endl;
		throw ex;
	      }
	      if(Cns_chclass(this->subrequestFileName.c_str(), 0, (char*)forcedFileClassName.c_str())){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_chclass"<<std::endl;
		throw ex;
	      }
	      if(Cns_setid(euid, egid) != 0){
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_setid"<<std::endl;
		throw ex;
	      }
	    }/* end of "if(hasDiskOnly Behavior)" */

	    memset(&(this->cnsFileclass),0, sizeof(cnsFileclass));
	    if (Cns_statx(subrequestFileName.c_str(),&(this->cnsFileid),&(this->cnsFilestat)) != 0) {
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_statx"<<std::endl;
	      throw ex;
	    }
	  }else{
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_statx (serrno != ENOENT)"<<std::endl;
	    throw ex; 
	  }
	}
     


      }; // end StagerCnsHelper class
      




    }//end namespace dbService
  }// end namespace stager
}// end namespace castor


#endif

