/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/SubRequest.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "Cns_struct.h"
#include "Cglobals.h"


#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "osdep.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/Constants.hpp"


#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>





namespace castor{
  namespace stager{
    namespace dbService{


      StagerCnsHelper::StagerCnsHelper() throw(castor::exception::Exception){
      }

      StagerCnsHelper::~StagerCnsHelper() throw(){
	//
      }


      /*******************/
      /* Cns structures */
      /*****************/ 

      /****************************************************************************************/
      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      void StagerCnsHelper::getCnsFileclass() throw(castor::exception::Exception){
	memset(&(cnsFileclass),0, sizeof(cnsFileclass));
	if( Cns_queryclass((cnsFileid.server),(cnsFilestat.fileclass), NULL, &(cnsFileclass)) != 0 ){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerCnsHelper getFileclass) Error on Cns_setid"<<std::endl;
	  throw ex;
	  }
	}
      
      /***************************************************************/
      /* set the user and group id needed to call the Cns functions */
      /*************************************************************/
      void StagerCnsHelper::cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest){
	euid = fileRequest->euid();
	egid = fileRequest->egid();
      }

      
      /************************************************************************************/
      /* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
      /**********************************************************************************/
      void StagerCnsHelper::cnsSettings(mode_t mask) throw(castor::exception::Exception){
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
      bool StagerCnsHelper::checkAndSetFileOnNameServer(std::string fileName, int type, int subrequestFlags, int modeBits, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception){

	try{
	  /* check if the required file exists */
	  memset(&(cnsFileid), '\0', sizeof(cnsFileid)); /* reset cnsFileid structure  */
	  fileExist = (0 == Cns_statx(fileName.c_str(),&(cnsFileid),&(cnsFilestat)));
	  
	 
	  if(!fileExist){
	    if(isFileToCreateOrException(type, subrequestFlags)){/* create the file is the type is like put, ...*/
	      if(serrno == ENOENT){
	     
		mode_t mode = (mode_t) modeBits;
		
		/* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
		memset(&(cnsFileid),0, sizeof(cnsFileid));
		
		if (Cns_creatx(fileName.c_str(), mode, &(cnsFileid)) != 0) {
		  castor::exception::Exception ex(serrno);
		  throw ex;
		}
		
		/* in case of Disk1 pool, we want to force the fileClass of the file */
		if(svcClass->hasDiskOnlyBehavior()){
		  std::string forcedFileClassName = svcClass->forcedFileClass();
		  
		  if(!forcedFileClassName.empty()){
		    
		    if(Cns_unsetid() != 0){ /* coming from the latest stager_db_service.c */
		      //throw(castor::exception::Exception ex(serrno));
		    }
		    if(Cns_chclass(fileName.c_str(), 0, (char*)forcedFileClassName.c_str())){
		      int tempserrno = serrno;
		      Cns_delete(fileName.c_str());
		      serrno = tempserrno;
		      castor::exception::Exception ex(serrno);
		      throw ex;
		    }
		    if(Cns_setid(euid, egid) != 0){
		      castor::exception::Exception ex(serrno);
		      throw ex;
		    }		  
		  }/* "only force the fileClass if one is given" */
		  
		}/* end of "if(hasDiskOnly Behavior)" */
		
		Cns_statx(fileName.c_str(),&(cnsFileid),&(cnsFilestat));
		/* we get the Cuuid_t fileid (needed to logging in dlf)  */
	      }
	    }
	  }
	  return(fileExist);
	}catch(castor::exception::Exception e){
	  castor::dlf::Param params[]={ castor::dlf::Param("Error while checking the file on the nameServer", "Handler level")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	    
	  e.getMessage()<< "(StagerCnsHelper checkAndSetFileOnNameServer)"<<std::endl;
	  throw e;    

	}

      }
      

      /******************************************************************************/
      /* return toCreate= true when type = put/prepareToPut/update/prepareToUpdate */
      /****************************************************************************/
      bool StagerCnsHelper::isFileToCreateOrException(int type, int subRequestFlags) throw(castor::exception::Exception){
	bool toCreate = false;
	
	
	if ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type)||((O_CREAT == (subRequestFlags & O_CREAT))&&((OBJ_StageUpdateRequest == type) ||(OBJ_StagePrepareToUpdateRequest == type)))) {
	    
	  toCreate = true;
	}else if((OBJ_StageGetRequest == type) || (OBJ_StagePrepareToGetRequest == type) ||(OBJ_StageRepackRequest == type) ||(OBJ_StageUpdateRequest == type) ||                          (OBJ_StagePrepareToUpdateRequest == type)||(OBJ_StageRmRequest == type) ||(OBJ_SetFileGCWeight == type) ||(OBJ_StagePutDoneRequest == type)){
	  
	  castor::dlf::Param params[]={ castor::dlf::Param("Function: isFileToCreateOrException", "Handler level")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 1 ,params);	   
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerCnsHelper isFileToCreateOrException) "<<std::endl;
	  throw ex;
	}
	return(toCreate);
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
