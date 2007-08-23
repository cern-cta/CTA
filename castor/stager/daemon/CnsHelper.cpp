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

      /* get the fileid pointer to logging on dlf */
      /*** inline void StagerCnsHelper::getFileid() ***/
      

     

      /* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
      /* for a subrequest.filename */
      /*** inline  int StagerCnsHelper::createCnsFileIdAndStat_setFileExist(const char* subrequestFileName) ***/
      


      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      /*** inline void StagerCnsHelper::getCnsFileclass() throw(castor::exception::Internal) ***/
      
     


      
      
      /************************************************************************************/
      /* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
      /**********************************************************************************/
      /*** inline  void StagerCnsHelper::cnsSettings(uid_t euid, uid_t egid, mode_t mask) throw(castor::exception::Internal) ***/
     


      /****************************************************************************************************************/
      /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
      /* create the file if it is needed/possible */
      /**************************************************************************************************************/
      bool StagerCnsHelper::checkAndSetFileOnNameServer(int type, int subrequestFlags, int modeBits, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception){

	bool fileExist;
	/* check if the required file exists */
	memset(&(this->cnsFileid), '\0', sizeof(this->cnsFileid)); /* reset cnsFileid structure  */
	this->fileExist = (0 == Cns_statx(this->subrequestFileName.c_str(),&(this->cnsFileid),&(this->cnsFilestat)));
	
	if(serrno == ENOENT){
	  if(!fileExist){
	    if(this->isFileToCreateOrException(type, subrequestFlags)){/* create the file is the type is like put, ...*/
	      
	      mode_t mode = (mode_t) modeBits;
	      
	      /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
	      memset(&(this->cnsFileid),0, sizeof(cnsFileid));
	      
	      if (Cns_creatx(this->subrequestFileName.c_str(), mode, &(this->cnsFileid)) != 0) {
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerCnsHelper checkAndSetFileOnNameServer) Error on Cns_creatx"<<std::endl;
		throw ex;
	      }
	      
	      /* in case of Disk1 pool, we want to force the fileClass of the file */
	      if(svcClass->hasDiskOnlyBehavior() == true){
		std::string forcedFileClassName = svcClass->forcedFileClass();
		
		if(Cns_unsetid() != 0){ /* coming from the latest stager_db_service.c */
		  castor::exception::Exception ex(SEINTERNAL);
		  ex.getMessage()<<"(StagerCnsHelper checkAndSetFileOnNameServer) Error on Cns_unsetid"<<std::endl;
		  throw ex;
		}
		if(Cns_chclass(this->subrequestFileName.c_str(), 0, (char*)forcedFileClassName.c_str())){
		  castor::exception::Exception ex(SEINTERNAL);
		  ex.getMessage()<<"(StagerCnsHelper checkAndSetFileOnNameServer) Error on Cns_chclass"<<std::endl;
		  throw ex;
		}
		if(Cns_setid(euid, egid) != 0){
		  castor::exception::Exception ex(SEINTERNAL);
		  ex.getMessage()<<"(StagerCnsHelper checkAndSetFileOnNameServer) Error on Cns_setid"<<std::endl;
		  throw ex;
		}
	      }/* end of "if(hasDiskOnly Behavior)" */
	      
	      
	      if (Cns_statx(subrequestFileName.c_str(),&(this->cnsFileid),&(this->cnsFilestat)) != 0) {
		castor::exception::Exception ex(SEINTERNAL);
		ex.getMessage()<<"(StagerCnsHelper checkAndSetFileOnNameServer) Error on Cns_statx"<<std::endl;
		throw ex;
	      }
	      
	      
	    }
	  }
	}
	return(fileExist);
      }
      

      /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
      /*** inline void StagerCnsHelper::createFileAndUpdateCns(const char* filename, mode_t mode) throw(castor::exception::Internal) ***/
     


      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
