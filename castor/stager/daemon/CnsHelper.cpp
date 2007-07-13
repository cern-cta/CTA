/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "castor/stager/dbService/StagerCnsHelper.hpp"



#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "Cns_struct.h"
#include "Cglobals.h"
#include "rm_api.h"
#include "rm_struct.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "osdep.h"

#include "castor/exception/Exception.hpp"
#include "serrno.h"
#include "castor/Constants.hpp"


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
     




      /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
      /*** inline void StagerCnsHelper::createFileAndUpdateCns(const char* filename, mode_t mode) throw(castor::exception::Internal) ***/
     
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
