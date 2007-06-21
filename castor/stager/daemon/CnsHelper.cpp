/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "StagerCnsHelper.hpp"



#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/Cns_struct.h"
#include "../../../h/Cglobals.h"
#include "../../../h/rm_api.h"
#include "../../../h/rm_struct.h"

#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/u64subr.h"
#include "../../../h/osdep.h"

#include "../../exception/Exception.hpp"
#include "../../../h/serrno.h"
#include "../../Constants.hpp"


#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>





namespace castor{
  namespace stager{
    namespace dbService{


      StagerCnsHelper::StagerCnsHelper() throw(castor::exception::Exception){
	/* set the initial value of our static variable */
	this->fileid_ts_key = -1;
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
