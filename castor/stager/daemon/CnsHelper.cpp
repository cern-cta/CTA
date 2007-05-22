/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "../../../h/Cns_api.h"
#include "../../../h/Cglobals.h"
#include "../../IObject.hpp"
#include "StagerCnsHelper.hpp"
#include "../../../stager/stager_uuid.h"
#include "../../../h/dlf_api.h"
#include "../../../dlf/Message.hpp"
#include "../../../dlf/Param.hpp"

namespace castor{
  namespace stager{
    namespace dbService{


      StagerCnsHelper::StagerCnsHelper() throw(){
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
      void StagerCnsHelper::getFileid() throw() 
      {
	Cns_fileid *var;
	Cglobals_get(&fileid_ts_key,(void**) &var, sizeof(struct Cns_fileid));
	if(var == NULL){
	  this->fileid = this->fileid_ts_static;
	  castor::dlf::Param param[]={castor::dlf::Param("Standard message", "(Cglobals_get) Impossible to get the fileid needed to login")};
	  castor::dlf::dlf_writep(this->fileid, DLF_LVL_SYSTEM,3,1,param);
	}else{
	  this->fileid = var;
	}
      }

     

      /* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
      /* for a subrequest.filename */
      int StagerCnsHelper::createCnsFileIdAndStat_setFileExist(char* subrequestFileName) throw()/* update fileExist*/
      {
	  memset(&(this->cnsFileid), '\0', sizeof(this->cnsFileid)); /* reset cnsFileid structure  */
	  this->fileExist = (0 == Cns_statx(subrequestFileName,&(this->cnsFileid),&(this->cnsFilestat)));
	
	  return(this->fileExist);
      }



      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      void StagerCnsHelper::getCnsFileclass() throw()
      {	
	  Cns_queryclass((this->fileid.server),(this->cnsFilestat.fileclass), NULL, &(this->cnsFileclass));
	  //check if cnsFileclass is NULL
      }
     


      
      
      /************************************************************************************/
      /* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
      /**********************************************************************************/
      void StagerCnsHelper::cnsSettings(uid_t euid, uid_t egid, char* mask) throw()
      {
	Cns_setid(euid,egid);
	
	Cns_umask((mode_t) mask);
      }




      /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
      void StagerCnsHelper::createFileAndUpdateCns(char* filename, mode_t mode) throw()
      {
	if (Cns_creatx(filename, mode, &(this->cnsFileid)) != 0) {
	  //throw exception
	}
	if (Cns_statx(filename,&(this->cnsFileid),&(this->cnsFilestat)) != 0) {
	  //throw exception
	}
      }
    
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
