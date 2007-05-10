/************************************************************************/
/* container for the c methods and structures related with the cns_api */
/**********************************************************************/

#include "Cns_api.h"
#include "castor/IObject.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/stager_uuid.h"


namespace castor{
  namespace stager{
    namespace dbService{


      StagerCnsHelper::StagerCnsHelper() throw(){
	///
      }
      StagerCnsHelper::~StagerCnsHelper() throw(){
      }


      /*******************/
      /* Cns structures */
      /*****************/ 

      /*since we are gonna use dlf: we won' t probably need it*/
      /* get the fileid pointer to print (since we are gonna use dlf: we won' t probably need it  */
      void StagerCnsHelper::createCnsFileidPointer() throw() 
      {
	////to implement!
      }

     

      /* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
      /* for a subrequest.filename */
      int StagerCnsHelper::createCnsFileIdAndStat_setFileExist(char* subrequestFileName) throw()/* update fileExist*/
      {
	  memset(&cnsFileid, '\0', sizeof(cnsFileid)); /* reset cnsFileid structure  */
	  this->fileExist = (0 == Cns_statx( subrequestFileName,&cnsFileid,&cnsFilestat));
	
	  return(this->fileExist);
      }



      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      void StagerCnsHelper::getCnsFileclass() throw()
      {	
	  Cns_queryclass(fileid.server,cnsFilestat.fileclass, NULL, &cnsFileclass);
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
	if (Cns_creatx(filename, mode, &cnsFileid) != 0) {
	  //throw exception
	}
	if (Cns_statx(filename,&cnsFileid,&cnsFilestat) != 0) {
	  //throw exception
	}
      }
    
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
