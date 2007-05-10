/************************************************************************/
/* container for the c methods and structures related with the cns_api */
/**********************************************************************/

#ifndef STAGER_CNS_HELPER_HPP
#define STAGER_CNS_HELPER_HPP 1

#include "Cns_api.h"
#include "castor/IObject.hpp"



namespace castor{
  namespace stager{
    namespace dbService{

      
     
      class StagerCnsHelper :public::castor::IObject{
	
      public:
	
	struct Cns_fileid *fileid;
	struct Cns_fileid cnsFileid;
	struct Cns_filestat cnsFilestat;
	struct Cns_fileclass cnsFileclass;
	int fileExist;	
	

	
	StagerCnsHelper();
	~StagerCnsHelper();

	

	/*******************/
	/* Cns structures */
	/*****************/ 
	
	/*since we are gonna use dlf: we won' t probably need it*/
	/* get the fileid pointer to print (since we are gonna use dlf: we won' t probably need it  */
	inline void StagerCnsHelper::createCnsFileidPointer() throw(); 
	
	/* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
	/* for a subrequest.filename */
	inline int StagerCnsHelper::createCnsFileIdAndStat_setFileExist(char* subrequestFileName)throw();/* update fileExist*/
     
	/* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
	inline void StagerCnsHelper::getCnsFileclass() throw();
      
      
	/************************************************************************************/
	/* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
	/**********************************************************************************/
	inline void StagerCnsHelper::cnsSettings(uid_t euid, uid_t egid, char* mask) throw();
	



	/* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
	inline void StagerCnsHelper::createFileAndUpdateCns(char* filename, mode_t mode) throw();
     


      }//end class
      




    }//end namespace dbService
  }// end namespace stager
}//end namespace castor




#endif

