/************************************************************************/
/* container for the c methods and structures related with the cns_api */
/**********************************************************************/

#ifndef STAGER_CNS_HELPER_HPP
#define STAGER_CNS_HELPER_HPP 1

#include "../../../h/Cns_api.h"
#include "../../../h/Cns_struct.h"
#include "../../../h/Cglobals.h"
#include "../../../h/serrno.h"

#include "../../IObject.hpp"
#include "../../exception/Exception.hpp"

#include "../../ObjectSet.hpp"

#include <iostream>
#include <string>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>




namespace castor{
  namespace stager{
    namespace dbService{

    
extern "C"{
  int Cns_setid(uid_t, gid_t);
  mode_t Cns_umask(mode_t);
}
  
     
      class StagerCnsHelper : public virtual castor::IObject{
	
      public:
	
	struct Cns_fileid *fileid;
	struct Cns_fileid cnsFileid;
	struct Cns_filestat cnsFilestat;
	struct Cns_fileclass cnsFileclass;
	int fileExist;	

	/* static variables needed to get the fileid for logging */
	static int fileid_ts_key;
	static struct Cns_fileid fileid_ts_static;
	
	
	StagerCnsHelper::StagerCnsHelper() throw(castor::exception::Exception);
	StagerCnsHelper::~StagerCnsHelper() throw();

	

	/***************************************************************************************/
	/*  virtual functions inherited from IObject                                          */
	/*************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const; 


	/*******************/
	/* Cns structures */
	/*****************/ 
	
	/*since we are gonna use dlf: we won' t probably need it*/
	/* get the fileid pointer to print (since we are gonna use dlf: we won' t probably need it  */
	inline void StagerCnsHelper::getFileid(){
	  Cns_fileid *var;
	  Cglobals_get(&fileid_ts_key,(void**) &var, sizeof(struct Cns_fileid));
	  if(var == NULL){
	    this->fileid =&(this->fileid_ts_static);
	  }else{
	    this->fileid = var;
	  }
	}
	
	/* create cnsFileid, cnsFilestat and update fileExist using the "Cns_statx()" C function */
	/* for a subrequest.filename *//* update fileExist*/
	inline int StagerCnsHelper::createCnsFileIdAndStat_setFileExist(const char* subrequestFileName){

	  memset(&(this->cnsFileid), '\0', sizeof(this->cnsFileid)); /* reset cnsFileid structure  */
	  this->fileExist = (0 == Cns_statx(subrequestFileName,&(this->cnsFileid),&(this->cnsFilestat)));
	  
	  return(this->fileExist);
	}
     
	/* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
	inline void StagerCnsHelper::getCnsFileclass() throw(castor::exception::Exception){
	  memset(&(this->cnsFileclass),0, sizeof(cnsFileclass));
	  if( Cns_queryclass((this->cnsFileid.server),(this->cnsFilestat.fileclass), NULL, &(this->cnsFileclass)) != 0 ){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper getFileclass) Error on Cns_setid"<<std::endl;
	    throw ex;
	  }
	}
      
      
	/************************************************************************************/
	/* get the parameters neededs and call to the Cns_setid and Cns_umask c functions  */
	/**********************************************************************************/
	inline void StagerCnsHelper::cnsSettings(uid_t euid, uid_t egid, mode_t mask) throw(castor::exception::Exception){
	  if (Cns_setid(euid,egid) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper cnsSettings) Error on Cns_setid"<<std::endl;
	    throw ex;
	  }
	  
	  if (Cns_umask((mode_t) mask) != 0){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper cnsSettings) Error on Cns_umask"<<std::endl;
	    throw ex;
	  }
	}


	/* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
	inline void StagerCnsHelper::createFileAndUpdateCns(const char* filename, mode_t mode) throw(castor::exception::Exception){
	  memset(&(this->cnsFileid),0, sizeof(cnsFileid));
	  if (Cns_creatx(filename, mode, &(this->cnsFileid)) != 0) {
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_creatx"<<std::endl;
	    throw ex;
	  }
	  memset(&(this->cnsFileclass),0, sizeof(cnsFileclass));
	  if (Cns_statx(filename,&(this->cnsFileid),&(this->cnsFilestat)) != 0) {
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerCnsHelper createFileAndUpdateCns) Error on Cns_statx"<<std::endl;
	    throw ex;
	  }
	}
     


      }; // end StagerCnsHelper class
      




    }//end namespace dbService
  }// end namespace stager
}// end namespace castor


#endif

