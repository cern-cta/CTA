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
      
      
      StagerCnsHelper::StagerCnsHelper(Cuuid_t requestUuid) throw(castor::exception::Exception){
        this->requestUuid = requestUuid;
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
          
          castor::dlf::Param params[]={	castor::dlf::Param("Function:","StagerCnsHelper->getCnsFileclass")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Error on the Name Server";
          throw ex;
        }
      }
      
      /***************************************************************/
      /* set the user and group id needed to call the Cns functions */
      /*************************************************************/
      void StagerCnsHelper::cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest) throw(castor::exception::Exception) {
        euid = fileRequest->euid();
        egid = fileRequest->egid();
        if (Cns_setid(euid,egid) != 0) {
          castor::dlf::Param params[]={	castor::dlf::Param("Function:","StagerCnsHelper->cnsSetEuidAndEgid")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Impossible to set the user on the Name Server";
          throw ex;
        }
        
        if (Cns_umask(fileRequest->mask()) < 0) {
          castor::dlf::Param params[]={	castor::dlf::Param("Function:","StagerCnsHelper->cnsSetEuidAndEgid")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Impossible to set the user on the Name Server";
          throw ex;
        }
      }
      
      /****************************************************************************************************************/
      /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
      /* create the file if it is needed/possible */
      /**************************************************************************************************************/
      bool StagerCnsHelper::checkAndSetFileOnNameServer(std::string fileName, int type, int subrequestFlags, mode_t modeBits, castor::stager::SvcClass* svcClass) throw(castor::exception::Exception){
        
        try{
          /* check if the required file exists */
          memset(&(cnsFileid), '\0', sizeof(cnsFileid)); /* reset cnsFileid structure  */
          fileExist = (0 == Cns_statx(fileName.c_str(),&(cnsFileid),&(cnsFilestat)));
          
          
          if(!fileExist){
            if(isFileToCreateOrException(type, subrequestFlags)){/* create the file is the type is like put, ...*/
              if(serrno == ENOENT){
                
                /* using Cns_creatx and Cns_stat c functions, create the file and update Cnsfileid and Cnsfilestat structures */
                memset(&(cnsFileid),0, sizeof(cnsFileid));
                
                if (Cns_creatx(fileName.c_str(), modeBits, &(cnsFileid)) != 0) {
                  castor::dlf::Param params[]={castor::dlf::Param("Function:","StagerCnsHelper->checkAndSetFileOnNameServer")};
                  castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
                  
                  castor::exception::Exception ex(serrno);
                  ex.getMessage()<<"Impossible to create the file";
                  throw ex;
                }
                
                /* in case of Disk1 pool, we want to force the fileClass of the file */
                if(svcClass->hasDiskOnlyBehavior()){
                  std::string forcedFileClassName = svcClass->forcedFileClass();
                  
                  if(!forcedFileClassName.empty()){
                    Cns_unsetid();
                    if(Cns_chclass(fileName.c_str(), 0, (char*)forcedFileClassName.c_str())){
                      int tempserrno = serrno;
                      Cns_delete(fileName.c_str());
                      serrno = tempserrno;
                      
                      castor::dlf::Param params[]={castor::dlf::Param("Function:","StagerCnsHelper->checkAndSetFileOnNameServer")};
                      castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
                      
                      castor::exception::Exception ex(serrno);
                      ex.getMessage()<<"Impossible to create the file";
                      throw ex;
                    }
                    Cns_setid(euid, egid);    // at this stage this call won't fail, so we ignore its result
                  }/* "only force the fileClass if one is given" */
                  
                }/* end of "if(hasDiskOnly Behavior)" */
                
                Cns_statx(fileName.c_str(),&(cnsFileid),&(cnsFilestat));
                
              }
            }
          }
          return(fileExist);
        }catch(castor::exception::Exception e){
          
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
          
          castor::dlf::Param params[]={ castor::dlf::Param("Function:", "StagerCnsHelper->isFileToCreateOrException")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 1 ,params);
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"User asking for a non existing file";
          throw ex;
        }
        return(toCreate);
      }
      
    }//end namespace dbService
  }//end namespace stager
}//end namespce castor
