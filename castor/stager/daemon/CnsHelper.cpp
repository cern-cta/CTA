/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileClass.hpp"


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
#include "castor/stager/daemon/DlfMessages.hpp"

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
    namespace daemon{
      
      
      CnsHelper::CnsHelper(Cuuid_t requestUuid) throw(castor::exception::Exception){
        this->requestUuid = requestUuid;
      }
      
      CnsHelper::~CnsHelper() throw() {}
      
      /****************************************************************************************/
      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      void CnsHelper::getCnsFileclass() throw(castor::exception::Exception){
        memset(&(cnsFileclass),0, sizeof(cnsFileclass));
        if( Cns_queryclass((cnsFileid.server),(cnsFilestat.fileclass), NULL, &(cnsFileclass)) != 0 ){
          
          castor::dlf::Param params[]={	castor::dlf::Param("Function","Cns_queryclass")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Error on the Name Server";
          throw ex;
        }
      }
      
      /***************************************************************/
      /* set the user and group id needed to call the Cns functions */
      /*************************************************************/
      void CnsHelper::cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest) throw(castor::exception::Exception) {
        euid = fileRequest->euid();
        egid = fileRequest->egid();
        if (Cns_setid(euid,egid) != 0) {
          castor::dlf::Param params[]={	castor::dlf::Param("Function","Cns_setid")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Impossible to set the user on the Name Server";
          throw ex;
        }
        
        if (Cns_umask(fileRequest->mask()) < 0) {
          castor::dlf::Param params[]={	castor::dlf::Param("Function","Cns_umask")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
          
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"Impossible to set the mask on the Name Server";
          throw ex;
        }
      }
      
      /****************************************************************************************************************/
      /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
      /* create the file if it is needed/possible */
      /**************************************************************************************************************/
      bool CnsHelper::checkFileOnNameServer(castor::stager::SubRequest* subReq, castor::stager::SvcClass* svcClass)
        throw(castor::exception::Exception){
        
        /* check if the required file exists */
        memset(&(cnsFileid), '\0', sizeof(cnsFileid)); /* reset cnsFileid structure  */
        bool newFile = (0 != Cns_statx(subReq->fileName().c_str(), &(cnsFileid), &(cnsFilestat))) && (serrno == ENOENT);
        int type = subReq->request()->type();
        
        if(newFile && ((OBJ_StagePutRequest == type) || 
                       (OBJ_StageUpdateRequest == type && (subReq->flags() & O_CREAT) == O_CREAT) ||
                       (OBJ_StagePrepareToPutRequest == type && (subReq->modeBits() & 0222) != 0) ||
                       (OBJ_StagePrepareToUpdateRequest == type && (subReq->flags() & O_CREAT) == O_CREAT && (subReq->modeBits() & 0222) != 0)
                       )) {
          // creation is allowed on the above type of requests

          // using Cns_creatx and Cns_stat c functions, create the file and update cnsFileid and cnsFilestat structures
          memset(&(cnsFileid), 0, sizeof(cnsFileid));
          serrno = 0;
          if ((0 != Cns_creatx(subReq->fileName().c_str(), subReq->modeBits(), &(cnsFileid))) && (serrno != EEXIST)) {
            castor::dlf::Param params[]={castor::dlf::Param("Function","CnsHelper->checkFileOnNameServer.1")};
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
            
            castor::exception::Exception ex(serrno);
            ex.getMessage() << "Impossible to create the file";
            throw ex;
          }
          else if(serrno == EEXIST) {
            // the file exists: this might happen if two threads are concurrently 
            // creating the same file and they both got ENOENT at the Cns_statx call above.
            // That's fine, one of the two will win at the end, as of now
            // we let the request go through and we'll call again Cns_statx to have the
            // up-to-date information from the nameserver.
            newFile = false;
          }

          /* in case of Disk1 pool, we want to force the fileClass of the file */
          if(svcClass->hasDiskOnlyBehavior() && svcClass->forcedFileClass()) {
            std::string forcedFileClassName = svcClass->forcedFileClass()->name();
            Cns_unsetid();
            if(Cns_chclass(subReq->fileName().c_str(), 0, (char*)forcedFileClassName.c_str())) {
              castor::dlf::Param params[]={castor::dlf::Param("Function","CnsHelper->checkFileOnNameServer.2")};
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 1 ,params);	  
              
              castor::exception::Exception ex(serrno);
              ex.getMessage() << "Impossible to force file class for this file";
              // before giving up we unlink the just created file
              Cns_delete(subReq->fileName().c_str());
              throw ex;
            }
            Cns_setid(euid, egid);    // at this stage this call won't fail, so we ignore its result
          }
          
          // update cns structures
          Cns_statx(subReq->fileName().c_str(), &(cnsFileid), &(cnsFilestat));
          // if no segments, reset filesize to 0
          // to be done
        }
        else if(newFile) {
          // other requests cannot create non existing files
          castor::dlf::Param params[]={ castor::dlf::Param("Function", "CnsHelper->checkFileOnNameServer.3")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 1, params);
          
          if(OBJ_StagePrepareToPutRequest == type || OBJ_StagePrepareToUpdateRequest == type) {
            // this should never happen: PrepareToPut was called with readonly mode. Let's provide a custom error message
            castor::exception::Exception ex(EINVAL);
            ex.getMessage() << "Cannot PrepareToPut a non-writable file";
            throw ex;
          }
          else {
            castor::exception::Exception ex(ENOENT);
            throw ex;
          }
        }
        return newFile;
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespce castor
