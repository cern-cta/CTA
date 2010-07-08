/***************************************************************************/
/* helper class for the c methods and structures related with the cns_api */
/*************************************************************************/

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "Cns_struct.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "osdep.h"
#include "castor/Constants.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileClass.hpp"

#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/daemon/NsOverride.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"



namespace castor{
  namespace stager{
    namespace daemon{


      CnsHelper::CnsHelper(Cuuid_t requestUuid) throw(castor::exception::Exception){
        memset(&(cnsFileclass), 0, sizeof(cnsFileclass));
        this->requestUuid = requestUuid;
      }

      CnsHelper::~CnsHelper() throw() {}

      /****************************************************************************************/
      /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
      void CnsHelper::getCnsFileclass() throw(castor::exception::Exception){
        // only enter if the info is not yet available
        if (cnsFileclass.classid != 0) return;
        if (Cns_queryclass(cnsFileid.server, cnsFilestat.fileclass, NULL, &cnsFileclass) != 0) {
          castor::dlf::Param params[]={
            castor::dlf::Param("Function","Cns_queryclass"),
            castor::dlf::Param("Error", sstrerror(serrno))};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 2, params);

          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage() << "Failed to query the fileclass on the Name Server";
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
          castor::dlf::Param params[]={
            castor::dlf::Param("Function","Cns_setid"),
            castor::dlf::Param("Error", sstrerror(serrno))};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 2, params);

          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage() << "Failed to set the user for the Name Server";
          throw ex;
        }
      }

      /****************************************************************************************************************/
      /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
      /* create the file if it is needed/possible */
      /**************************************************************************************************************/
      bool CnsHelper::checkFileOnNameServer(castor::stager::SubRequest* subReq, castor::stager::SvcClass* svcClass)
        throw(castor::exception::Exception) {
        int type = subReq->request()->type();

        // check if the filename is valid (it has to start with /)
        if (subReq->fileName().empty() || subReq->fileName().at(0)!='/'){
          castor::exception::Exception ex(EINVAL);
          ex.getMessage() << "Invalid file path";
          throw ex;
        }

        // check if the required file exists
        memset(&(cnsFileid), '\0', sizeof(cnsFileid));
        bool newFile = false;
        if (Cns_statx(subReq->fileName().c_str(), &cnsFileid, &cnsFilestat) != 0) {
          if (serrno == ENOENT) {
            newFile = true;
          } else {
            castor::exception::Exception ex(serrno);
            ex.getMessage() << "Failed to stat the file in the Name Server";

            // Error on the name server
            castor::dlf::Param params[] = {
              castor::dlf::Param("Filename", subReq->fileName()),
              castor::dlf::Param("Function", "Cns_statx"),
              castor::dlf::Param("Error", sstrerror(serrno))};
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR,
                                    STAGER_CNS_EXCEPTION, 3, params);
            throw ex;
          }
        }

        // deny any request on a directory
        if(!newFile && (cnsFilestat.filemode & S_IFDIR) == S_IFDIR) {
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, 0);
          serrno = EISDIR;
          castor::exception::Exception ex(serrno);
          ex.getMessage() << "Cannot perform this request on a directory";
          throw ex;
        }

        // check if the request is allowed to create the file
        if(((OBJ_StagePutRequest == type) ||
            (OBJ_StageUpdateRequest == type && (subReq->flags() & O_CREAT) == O_CREAT) ||
            (OBJ_StagePrepareToPutRequest == type && (subReq->modeBits() & 0222) != 0) ||
            (OBJ_StagePrepareToUpdateRequest == type && (subReq->flags() & O_CREAT) == O_CREAT && (subReq->modeBits() & 0222) != 0)
            )) {   // (re)creation is allowed
          if(!newFile && ((OBJ_StagePutRequest == type) || (OBJ_StagePrepareToPutRequest == type))) {
            // if no segments, we recreate, otherwise it's better to keep the original segments
            struct Cns_segattrs *nsSegmentAttrs = 0;
            int nbNsSegments = 0;
            serrno = 0;
            if(Cns_getsegattrs(0, &cnsFileid, &nbNsSegments, &nsSegmentAttrs) != 0) {
              castor::exception::Exception ex(serrno);
              ex.getMessage() << "Failed to getsegattrs for the file in the Name Server";

              // Error on the name server
              castor::dlf::Param params[] = {
                castor::dlf::Param("Filename", subReq->fileName()),
                castor::dlf::Param("Function", "Cns_getsegattrs"),
                castor::dlf::Param("Error", sstrerror(serrno))};
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR,
                                      STAGER_CNS_EXCEPTION, 3, params);
            } else {
              if(nbNsSegments == 0) {
                // This file has no copy on tape
                getCnsFileclass();
                if (cnsFileclass.nbcopies == 0) {
                  // This file will never have a copy on tape, so we force recreation
                  newFile = true;
                }
              }
              free(nsSegmentAttrs);
            }
          }

          // create the file and update cnsFileid and cnsFilestat structures
          memset(&(cnsFileid), 0, sizeof(cnsFileid));
          serrno = 0;
          if(newFile && (0 != Cns_creatx(subReq->fileName().c_str(), subReq->modeBits(), &cnsFileid)) && (serrno != EEXIST)) {
            castor::dlf::Param params[]={
              castor::dlf::Param("Filename", subReq->fileName()),
              castor::dlf::Param("Function", "Cns_creatx"),
              castor::dlf::Param("Error", sstrerror(serrno))};

            if ((serrno == ENOENT) || (serrno == EACCES) || (serrno == ENOTDIR) ||
                (serrno == ENAMETOOLONG) || (serrno == EMLINK)) {
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR,
                                      STAGER_CNS_EXCEPTION, 3, params);
            } else {
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR,
                                      STAGER_CNS_EXCEPTION, 3, params);
              serrno = SEINTERNAL;
            }
            castor::exception::Exception ex(serrno);
            ex.getMessage() << "Failed to (re)create the file";
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

          // in case of Disk1 pool, force the fileClass of the file
          if(newFile && svcClass && svcClass->forcedFileClass()) {
            std::string forcedFileClassName = svcClass->forcedFileClass()->name();
            Cns_unsetid();           // local call, ignore result
            if(Cns_chclass(subReq->fileName().c_str(), 0, (char*)forcedFileClassName.c_str()) != 0) {
              castor::dlf::Param params[]={
                castor::dlf::Param("Filename", subReq->fileName()),
                castor::dlf::Param("Function", "Cns_chclass"),
                castor::dlf::Param("Error", sstrerror(serrno))};
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 3, params);

              castor::exception::Exception ex(SEINTERNAL);
              ex.getMessage() << "Failed to force the fileclass on this file";
              // before giving up we unlink the just created file, and we ignore ns errors
              Cns_unlink(subReq->fileName().c_str());
              throw ex;
            }
            Cns_setid(euid, egid);   // local call, ignore result
          }

          // update cns structures
          if(Cns_statx(subReq->fileName().c_str(), &cnsFileid, &cnsFilestat) != 0) {
            castor::dlf::Param params[]={
              castor::dlf::Param("Filename", subReq->fileName()),
              castor::dlf::Param("Function","Cns_statx"),
              castor::dlf::Param("Error", sstrerror(serrno))};
            if (serrno == ENOTDIR) {
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_CNS_EXCEPTION, 3, params);
            } else {
              castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 3, params);
              serrno = SEINTERNAL;
            }

            castor::exception::Exception ex(serrno);
            ex.getMessage() << "Failed to stat the file in the Name Server";
            throw ex;
          }
        }
        else {
          // other requests go through only on existing files
          if(newFile) {
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 0);

            if(OBJ_StagePrepareToPutRequest == type || OBJ_StagePrepareToUpdateRequest == type) {
              // this should never happen: PrepareToPut was called with readonly mode. Let's provide a custom error message
              castor::exception::Exception ex(EINVAL);
              ex.getMessage() << "Cannot PrepareToPut a non-writable file";
              throw ex;
            }
            else {
              // we just answer with the standard error message
              castor::exception::Exception ex(ENOENT);
              throw ex;
            }
          }
        }
        // before returning, replace for logging purposes the CNS host
        // in case it has been overridden
        std::string cnsHost = NsOverride::getInstance()->getTargetCnsHost();
        if(cnsHost.length() > 0) {
          strncpy(cnsFileid.server, cnsHost.c_str(), CA_MAXHOSTNAMELEN+1);
        }

        return newFile;
      }

    }//end namespace daemon
  }//end namespace stager
}//end namespce castor
