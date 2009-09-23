/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (DBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"


#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "Cupv_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "osdep.h"
/* to get a instance of the services */
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"




#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"
#include "castor/System.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include "serrno.h"
#include <errno.h>
#include <vector>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>




namespace castor{
  namespace stager{
    namespace daemon{


      /* constructor-> return the request type, called on the different thread (job, pre, stg) */
      RequestHelper::RequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest)
        throw(castor::exception::Exception) :
        stagerService(0), dbSvc(0), baseAddr(0),
        subrequest(0), fileRequest(0), svcClass(0), castorFile(0) {

        try{
          // for monitoring purposes
          gettimeofday(&tvStart, NULL);

          // get thread-safe pointers to services. They were already initialized
          // in the main, so we are sure pointers are valid
          castor::IService* svc = castor::BaseObject::services()->
            service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
          stagerService = dynamic_cast<castor::stager::IStagerSvc*>(svc);

          svc = castor::BaseObject::services()->
            service("DbCnvSvc", castor::SVC_DBCNV);
          dbSvc = dynamic_cast<castor::db::DbCnvSvc*>(svc);

          baseAddr = new castor::BaseAddress;
          baseAddr->setCnvSvcName("DbCnvSvc");
          baseAddr->setCnvSvcType(SVC_DBCNV);

          subrequest=subRequestToProcess;
          default_protocol = "rfio";
          memset(&cnsFileId, 0, sizeof(cnsFileId));

          dbSvc->fillObj(baseAddr, subrequest, castor::OBJ_FileRequest, false);
          this->fileRequest=subrequest->request();

          typeRequest = fileRequest->type();
        }
        catch(castor::exception::Exception e){
          // should never happen: the db service is initialized in the main as well
          castor::dlf::Param params[]={
            castor::dlf::Param("Function","RequestHelper constructor")};

          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_SERVICES_EXCEPTION, 1 ,params);
          e.getMessage()<< "Error on the Database";
          throw e;
        }
      }


      /* destructor */
      RequestHelper::~RequestHelper() throw()
      {
        if(fileRequest && subrequest) {
          // Calculate statistics
          timeval tv;
          gettimeofday(&tv, NULL);
          signed64 procTime = ((tv.tv_sec * 1000000) + tv.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
          castor::dlf::Param params[] = {
            castor::dlf::Param(subrequestUuid),
            castor::dlf::Param("Type", castor::ObjectsIdStrings[fileRequest->type()]),
            castor::dlf::Param("Filename", subrequest->fileName()),
            castor::dlf::Param("Username", username),
            castor::dlf::Param("Groupname", groupname),
            castor::dlf::Param("SvcClass", (svcClass ? svcClass->name() : "Unknown")),
            castor::dlf::Param("ProcessingTime", procTime * 0.000001)
          };
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_SYSTEM, STAGER_REQ_PROCESSED, 7, params, &cnsFileId);
        }

        delete baseAddr;
        if(castorFile) {
          if(castorFile->fileClass())
            delete castorFile->fileClass();
          delete castorFile;
        }
        if(fileRequest) {
          delete fileRequest;  // will delete subrequest too
        }
        else if(subrequest) {
          delete subrequest;
        }
        if(svcClass)
          delete svcClass;
      }


      /************************************************************************************/
      /* set the username and groupname string versions using id2name c function  */
      /**********************************************************************************/
      void RequestHelper::setUsernameAndGroupname() throw(castor::exception::Exception){
        struct passwd *this_passwd = 0;
        struct group *this_gr = 0;
        try{
          uid_t euid = fileRequest->euid();
          uid_t egid = fileRequest->egid();

          if ((this_passwd = Cgetpwuid(euid)) == NULL) {
            castor::exception::Exception ex(serrno);
            throw ex;
          }

          if (egid != this_passwd->pw_gid) {
            castor::exception::Exception ex(SEINTERNAL);
            throw ex;
          }

          if ((this_gr = Cgetgrgid(egid)) == NULL) {
            castor::exception::Exception ex(serrno);
            throw ex;
          }

          username = this_passwd->pw_name;
          groupname = this_gr->gr_name;
        } catch(castor::exception::Exception e) {
          castor::dlf::Param params[]=
            {castor::dlf::Param("Filename", subrequest->fileName()),
             castor::dlf::Param("Euid", fileRequest->euid()),
             castor::dlf::Param("Egid", fileRequest->egid()),
             castor::dlf::Param("Function", "RequestHelper->setUsernameAndGroupname")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_INVALID, 4, params);
          e.getMessage() << "Invalid user";
          throw e;
        }
      }


      void RequestHelper::setUuids() throw ()
      {
        // reqUuid
        if(fileRequest->reqId().empty() ||
           /* convert to the Cuuid_t version and copy in our thread safe variable */
           (string2Cuuid(&requestUuid, (char*) fileRequest->reqId().c_str()) != 0)) {
          requestUuid = nullCuuid;
        }
        // subreqUuid: this is never empty
        if(string2Cuuid(&subrequestUuid, (char *)subrequest->subreqId().c_str()) != 0) {
          subrequestUuid = nullCuuid;
        }
      }

      void RequestHelper::resolveSvcClass() throw(castor::exception::Exception){
        // check if the service class has been resolved
        dbSvc->fillObj(baseAddr, fileRequest, castor::OBJ_SvcClass, false);
        svcClass = fileRequest->svcClass();
        if(!svcClass) {
          castor::exception::Exception e(ENOENT);
          e.getMessage() << "Invalid service class";
          throw e;
        }
        // if defined, this is the forced file class
        dbSvc->fillObj(baseAddr, svcClass, castor::OBJ_FileClass, false);
      }


      /*******************************************************************************************************************************************/
      /*  link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name) ): called in Request.jobOriented()*/
      /*****************************************************************************************************************************************/
      void RequestHelper::getCastorFileFromSvcClass(CnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        // get the fileClass by name
        castor::stager::FileClass* fileClass = stagerService->selectFileClass(stgCnsHelper->cnsFileclass.name);
        if(fileClass == 0) {
          logToDlf(DLF_LVL_USER_ERROR, STAGER_SVCCLASS_EXCEPTION);
          castor::exception::Internal ex;
          ex.getMessage() << "No fileclass " << stgCnsHelper->cnsFileclass.name << " in DB";
          throw ex;
        }
        try{
          u_signed64 fileClassId = fileClass->id();
          u_signed64 svcClassId = svcClass->id();
          // copy the fileid structure for logging purposes
          // XXX a pointer doesn't work for bad owning relationships. To be redesigned
          memcpy(&cnsFileId, &stgCnsHelper->cnsFileid, sizeof(cnsFileId));

          // get the castorFile from the stagerService and fill it on the subrequest
          // note that for a Put request we should truncate the size, but this is done later on by
          // recreateCastorFile after all necessary checks
          castorFile = stagerService->selectCastorFile
            (cnsFileId.fileid, cnsFileId.server, svcClassId, fileClassId,
             stgCnsHelper->cnsFilestat.filesize,
             subrequest->fileName(), stgCnsHelper->cnsFilestat.mtime);

          subrequest->setCastorFile(castorFile);
          castorFile->setFileClass(fileClass);

          // create links in db
          dbSvc->fillRep(baseAddr, subrequest, castor::OBJ_CastorFile, false);
          dbSvc->fillRep(baseAddr, castorFile, castor::OBJ_FileClass, false);
        }
        catch(castor::exception::Exception e){
          logToDlf(DLF_LVL_ERROR, STAGER_CASTORFILE_EXCEPTION, &(cnsFileId));

          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"Impossible to perform the request: " << e.getMessage().str();
          throw(ex);
        }
      }


      /*********************************************************************************/
      /* check if the user (euid,egid) has the right permission for the request's type */
      /*********************************************************************************/
      void RequestHelper::checkFilePermission(bool fileCreated,
                                              castor::stager::daemon::CnsHelper* stgCnsHelper)
        throw(castor::exception::Exception)
      {
        try{
          std::string filename = this->subrequest->fileName();
          uid_t euid = this->fileRequest->euid();
          uid_t egid = this->fileRequest->egid();

          switch(fileRequest->type()) {
          case OBJ_StageGetRequest:
          case OBJ_StagePrepareToGetRequest:
          case OBJ_StageRepackRequest:
            if ( Cns_accessUser(filename.c_str(), R_OK, euid, egid) == -1 ) {
              castor::exception::Exception ex(serrno);
              throw ex;
            }
            break;

          case OBJ_StagePrepareToPutRequest:
          case OBJ_StagePrepareToUpdateRequest:
          case OBJ_StagePutRequest:
          case OBJ_StageUpdateRequest:
          case OBJ_StagePutDoneRequest:
            if ( Cns_accessUser(filename.c_str(), (fileCreated ? R_OK : W_OK), euid, egid) == -1 ) {
              castor::exception::Exception ex(serrno);
              throw ex;
            }
            break;

          case OBJ_SetFileGCWeight: {
            // Get the name of the localhost to pass into the Cupv interface.
            std::string localHost;
            try {
              localHost = castor::System::getHostName();
            } catch (castor::exception::Exception e) {
              castor::exception::Exception ex(SEINTERNAL);
              ex.getMessage() << "Failed to determine the name of the localhost";
              throw ex;
            }
            // Check if the user has GRP_ADMIN or ADMIN privileges.
            int rc = Cupv_check(euid, egid, localHost.c_str(), localHost.c_str(), P_GRP_ADMIN);
            if ((rc < 0) && (serrno != EACCES)) {
              castor::exception::Exception ex(serrno);
              ex.getMessage() << "Failed Cupv_check call for "
                              << euid << ":" << egid << " (GRP_ADMIN)";
              throw ex;
            } else if ((stgCnsHelper->cnsFilestat.gid == egid) && (rc == 0)) {
              return;
            }

            rc = Cupv_check(euid, egid, localHost.c_str(), localHost.c_str(), P_ADMIN);
            if (rc == -1) {
              castor::exception::Exception ex(serrno);
              if (serrno != EACCES) {
                ex.getMessage() << "Failed Cupv_check call for "
                                << euid << ":" << egid << " (ADMIN)";
              }
              throw ex;
            }
          }
            break;

          default:
            break;
          }
        }
        catch(castor::exception::Exception e){
          if ((e.code() == SEINTERNAL) || (e.code() != EACCES)) {
            castor::dlf::Param params[] = {
              castor::dlf::Param(subrequestUuid),
              castor::dlf::Param("Type",
                                 ((unsigned)fileRequest->type() < castor::ObjectsIdsNb ?
                                  castor::ObjectsIdStrings[fileRequest->type()] : "Unknown")),
              castor::dlf::Param("Filename", subrequest->fileName()),
              castor::dlf::Param("Username", username),
              castor::dlf::Param("Groupname", groupname),
              castor::dlf::Param("Function", "RequestHelper.checkFilePermission"),
              castor::dlf::Param("Error", sstrerror(e.code()))
            };
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CNS_EXCEPTION, 7, params, &cnsFileId);
          } else {
            logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION, &cnsFileId);
          }
          e.getMessage() << sstrerror(e.code());
          throw e;
        }
      }


      void RequestHelper::logToDlf(int level, int messageNb, Cns_fileid* fid) throw()
      {
        castor::dlf::Param params[] = {
          castor::dlf::Param(subrequestUuid),
          castor::dlf::Param("Type",
                             ((unsigned)fileRequest->type() < castor::ObjectsIdsNb ?
                              castor::ObjectsIdStrings[fileRequest->type()] : "Unknown")),
          castor::dlf::Param("Filename", subrequest->fileName()),
          castor::dlf::Param("Username", username),
          castor::dlf::Param("Groupname", groupname),
          castor::dlf::Param("SvcClass", (svcClass ? svcClass->name() : "Unknown"))
        };
        castor::dlf::dlf_writep(requestUuid, level, messageNb, 6, params, fid);
      }

    } //end namespace daemon

  } //end namespace stager

} //end namespace castor
