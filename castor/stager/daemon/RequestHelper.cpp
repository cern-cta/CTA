/******************************************************************************
 *                castor/stager/daemon/RequestHelper.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * Helper class for handling file-oriented user requests
 *
 * @author castor dev team
 *****************************************************************************/

#include "castor/System.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/daemon/NsOverride.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"


namespace castor {

  namespace stager {

    namespace daemon {

      /* constructor, returns the request type */
      RequestHelper::RequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest)
        throw(castor::exception::Exception) :
        stagerService(0), dbSvc(0), baseAddr(0), subrequest(subRequestToProcess),
        fileRequest(0), svcClass(0), castorFile(0) {

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
          baseAddr = new BaseAddress();
          baseAddr->setCnvSvcName("DbCnvSvc");
          baseAddr->setCnvSvcType(SVC_DBCNV);

          fileRequest = subrequest->request();
          typeRequest = fileRequest->type();
          // uuids, never empty in normal cases 
          if(fileRequest->reqId().empty() ||
             (string2Cuuid(&requestUuid, (char*) fileRequest->reqId().c_str()) != 0)) {
            requestUuid = nullCuuid;
          }
          if(string2Cuuid(&subrequestUuid, (char *)subrequest->subreqId().c_str()) != 0) {
            subrequestUuid = nullCuuid;
          }
          memset(&cnsFileid, 0, sizeof(cnsFileid));
          memset(&cnsFilestat, 0, sizeof(cnsFilestat));
          m_stagerOpenTimeInUsec = 0;
        }
        catch(castor::exception::Exception& e){
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
            castor::dlf::Param("SvcClass", fileRequest->svcClassName()),
            castor::dlf::Param("ProcessingTime", procTime * 0.000001)
          };
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_SYSTEM, STAGER_REQ_PROCESSED, 7, params, &cnsFileid);
        }
        if(castorFile) {
          delete castorFile;
        }
        if(fileRequest) {
          delete fileRequest;  // will delete subrequest too
        }
        else if(subrequest) {
          delete subrequest;
        }
        if(svcClass) {
          delete svcClass;
        }
        delete baseAddr;
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
          castor::dlf::Param("SvcClass", fileRequest->svcClassName())
        };
        castor::dlf::dlf_writep(requestUuid, level, messageNb, 6, params, fid);
      }


      /* set the username and groupname string versions */
      void RequestHelper::setUsernameAndGroupname() throw(castor::exception::Exception){
        struct passwd *this_passwd = 0;
        struct group *this_gr = 0;
        try{
          uid_t euid = fileRequest->euid();
          uid_t egid = fileRequest->egid();

          if ((this_passwd = Cgetpwuid(euid)) == NULL) {
            castor::exception::Exception ex(serrno);
            ex.getMessage() << "RequestHelper: Cgetpwuid failed for uid " << euid << " : " << strerror(errno);
            throw ex;
          }

          if (egid != this_passwd->pw_gid) {
            castor::exception::Exception ex(SEINTERNAL);
            ex.getMessage() << "RequestHelper: given egid does not match passwd.gid";
            throw ex;
          }

          if ((this_gr = Cgetgrgid(egid)) == NULL) {
            castor::exception::Exception ex(serrno);
            ex.getMessage() << "RequestHelper: Cgetgrgid failed for gid " << egid << " : " << strerror(errno);
            throw ex;
          }

          username = this_passwd->pw_name;
          groupname = this_gr->gr_name;
        } catch(castor::exception::Exception& e) {
          castor::dlf::Param params[] = {
            castor::dlf::Param("Filename", subrequest->fileName()),
            castor::dlf::Param("Euid", fileRequest->euid()),
            castor::dlf::Param("Egid", fileRequest->egid()),
            castor::dlf::Param("Function", "RequestHelper::setUsernameAndGroupname")};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR, STAGER_USER_INVALID, 4, params);
          e.getMessage() << "Invalid user";
          throw e;
        }
      }


      void RequestHelper::resolveSvcClass() throw(castor::exception::Exception) {
        // XXX we're still using fillObj here, a single db method
        // XXX (better piggybacking on existing calls) should be implemented
        dbSvc->fillObj(baseAddr, fileRequest, castor::OBJ_SvcClass, false);
        svcClass = fileRequest->svcClass();
        if(!svcClass) {
          // not resolved, we cancel the request
          castor::exception::InvalidArgument e;
          e.getMessage() << "Invalid service class '" << fileRequest->svcClassName() << "'";
          throw e;
        }
        // if defined, this is the forced file class
        dbSvc->fillObj(baseAddr, svcClass, castor::OBJ_FileClass, false);
      }


      /* Gets the CastorFile from the db */
      void RequestHelper::getCastorFile() throw(castor::exception::Exception)
      {
        try{
          // get the castorFile from the stagerService and fill it on the subrequest
          // note that for a Put request we should truncate the size, but this is done later on by
          // handlePut after all necessary checks
          castorFile = stagerService->selectCastorFile(subrequest, &cnsFileid, &cnsFilestat, m_stagerOpenTimeInUsec);
        }
        catch (castor::exception::Exception& e) {
          castor::dlf::Param params[] = {
            castor::dlf::Param("ErrorMessage", e.getMessage().str())};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR, STAGER_CASTORFILE_EXCEPTION,
                                  1, params, &cnsFileid);

          castor::exception::Exception ex(e.code());
          ex.getMessage() << "Impossible to perform the request: " << e.getMessage().str();
          throw(ex);
        }
      }
      
      /* Stats the file in the NameServer, throws exception in case of error, except ENOENT */
      void RequestHelper::statNameServerFile() throw(castor::exception::Exception) {
        // Check the existence of the file. Don't throw exception if ENOENT
        Cns_setid(fileRequest->euid(), fileRequest->egid());
        serrno = 0;
        if (Cns_statcsx(subrequest->fileName().c_str(), &cnsFileid, &cnsFilestat) != 0 && serrno != ENOENT) {
          castor::exception::Exception ex(serrno);
          ex.getMessage() << "Failed to stat the file in the Name Server";
          // Error on the name server
          castor::dlf::Param params[] = {
            castor::dlf::Param("Filename", subrequest->fileName()),
            castor::dlf::Param("Function", "Cns_statx"),
            castor::dlf::Param("Error", sstrerror(serrno))};
          castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR,
                                  STAGER_CNS_EXCEPTION, 3, params);
          throw ex;
        }
      }
        
      /* Checks the existence of the file in the NameServer, and creates it if the request allows for
       * creation. Internally sets the fileId and nsHost for the file. */
      bool RequestHelper::openNameServerFile() throw(castor::exception::Exception) {
        // check if the filename is valid (it has to start with /)
        if (subrequest->fileName().empty() || subrequest->fileName().at(0) != '/') {
          castor::exception::Exception ex(EINVAL);
          ex.getMessage() << "Invalid file path";
          throw ex;
        }
        
        // Massage the flags so that the request type always wins over them.
        // See also the stage_open API.
        if(fileRequest->type() == OBJ_StagePutRequest || fileRequest->type() == OBJ_StagePrepareToPutRequest) {
          // a Put must (re)create and truncate, plus it's a write operation
          subrequest->setFlags(subrequest->flags() | O_CREAT | O_TRUNC | O_WRONLY);
        }
        else if(fileRequest->type() == OBJ_StageUpdateRequest || fileRequest->type() == OBJ_StagePrepareToUpdateRequest) {
          // an Update may recreate, plus it's a read/write operation
          subrequest->setFlags(subrequest->flags() | O_CREAT | O_RDWR);
        }
        else if(fileRequest->type() == OBJ_StageGetRequest || fileRequest->type() == OBJ_StagePrepareToGetRequest
          || fileRequest->type() == OBJ_StageRepackRequest) {
          // a Get is always a read-only operation (O_RDONLY == 0, hence the or is a no-op)
          subrequest->setFlags((subrequest->flags() | O_RDONLY) & ~O_RDWR & ~O_WRONLY);
        }
        // Open file in the NameServer. This eventually creates it when allowed according to the flags
        memset(&cnsFilestat, '\0', sizeof(cnsFilestat));
        serrno = 0;
        int newFile = 0;
        int rc = Cns_openx(fileRequest->euid(), fileRequest->egid(),
                  subrequest->fileName().c_str(), subrequest->flags(), subrequest->modeBits(),
                  (svcClass && svcClass->forcedFileClass() ? svcClass->forcedFileClass()->classId() : 0),
                  &newFile, &cnsFileid, &cnsFilestat, &m_stagerOpenTimeInUsec);
        
        // replace for logging purposes the CNS host in case it has been overridden
        std::string cnsHost = NsOverride::getInstance()->getTargetCnsHost();
        if(cnsHost.length() > 0) {
          strncpy(cnsFileid.server, cnsHost.c_str(), CA_MAXHOSTNAMELEN+1);
          cnsFileid.server[CA_MAXHOSTNAMELEN] = 0;
        }
        
        // Deny PrepareToPut|Update on files with preset checksums
        if(rc == 0 && (strncmp(cnsFilestat.csumtype, "PA", 2) == 0)
            && (fileRequest->type() == OBJ_StagePrepareToPutRequest || fileRequest->type() == OBJ_StagePrepareToUpdateRequest)) {
          rc = -1;
          serrno = ENOTSUP;
        }
        
        if(rc != 0) {
          // the open failed, log it along with the fileid info in case the file existed in advance
          castor::exception::Exception ex(serrno);
          castor::dlf::Param params[] = {
            castor::dlf::Param(subrequestUuid),
            castor::dlf::Param("Type",
                               ((unsigned)fileRequest->type() < castor::ObjectsIdsNb ?
                                castor::ObjectsIdStrings[fileRequest->type()] : "Unknown")),
            castor::dlf::Param("Filename", subrequest->fileName()),
            castor::dlf::Param("Username", username),
            castor::dlf::Param("Groupname", groupname),
            castor::dlf::Param("Flags", subrequest->flags()),
            castor::dlf::Param("Function", "Cns_openx"),
            castor::dlf::Param("Error", sstrerror(ex.code()))
          };
          if ((ex.code() != ENOENT) && (ex.code() != EACCES) && (ex.code() != EISDIR)
              && (ex.code() != EBUSY) && (ex.code() != ENOTSUP)) {
            // Error on the Name Server
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_ERROR,
              STAGER_CNS_EXCEPTION, 8, params, &cnsFileid);
          } else {
            // User error, operation not permitted
            castor::dlf::dlf_writep(requestUuid, DLF_LVL_USER_ERROR,
              STAGER_USER_PERMISSION, 8, params, &cnsFileid);
          }
          throw ex;
        }

        // return true if the file has just been created
        return newFile;
      }
      
    } //end namespace daemon

  } //end namespace stager

} //end namespace castor
