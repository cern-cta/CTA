/******************************************************************************
 *                      IStagerSvcCInt.cpp
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
 * @(#)$RCSfile: IStagerSvcCInt.cpp,v $ $Revision: 1.54 $ $Release$ $Date: 2005/09/05 12:54:34 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include <errno.h>
#include <string>

extern "C" {

  // C include files
#include "castor/ServicesCInt.hpp"
#include "castor/stager/IStagerSvcCInt.hpp"

  //----------------------------------------------------------------------------
  // checkIStagerSvc
  //----------------------------------------------------------------------------
  bool checkIStagerSvc(Cstager_IStagerSvc_t* stgSvc) {
    if (0 == stgSvc || 0 == stgSvc->stgSvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  // Cstager_IStagerSvc_fromIService
  //----------------------------------------------------------------------------
  Cstager_IStagerSvc_t*
  Cstager_IStagerSvc_fromIService(castor::IService* obj) {
    Cstager_IStagerSvc_t* stgSvc = new Cstager_IStagerSvc_t();
    stgSvc->errorMsg = "";
    stgSvc->stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(obj);
    return stgSvc;
  }

  //----------------------------------------------------------------------------
  // Cstager_IStagerSvc_delete
  //----------------------------------------------------------------------------
  int Cstager_IStagerSvc_delete(Cstager_IStagerSvc_t* stgSvc) {
    try {
      if (0 == stgSvc) return 0;
      if (0 != stgSvc->stgSvc) stgSvc->stgSvc->release();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete stgSvc;
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_subRequestToDo
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_subRequestToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                        enum castor::ObjectsIds* types,
                                        unsigned int nbTypes,
                                        castor::stager::SubRequest** subreq) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<enum castor::ObjectsIds> cppTypes;
      for (unsigned int i = 0; i < nbTypes; i++) {
        cppTypes.push_back(types[i]);
      }
      *subreq = stgSvc->stgSvc->subRequestToDo(cppTypes);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  };

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_isSubRequestToSchedule
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_isSubRequestToSchedule
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::DiskCopyForRecall*** sources,
   unsigned int* sourcesNb) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::list<castor::stager::DiskCopyForRecall*> sourcesList;
      if (stgSvc->stgSvc->isSubRequestToSchedule(subreq, sourcesList)) {
        *sourcesNb = sourcesList.size();
        if (*sourcesNb > 0) {
          *sources = (castor::stager::DiskCopyForRecall**)
            malloc((*sourcesNb) * sizeof(struct Cstager_DiskCopyForRecall_t*));
          std::list<castor::stager::DiskCopyForRecall*>::iterator it =
            sourcesList.begin();
          for (unsigned int i = 0; i < *sourcesNb; i++, it++) {
            (*sources)[i] = *it;
          }
        }
        return 1;
      } else {
        *sourcesNb = 0;
        return 0;
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_errorMsg
  //-------------------------------------------------------------------------
  const char* Cstager_IStagerSvc_errorMsg(Cstager_IStagerSvc_t* stgSvc) {
    return stgSvc->errorMsg.c_str();
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectSvcClass
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectSvcClass(struct Cstager_IStagerSvc_t* stgSvc,
                                        castor::stager::SvcClass** svcClass,
                                        const char* name) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *svcClass = stgSvc->stgSvc->selectSvcClass(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectFileClass
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectFileClass(struct Cstager_IStagerSvc_t* stgSvc,
                                         castor::stager::FileClass** fileClass,
                                         const char* name) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *fileClass = stgSvc->stgSvc->selectFileClass(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectCastorFile
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectCastorFile
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::CastorFile** castorFile,
   const u_signed64 fileId, const char* nsHost,
   u_signed64 svcClass, u_signed64 fileClass,
   u_signed64 fileSize) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *castorFile = stgSvc->stgSvc->selectCastorFile
        (fileId, nsHost, svcClass, fileClass, fileSize);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_updateAndCheckSubRequest
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_updateAndCheckSubRequest
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   int* result) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *result =
        stgSvc->stgSvc->updateAndCheckSubRequest(subreq) ? 1 : 0;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_recreateCastorFile
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_recreateCastorFile
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::CastorFile* castorFile,
   castor::stager::SubRequest *subreq,
   castor::stager::DiskCopyForRecall** diskCopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *diskCopy = stgSvc->stgSvc->recreateCastorFile(castorFile, subreq);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_bestFileSystemForJob
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_bestFileSystemForJob
  (struct Cstager_IStagerSvc_t* stgSvc,
   char** fileSystems, char** machines,
   u_signed64* minFree, unsigned int fileSystemsNb,
   char** mountPoint, char** diskServer) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::string mp, ds;
      stgSvc->stgSvc->bestFileSystemForJob
        (fileSystems, machines, minFree, fileSystemsNb, &mp, &ds);
      *mountPoint = strdup(mp.c_str());
      *diskServer = strdup(ds.c_str());
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_updateFileSystemForJob
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_updateFileSystemForJob
  (struct Cstager_IStagerSvc_t* stgSvc,
   char* fileSystem, char* diskServer,
   u_signed64 fileSize) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->updateFileSystemForJob
        (fileSystem, diskServer, fileSize);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_archiveSubReq
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_archiveSubReq
  (struct Cstager_IStagerSvc_t* stgSvc,
   u_signed64 subReqId) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->archiveSubReq(subReqId);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_stageRm
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_stageRm
  (struct Cstager_IStagerSvc_t* stgSvc,
   const u_signed64 fileId,
   const char* nsHost) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->stageRm(fileId, nsHost);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_stageRelease
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_stageRelease
  (struct Cstager_IStagerSvc_t* stgSvc,
   const u_signed64 fileId,
   const char* nsHost) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->stageRelease(fileId, nsHost);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

}
