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
 * @(#)$RCSfile: IStagerSvcCInt.cpp,v $ $Revision: 1.36 $ $Release$ $Date: 2005/01/22 19:25:49 $ $Author: jdurand $
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
  // Cstager_IStagerSvc_anySegmentsForTape
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_anySegmentsForTape(Cstager_IStagerSvc_t* stgSvc,
                                            castor::stager::Tape* searchItem) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      return stgSvc->stgSvc->anySegmentsForTape(searchItem);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_segmentsForTape
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_segmentsForTape
  (Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Tape* searchItem,
   castor::stager::Segment*** segmentArray,
   int* nbItems) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<castor::stager::Segment*> result =
        stgSvc->stgSvc->segmentsForTape(searchItem);
      *nbItems = result.size();
      *segmentArray = (castor::stager::Segment**)
        malloc((*nbItems) * sizeof(castor::stager::Segment*));
      for (int i = 0; i < *nbItems; i++) {
        (*segmentArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_bestFileSystemForSegment
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_bestFileSystemForSegment
  (Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Segment* segment,
   castor::stager::DiskCopyForRecall** diskcopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *diskcopy = stgSvc->stgSvc->bestFileSystemForSegment(segment);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_anyTapeCopyForStream
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_anyTapeCopyForStream
  (Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Stream* searchItem) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      if (stgSvc->stgSvc->anyTapeCopyForStream(searchItem)) {
        return 1;
      } else {
        return 0;
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_bestTapeCopyForStream
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_bestTapeCopyForStream
  (Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Stream* searchItem,
   castor::stager::TapeCopyForMigration** tapecopy,
   char autocommit) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *tapecopy = stgSvc->stgSvc->bestTapeCopyForStream
        (searchItem, autocommit);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_streamsForTapePool
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_streamsForTapePool
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::TapePool * tapePool) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->streamsForTapePool(tapePool);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_fileRecalled
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_fileRecalled(Cstager_IStagerSvc_t* stgSvc,
                                      castor::stager::TapeCopy* tapeCopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->fileRecalled(tapeCopy);
      return 0;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_tapesToDo
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_tapesToDo(Cstager_IStagerSvc_t* stgSvc,
                                   castor::stager::Tape*** tapeArray,
                                   int *nbItems) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<castor::stager::Tape*> result =
        stgSvc->stgSvc->tapesToDo();
      *nbItems = result.size();
      *tapeArray = (castor::stager::Tape**)
        malloc((*nbItems) * sizeof(castor::stager::Tape*));
      for (int i = 0; i < *nbItems; i++) {
        (*tapeArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_streamsToDo
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_streamsToDo(Cstager_IStagerSvc_t* stgSvc,
                                     castor::stager::Stream*** streamArray,
                                     int *nbItems) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<castor::stager::Stream*> result =
        stgSvc->stgSvc->streamsToDo();
      *nbItems = result.size();
      *streamArray = (castor::stager::Stream**)
        malloc((*nbItems) * sizeof(castor::stager::Stream*));
      for (int i = 0; i < *nbItems; i++) {
        (*streamArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectTape
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectTape(struct Cstager_IStagerSvc_t* stgSvc,
                                    castor::stager::Tape** tape,
                                    const char* vid,
                                    const int side,
                                    const int tpmode) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *tape = stgSvc->stgSvc->selectTape(vid, side, tpmode);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
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
  // Cstager_IStagerSvc_requestToDo
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_requestToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                     enum castor::ObjectsIds* types,
                                     unsigned int nbTypes,
                                     castor::stager::Request** request) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<enum castor::ObjectsIds> cppTypes;
      for (unsigned int i = 0; i < nbTypes; i++) {
        cppTypes.push_back(types[i]);
      }
      *request = stgSvc->stgSvc->requestToDo(cppTypes);
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
  // Cstager_IStagerSvc_getUpdateStart
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_getUpdateStart
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::FileSystem* fileSystem,
   castor::stager::DiskCopyForRecall*** sources,
   unsigned int* sourcesNb,
   int *emptyFile,
   castor::stager::DiskCopy** diskCopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      bool ef;
      std::list<castor::stager::DiskCopyForRecall*> sourceslist;
      *diskCopy = stgSvc->stgSvc->getUpdateStart
        (subreq, fileSystem, sourceslist, &ef);
      *emptyFile = ef ? 1 : 0;
      *sourcesNb = sourceslist.size();
      if (*sourcesNb > 0) {
        *sources = (castor::stager::DiskCopyForRecall**)
          malloc((*sourcesNb) * sizeof(struct Cstager_DiskCopyForRecall_t*));
        std::list<castor::stager::DiskCopyForRecall*>::iterator it =
          sourceslist.begin();
        for (unsigned int i = 0; i < *sourcesNb; i++, it++) {
          (*sources)[i] = *it;
        }
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_putStart
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_putStart
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::FileSystem* fileSystem,
   castor::stager::DiskCopy** diskCopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *diskCopy =
        stgSvc->stgSvc->putStart(subreq, fileSystem);
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
  // Cstager_IStagerSvc_selectFileSystem
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectFileSystem
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::FileSystem** fileSystem,
   const char* mountPoint,
   const char* diskServer) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *fileSystem = stgSvc->stgSvc->selectFileSystem(mountPoint, diskServer);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectDiskPool
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectDiskPool(struct Cstager_IStagerSvc_t* stgSvc,
                                        castor::stager::DiskPool** diskPool,
                                        const char* name) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *diskPool = stgSvc->stgSvc->selectDiskPool(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectTapePool
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectTapePool(struct Cstager_IStagerSvc_t* stgSvc,
                                        castor::stager::TapePool** tapePool,
                                        const char* name) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *tapePool = stgSvc->stgSvc->selectTapePool(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectDiskServer
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectDiskServer
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::DiskServer** diskServer,
   const char* name) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *diskServer = stgSvc->stgSvc->selectDiskServer(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectTapeCopiesForMigration
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_selectTapeCopiesForMigration
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SvcClass* svcClass,
   castor::stager::TapeCopy*** tapeCopyArray,
   int *nbItems) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<castor::stager::TapeCopy*> result =
        stgSvc->stgSvc->selectTapeCopiesForMigration(svcClass);
      *nbItems = result.size();
      *tapeCopyArray = (castor::stager::TapeCopy**)
        malloc((*nbItems) * sizeof(castor::stager::TapeCopy*));
      for (int i = 0; i < *nbItems; i++) {
        (*tapeCopyArray)[i] = result[i];
      }
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
  // Cstager_IStagerSvc_disk2DiskCopyDone
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_disk2DiskCopyDone(struct Cstager_IStagerSvc_t* stgSvc,
                                           u_signed64 diskCopyId) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->disk2DiskCopyDone(diskCopyId);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_prepareForMigration
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_recreateCastorFile
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::CastorFile* castorFile,
   castor::stager::SubRequest *subreq,
   castor::stager::DiskCopy** diskCopy) {
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
  // Cstager_IStagerSvc_prepareForMigration
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_prepareForMigration
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   u_signed64 fileSize) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->prepareForMigration(subreq, fileSize);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IStagerSvc_resetStream
  //-------------------------------------------------------------------------
  int Cstager_IStagerSvc_resetStream
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Stream* stream) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      stgSvc->stgSvc->resetStream(stream);
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
   unsigned int fileSystemsNb, u_signed64 minFree,
   char** mountPoint, char** diskServer) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::vector<std::string> fss;
      std::vector<std::string> ms;
      for (unsigned int i = 0; i < fileSystemsNb; i++) {
        fss.push_back(fileSystems[i]);
        ms.push_back(machines[i]);
      }
      std::string mp, ds;
      stgSvc->stgSvc->bestFileSystemForJob
        (fss, ms, minFree, &mp, &ds);
      *mountPoint = strdup(mp.c_str());
      *diskServer = strdup(ds.c_str());
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;    
  }

}
