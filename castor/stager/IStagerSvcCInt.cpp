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
 * @(#)$RCSfile: IStagerSvcCInt.cpp,v $ $Revision: 1.10 $ $Release$ $Date: 2004/10/28 16:48:56 $ $Author: sponcec3 $
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

  //------------------------------------------------------------------------------
  // checkIStagerSvc
  //------------------------------------------------------------------------------
  bool checkIStagerSvc(Cstager_IStagerSvc_t* stgSvc) {
    if (0 == stgSvc || 0 == stgSvc->stgSvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //------------------------------------------------------------------------------
  // Cstager_IStagerSvc_fromIService
  //------------------------------------------------------------------------------
  Cstager_IStagerSvc_t*
  Cstager_IStagerSvc_fromIService(castor::IService* obj) {
    Cstager_IStagerSvc_t* stgSvc = new Cstager_IStagerSvc_t();
    stgSvc->errorMsg = "";
    stgSvc->stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(obj);
    return stgSvc;
  }

  //------------------------------------------------------------------------------
  // Cstager_IStagerSvc_delete
  //------------------------------------------------------------------------------
  int Cstager_IStagerSvc_delete(Cstager_IStagerSvc_t* stgSvc) {
    try {
      if (0 == stgSvc) return 0;
      if (0 != stgSvc->stgSvc) delete stgSvc->stgSvc;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    free(stgSvc);
    return 0;
  }

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_anySegmentsForTape
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_segmentsForTape
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_segmentsForTape(Cstager_IStagerSvc_t* stgSvc,
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
  
  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_bestFileSystemForSegment
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_anyTapeCopyForStream
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_anyTapeCopyForStream(Cstager_IStagerSvc_t* stgSvc,
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_bestTapeCopyForStream
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_bestTapeCopyForStream
  (Cstager_IStagerSvc_t* stgSvc,
   castor::stager::Stream* searchItem,
   castor::stager::TapeCopyForMigration** tapecopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *tapecopy = stgSvc->stgSvc->bestTapeCopyForStream(searchItem);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_fileRecalled
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_tapesToDo
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_streamsToDo
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_selectTape
  //---------------------------------------------------------------------------
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_subRequestToDo
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_subRequestToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                        castor::stager::SubRequest** subreq) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      *subreq = stgSvc->stgSvc->subRequestToDo();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  };

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_isSubRequestToSchedule
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_isSubRequestToSchedule
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      if (stgSvc->stgSvc->isSubRequestToSchedule(subreq)) {
        return 1;
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_scheduleSubRequest
  //---------------------------------------------------------------------------
  int Cstager_IStagerSvc_scheduleSubRequest
  (struct Cstager_IStagerSvc_t* stgSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::FileSystem* fileSystem,
   castor::stager::DiskCopyForRecall*** sources,
   unsigned int* sourcesNb,
   castor::stager::DiskCopy** diskCopy) {
    if (!checkIStagerSvc(stgSvc)) return -1;
    try {
      std::list<castor::stager::DiskCopyForRecall*> sourceslist;
      *diskCopy = stgSvc->stgSvc->scheduleSubRequest
        (subreq, fileSystem, sourceslist);
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

  //---------------------------------------------------------------------------
  // Cstager_IStagerSvc_errorMsg
  //---------------------------------------------------------------------------
  const char* Cstager_IStagerSvc_errorMsg(Cstager_IStagerSvc_t* stgSvc) {
    return stgSvc->errorMsg.c_str();
  }
  
}
