/******************************************************************************
 *                      ITapeSvcCInt.cpp
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
 * @(#)$RCSfile: ITapeSvcCInt.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/05/11 12:46:39 $ $Author: felixehm $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/stager/ITapeSvc.hpp"
#include "castor/Services.hpp"
#include <errno.h>
#include <string>

extern "C" {

  // C include files
#include "castor/ServicesCInt.hpp"
#include "castor/stager/ITapeSvcCInt.hpp"

  //----------------------------------------------------------------------------
  // checkITapeSvc
  //----------------------------------------------------------------------------
  bool checkITapeSvc(Cstager_ITapeSvc_t* tpSvc) {
    if (0 == tpSvc || 0 == tpSvc->tpSvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  // Cstager_ITapeSvc_fromIService
  //----------------------------------------------------------------------------
  Cstager_ITapeSvc_t*
  Cstager_ITapeSvc_fromIService(castor::IService* obj) {
    Cstager_ITapeSvc_t* tpSvc = new Cstager_ITapeSvc_t();
    tpSvc->errorMsg = "";
    tpSvc->tpSvc = dynamic_cast<castor::stager::ITapeSvc*>(obj);
    return tpSvc;
  }

  //----------------------------------------------------------------------------
  // Cstager_ITapeSvc_delete
  //----------------------------------------------------------------------------
  int Cstager_ITapeSvc_delete(Cstager_ITapeSvc_t* tpSvc) {
    try {
      if (0 == tpSvc) return 0;
      if (0 != tpSvc->tpSvc) tpSvc->tpSvc->release();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete tpSvc;
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_anySegmentsForTape
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_anySegmentsForTape(Cstager_ITapeSvc_t* tpSvc,
                                            castor::stager::Tape* searchItem) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      return tpSvc->tpSvc->anySegmentsForTape(searchItem);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_segmentsForTape
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_segmentsForTape
  (Cstager_ITapeSvc_t* tpSvc,
   castor::stager::Tape* searchItem,
   castor::stager::Segment*** segmentArray,
   int* nbItems) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      std::vector<castor::stager::Segment*> result =
        tpSvc->tpSvc->segmentsForTape(searchItem);
      *nbItems = result.size();
      *segmentArray = (castor::stager::Segment**)
        malloc((*nbItems) * sizeof(castor::stager::Segment*));
      for (int i = 0; i < *nbItems; i++) {
        (*segmentArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_bestFileSystemForSegment
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_bestFileSystemForSegment
  (Cstager_ITapeSvc_t* tpSvc,
   castor::stager::Segment* segment,
   castor::stager::DiskCopyForRecall** diskcopy) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      *diskcopy = tpSvc->tpSvc->bestFileSystemForSegment(segment);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_anyTapeCopyForStream
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_anyTapeCopyForStream
  (Cstager_ITapeSvc_t* tpSvc,
   castor::stager::Stream* searchItem) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      if (tpSvc->tpSvc->anyTapeCopyForStream(searchItem)) {
        return 1;
      } else {
        return 0;
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_bestTapeCopyForStream
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_bestTapeCopyForStream
  (Cstager_ITapeSvc_t* tpSvc,
   castor::stager::Stream* searchItem,
   castor::stager::TapeCopyForMigration** tapecopy) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      *tapecopy =
        tpSvc->tpSvc->bestTapeCopyForStream(searchItem);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_streamsForTapePool
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_streamsForTapePool
  (struct Cstager_ITapeSvc_t* tpSvc,
   castor::stager::TapePool * tapePool) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      tpSvc->tpSvc->streamsForTapePool(tapePool);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_fileRecalled
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_fileRecalled(Cstager_ITapeSvc_t* tpSvc,
                                      castor::stager::TapeCopy* tapeCopy) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      tpSvc->tpSvc->fileRecalled(tapeCopy);
      return 0;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_fileRecallFailed
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_fileRecallFailed(Cstager_ITapeSvc_t* tpSvc,
                                          castor::stager::TapeCopy* tapeCopy) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      tpSvc->tpSvc->fileRecallFailed(tapeCopy);
      return 0;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_tapesToDo
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_tapesToDo(Cstager_ITapeSvc_t* tpSvc,
                                   castor::stager::Tape*** tapeArray,
                                   int *nbItems) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      std::vector<castor::stager::Tape*> result =
        tpSvc->tpSvc->tapesToDo();
      *nbItems = result.size();
      *tapeArray = (castor::stager::Tape**)
        malloc((*nbItems) * sizeof(castor::stager::Tape*));
      for (int i = 0; i < *nbItems; i++) {
        (*tapeArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_streamsToDo
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_streamsToDo(Cstager_ITapeSvc_t* tpSvc,
                                     castor::stager::Stream*** streamArray,
                                     int *nbItems) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      std::vector<castor::stager::Stream*> result =
        tpSvc->tpSvc->streamsToDo();
      *nbItems = result.size();
      *streamArray = (castor::stager::Stream**)
        malloc((*nbItems) * sizeof(castor::stager::Stream*));
      for (int i = 0; i < *nbItems; i++) {
        (*streamArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_selectTape
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_selectTape(struct Cstager_ITapeSvc_t* tpSvc,
                                    castor::stager::Tape** tape,
                                    const char* vid,
                                    const int side,
                                    const int tpmode) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      *tape = tpSvc->tpSvc->selectTape(vid, side, tpmode);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_errorMsg
  //-------------------------------------------------------------------------
  const char* Cstager_ITapeSvc_errorMsg(Cstager_ITapeSvc_t* tpSvc) {
    return tpSvc->errorMsg.c_str();
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_selectSvcClass
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_selectSvcClass(struct Cstager_ITapeSvc_t* tpSvc,
                                        castor::stager::SvcClass** svcClass,
                                        const char* name) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      *svcClass = tpSvc->tpSvc->selectSvcClass(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_selectTapeCopiesForMigration
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_selectTapeCopiesForMigration
  (struct Cstager_ITapeSvc_t* tpSvc,
   castor::stager::SvcClass* svcClass,
   castor::stager::TapeCopy*** tapeCopyArray,
   int *nbItems) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      std::vector<castor::stager::TapeCopy*> result =
        tpSvc->tpSvc->selectTapeCopiesForMigration(svcClass);
      *nbItems = result.size();
      *tapeCopyArray = (castor::stager::TapeCopy**)
        malloc((*nbItems) * sizeof(castor::stager::TapeCopy*));
      for (int i = 0; i < *nbItems; i++) {
        (*tapeCopyArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_resetStream
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_resetStream
  (struct Cstager_ITapeSvc_t* tpSvc,
   castor::stager::Stream* stream) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      tpSvc->tpSvc->resetStream(stream);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_ITapeSvc_failedSegments
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_failedSegments
  (struct Cstager_ITapeSvc_t* tpSvc,
   struct castor::stager::Segment*** segmentArray,
   int* nbItems) {
    if (!checkITapeSvc(tpSvc)) return -1;
    try {
      std::vector<castor::stager::Segment*> result =
        tpSvc->tpSvc->failedSegments();
      *nbItems = result.size();
      *segmentArray = (castor::stager::Segment**)
        malloc((*nbItems) * sizeof(castor::stager::Segment*));
      for (int i = 0; i < *nbItems; i++) {
        (*segmentArray)[i] = result[i];
      }
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }


  //-------------------------------------------------------------------------
  // C_stager_ITapeSvc_getSubRequestForFile
  //-------------------------------------------------------------------------
  int Cstager_ITapeSvc_checkFileForRepack
  (struct Cstager_ITapeSvc_t* tpSvc,
   castor::stager::SubRequest** subRequest,
   const u_signed64 fileid) {
    if (!checkITapeSvc(tpSvc) ) return -1;
    try {
      *subRequest = tpSvc->tpSvc->checkFileForRepack(fileid);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      tpSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }





}
