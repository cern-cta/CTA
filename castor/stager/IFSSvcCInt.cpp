/******************************************************************************
 *                      IFSSvcCInt.cpp
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
 * @(#)$RCSfile: IFSSvcCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 14:58:41 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/stager/IFSSvc.hpp"
#include "castor/Services.hpp"
#include <errno.h>
#include <string>

extern "C" {

  // C include files
#include "castor/ServicesCInt.hpp"
#include "castor/stager/IFSSvcCInt.hpp"

  //----------------------------------------------------------------------------
  // checkIFSSvc
  //----------------------------------------------------------------------------
  bool checkIFSSvc(Cstager_IFSSvc_t* fsSvc) {
    if (0 == fsSvc || 0 == fsSvc->fsSvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  // Cstager_IFSSvc_fromIService
  //----------------------------------------------------------------------------
  Cstager_IFSSvc_t*
  Cstager_IFSSvc_fromIService(castor::IService* obj) {
    Cstager_IFSSvc_t* fsSvc = new Cstager_IFSSvc_t();
    fsSvc->errorMsg = "";
    fsSvc->fsSvc = dynamic_cast<castor::stager::IFSSvc*>(obj);
    return fsSvc;
  }

  //----------------------------------------------------------------------------
  // Cstager_IFSSvc_delete
  //----------------------------------------------------------------------------
  int Cstager_IFSSvc_delete(Cstager_IFSSvc_t* fsSvc) {
    try {
      if (0 == fsSvc) return 0;
      if (0 != fsSvc->fsSvc) fsSvc->fsSvc->release();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete fsSvc;
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_requestToDo
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_requestToDo(struct Cstager_IFSSvc_t* fsSvc,
                                     enum castor::ObjectsIds* types,
                                     unsigned int nbTypes,
                                     castor::stager::Request** request) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      std::vector<enum castor::ObjectsIds> cppTypes;
      for (unsigned int i = 0; i < nbTypes; i++) {
        cppTypes.push_back(types[i]);
      }
      *request = fsSvc->fsSvc->requestToDo(cppTypes);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  };

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_errorMsg
  //-------------------------------------------------------------------------
  const char* Cstager_IFSSvc_errorMsg(Cstager_IFSSvc_t* fsSvc) {
    return fsSvc->errorMsg.c_str();
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectSvcClass
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectSvcClass(struct Cstager_IFSSvc_t* fsSvc,
                                        castor::stager::SvcClass** svcClass,
                                        const char* name) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *svcClass = fsSvc->fsSvc->selectSvcClass(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectFileClass
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectFileClass(struct Cstager_IFSSvc_t* fsSvc,
                                         castor::stager::FileClass** fileClass,
                                         const char* name) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *fileClass = fsSvc->fsSvc->selectFileClass(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectFileSystem
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectFileSystem
  (struct Cstager_IFSSvc_t* fsSvc,
   castor::stager::FileSystem** fileSystem,
   const char* mountPoint,
   const char* diskServer) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *fileSystem = fsSvc->fsSvc->selectFileSystem(mountPoint, diskServer);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectDiskPool
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectDiskPool(struct Cstager_IFSSvc_t* fsSvc,
                                        castor::stager::DiskPool** diskPool,
                                        const char* name) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *diskPool = fsSvc->fsSvc->selectDiskPool(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectTapePool
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectTapePool(struct Cstager_IFSSvc_t* fsSvc,
                                        castor::stager::TapePool** tapePool,
                                        const char* name) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *tapePool = fsSvc->fsSvc->selectTapePool(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IFSSvc_selectDiskServer
  //-------------------------------------------------------------------------
  int Cstager_IFSSvc_selectDiskServer
  (struct Cstager_IFSSvc_t* fsSvc,
   castor::stager::DiskServer** diskServer,
   const char* name) {
    if (!checkIFSSvc(fsSvc)) return -1;
    try {
      *diskServer = fsSvc->fsSvc->selectDiskServer(name);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      fsSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

}
