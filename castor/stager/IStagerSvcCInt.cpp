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
 * @(#)$RCSfile: IStagerSvcCInt.cpp,v $ $Revision: 1.68 $ $Release$ $Date: 2008/12/08 14:08:47 $ $Author: sponcec3 $
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
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete stgSvc;
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
    } catch (castor::exception::Exception& e) {
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
    } catch (castor::exception::Exception& e) {
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
    } catch (castor::exception::Exception& e) {
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
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      stgSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

}
