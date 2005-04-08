/******************************************************************************
 *                      castor/stager/GCRemovedFileCInt.cpp
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
 * @(#)$RCSfile: GCRemovedFileCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/04/08 08:50:48 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/GCRemovedFile.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_create
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_create(castor::stager::GCRemovedFile** obj) {
    *obj = new castor::stager::GCRemovedFile();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_delete
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_delete(castor::stager::GCRemovedFile* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_GCRemovedFile_getIObject(castor::stager::GCRemovedFile* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::GCRemovedFile* Cstager_GCRemovedFile_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::GCRemovedFile*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_print
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_print(castor::stager::GCRemovedFile* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_TYPE
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_TYPE(int* ret) {
    *ret = castor::stager::GCRemovedFile::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_type
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_type(castor::stager::GCRemovedFile* instance,
                                 int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_clone
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_clone(castor::stager::GCRemovedFile* instance,
                                  castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_diskCopyId
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_diskCopyId(castor::stager::GCRemovedFile* instance, u_signed64* var) {
    *var = instance->diskCopyId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_setDiskCopyId
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_setDiskCopyId(castor::stager::GCRemovedFile* instance, u_signed64 new_var) {
    instance->setDiskCopyId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_id
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_id(castor::stager::GCRemovedFile* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_setId
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_setId(castor::stager::GCRemovedFile* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_request
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_request(castor::stager::GCRemovedFile* instance, castor::stager::FilesDeleted** var) {
    *var = instance->request();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GCRemovedFile_setRequest
  //----------------------------------------------------------------------------
  int Cstager_GCRemovedFile_setRequest(castor::stager::GCRemovedFile* instance, castor::stager::FilesDeleted* new_var) {
    instance->setRequest(new_var);
    return 0;
  }

} // End of extern "C"
