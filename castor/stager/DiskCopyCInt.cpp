/******************************************************************************
 *                      castor/stager/DiskCopyCInt.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyStatusCode.hpp"
#include "castor/stager/FileSystem.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_create
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_create(castor::stager::DiskCopy** obj) {
    *obj = new castor::stager::DiskCopy();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_delete
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_delete(castor::stager::DiskCopy* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_DiskCopy_getIObject(castor::stager::DiskCopy* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::DiskCopy* Cstager_DiskCopy_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::DiskCopy*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_print
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_print(castor::stager::DiskCopy* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_TYPE
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_TYPE(int* ret) {
    *ret = castor::stager::DiskCopy::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_setId
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_setId(castor::stager::DiskCopy* instance,
                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_id
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_id(castor::stager::DiskCopy* instance,
                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_type
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_type(castor::stager::DiskCopy* instance,
                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_path
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_path(castor::stager::DiskCopy* instance, const char** var) {
    *var = instance->path().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_setPath
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_setPath(castor::stager::DiskCopy* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setPath(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_fileSystem
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_fileSystem(castor::stager::DiskCopy* instance, castor::stager::FileSystem** var) {
    *var = instance->fileSystem();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_setFileSystem
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_setFileSystem(castor::stager::DiskCopy* instance, castor::stager::FileSystem* new_var) {
    instance->setFileSystem(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_castorFile
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_castorFile(castor::stager::DiskCopy* instance, castor::stager::CastorFile** var) {
    *var = instance->castorFile();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_setCastorFile
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_setCastorFile(castor::stager::DiskCopy* instance, castor::stager::CastorFile* new_var) {
    instance->setCastorFile(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_status
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_status(castor::stager::DiskCopy* instance, castor::stager::DiskCopyStatusCode* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopy_setStatus
  //----------------------------------------------------------------------------
  int Cstager_DiskCopy_setStatus(castor::stager::DiskCopy* instance, castor::stager::DiskCopyStatusCode new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
