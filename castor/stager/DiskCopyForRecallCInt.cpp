/******************************************************************************
 *                      castor/stager/DiskCopyForRecallCInt.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/SubRequest.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_create
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_create(castor::stager::DiskCopyForRecall** obj) {
    *obj = new castor::stager::DiskCopyForRecall();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_delete
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_delete(castor::stager::DiskCopyForRecall* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_getDiskCopy
  //----------------------------------------------------------------------------
  castor::stager::DiskCopy* Cstager_DiskCopyForRecall_getDiskCopy(castor::stager::DiskCopyForRecall* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_fromDiskCopy
  //----------------------------------------------------------------------------
  castor::stager::DiskCopyForRecall* Cstager_DiskCopyForRecall_fromDiskCopy(castor::stager::DiskCopy* obj) {
    return dynamic_cast<castor::stager::DiskCopyForRecall*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_DiskCopyForRecall_getIObject(castor::stager::DiskCopyForRecall* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::DiskCopyForRecall* Cstager_DiskCopyForRecall_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::DiskCopyForRecall*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_print
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_print(castor::stager::DiskCopyForRecall* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_TYPE
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_TYPE(int* ret) {
    *ret = castor::stager::DiskCopyForRecall::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_type
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_type(castor::stager::DiskCopyForRecall* instance,
                                     int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_path
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_path(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->path().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setPath
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setPath(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setPath(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_diskcopyId
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_diskcopyId(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->diskcopyId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setDiskcopyId
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setDiskcopyId(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskcopyId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_id
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_id(castor::stager::DiskCopyForRecall* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setId
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setId(castor::stager::DiskCopyForRecall* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_addSubRequests(castor::stager::DiskCopyForRecall* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_removeSubRequests(castor::stager::DiskCopyForRecall* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_subRequests
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_subRequests(castor::stager::DiskCopyForRecall* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_fileSystem
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_fileSystem(castor::stager::DiskCopyForRecall* instance, castor::stager::FileSystem** var) {
    *var = instance->fileSystem();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setFileSystem
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setFileSystem(castor::stager::DiskCopyForRecall* instance, castor::stager::FileSystem* new_var) {
    instance->setFileSystem(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_castorFile
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_castorFile(castor::stager::DiskCopyForRecall* instance, castor::stager::CastorFile** var) {
    *var = instance->castorFile();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setCastorFile
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setCastorFile(castor::stager::DiskCopyForRecall* instance, castor::stager::CastorFile* new_var) {
    instance->setCastorFile(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_status
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_status(castor::stager::DiskCopyForRecall* instance, castor::stager::DiskCopyStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setStatus
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setStatus(castor::stager::DiskCopyForRecall* instance, castor::stager::DiskCopyStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_clone
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_clone(castor::stager::DiskCopyForRecall* instance,
                                      castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_mountPoint
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_mountPoint(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->mountPoint().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setMountPoint
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setMountPoint(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMountPoint(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_diskServer
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_diskServer(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setDiskServer(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

} // End of extern "C"
