/******************************************************************************
 *                      castor/stager/DiskServerCInt.cpp
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
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/stager/FileSystem.hpp"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_create
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_create(castor::stager::DiskServer** obj) {
    *obj = new castor::stager::DiskServer();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_DiskServer_delete
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_delete(castor::stager::DiskServer* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_DiskServer_getIObject(castor::stager::DiskServer* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::DiskServer* Cstager_DiskServer_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::DiskServer*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_print
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_print(castor::stager::DiskServer* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_TYPE
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_TYPE(int* ret) {
    *ret = castor::stager::DiskServer::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_setId
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_setId(castor::stager::DiskServer* instance,
                               unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_id
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_id(castor::stager::DiskServer* instance,
                            unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_type
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_type(castor::stager::DiskServer* instance,
                              int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_name
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_name(castor::stager::DiskServer* instance, const char** var) {
    *var = instance->name().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_setName
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_setName(castor::stager::DiskServer* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_addFileSystems
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_addFileSystems(castor::stager::DiskServer* instance, castor::stager::FileSystem* obj) {
    instance->addFileSystems(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_removeFileSystems
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_removeFileSystems(castor::stager::DiskServer* instance, castor::stager::FileSystem* obj) {
    instance->removeFileSystems(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_fileSystems
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_fileSystems(castor::stager::DiskServer* instance, castor::stager::FileSystem*** var, int* len) {
    std::vector<castor::stager::FileSystem*> result = instance->fileSystems();
    *len = result.size();
    *var = (castor::stager::FileSystem**) malloc((*len) * sizeof(castor::stager::FileSystem*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_status
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_status(castor::stager::DiskServer* instance, castor::stager::DiskServerStatusCode* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskServer_setStatus
  //----------------------------------------------------------------------------
  int Cstager_DiskServer_setStatus(castor::stager::DiskServer* instance, castor::stager::DiskServerStatusCode new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
