/******************************************************************************
 *                      castor/stager/TapePoolCInt.cpp
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
#include "castor/stager/Stream.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_TapePool_create
  //----------------------------------------------------------------------------
  int Cstager_TapePool_create(castor::stager::TapePool** obj) {
    *obj = new castor::stager::TapePool();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_TapePool_delete
  //----------------------------------------------------------------------------
  int Cstager_TapePool_delete(castor::stager::TapePool* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_TapePool_getIObject(castor::stager::TapePool* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::TapePool* Cstager_TapePool_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::TapePool*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_print
  //----------------------------------------------------------------------------
  int Cstager_TapePool_print(castor::stager::TapePool* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_TYPE
  //----------------------------------------------------------------------------
  int Cstager_TapePool_TYPE(int* ret) {
    *ret = castor::stager::TapePool::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_setId
  //----------------------------------------------------------------------------
  int Cstager_TapePool_setId(castor::stager::TapePool* instance,
                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_id
  //----------------------------------------------------------------------------
  int Cstager_TapePool_id(castor::stager::TapePool* instance,
                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_type
  //----------------------------------------------------------------------------
  int Cstager_TapePool_type(castor::stager::TapePool* instance,
                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_name
  //----------------------------------------------------------------------------
  int Cstager_TapePool_name(castor::stager::TapePool* instance, const char** var) {
    *var = instance->name().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_setName
  //----------------------------------------------------------------------------
  int Cstager_TapePool_setName(castor::stager::TapePool* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_addSvcClasses
  //----------------------------------------------------------------------------
  int Cstager_TapePool_addSvcClasses(castor::stager::TapePool* instance, castor::stager::SvcClass* obj) {
    instance->addSvcClasses(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_removeSvcClasses
  //----------------------------------------------------------------------------
  int Cstager_TapePool_removeSvcClasses(castor::stager::TapePool* instance, castor::stager::SvcClass* obj) {
    instance->removeSvcClasses(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_svcClasses
  //----------------------------------------------------------------------------
  int Cstager_TapePool_svcClasses(castor::stager::TapePool* instance, castor::stager::SvcClass*** var, int* len) {
    std::vector<castor::stager::SvcClass*> result = instance->svcClasses();
    *len = result.size();
    *var = (castor::stager::SvcClass**) malloc((*len) * sizeof(castor::stager::SvcClass*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_addStreams
  //----------------------------------------------------------------------------
  int Cstager_TapePool_addStreams(castor::stager::TapePool* instance, castor::stager::Stream* obj) {
    instance->addStreams(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_removeStreams
  //----------------------------------------------------------------------------
  int Cstager_TapePool_removeStreams(castor::stager::TapePool* instance, castor::stager::Stream* obj) {
    instance->removeStreams(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_streams
  //----------------------------------------------------------------------------
  int Cstager_TapePool_streams(castor::stager::TapePool* instance, castor::stager::Stream*** var, int* len) {
    std::vector<castor::stager::Stream*> result = instance->streams();
    *len = result.size();
    *var = (castor::stager::Stream**) malloc((*len) * sizeof(castor::stager::Stream*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
