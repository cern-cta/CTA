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
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapePool.hpp"
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
                             unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_id
  //----------------------------------------------------------------------------
  int Cstager_TapePool_id(castor::stager::TapePool* instance,
                          unsigned long* ret) {
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
  // Cstager_TapePool_addTapes
  //----------------------------------------------------------------------------
  int Cstager_TapePool_addTapes(castor::stager::TapePool* instance, castor::stager::Tape* obj) {
    instance->addTapes(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_removeTapes
  //----------------------------------------------------------------------------
  int Cstager_TapePool_removeTapes(castor::stager::TapePool* instance, castor::stager::Tape* obj) {
    instance->removeTapes(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapePool_tapes
  //----------------------------------------------------------------------------
  int Cstager_TapePool_tapes(castor::stager::TapePool* instance, castor::stager::Tape*** var, int* len) {
    std::vector<castor::stager::Tape*> result = instance->tapes();
    *len = result.size();
    *var = (castor::stager::Tape**) malloc((*len) * sizeof(castor::stager::Tape*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
