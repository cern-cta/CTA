/******************************************************************************
 *                      castor/stager/SvcClassCInt.cpp
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
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_create
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_create(castor::stager::SvcClass** obj) {
    *obj = new castor::stager::SvcClass();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_SvcClass_delete
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_delete(castor::stager::SvcClass* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_SvcClass_getIObject(castor::stager::SvcClass* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::SvcClass* Cstager_SvcClass_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::SvcClass*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_print
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_print(castor::stager::SvcClass* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_TYPE
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_TYPE(int* ret) {
    *ret = castor::stager::SvcClass::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_setId
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_setId(castor::stager::SvcClass* instance,
                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_id
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_id(castor::stager::SvcClass* instance,
                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_type
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_type(castor::stager::SvcClass* instance,
                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_policy
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_policy(castor::stager::SvcClass* instance, const char** var) {
    *var = instance->policy().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_setPolicy
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_setPolicy(castor::stager::SvcClass* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setPolicy(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_nbDrives
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_nbDrives(castor::stager::SvcClass* instance, unsigned int* var) {
    *var = instance->nbDrives();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_setNbDrives
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_setNbDrives(castor::stager::SvcClass* instance, unsigned int new_var) {
    instance->setNbDrives(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_name
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_name(castor::stager::SvcClass* instance, const char** var) {
    *var = instance->name().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SvcClass_setName
  //----------------------------------------------------------------------------
  int Cstager_SvcClass_setName(castor::stager::SvcClass* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setName(snew_var);
    return 0;
  }

} // End of extern "C"
