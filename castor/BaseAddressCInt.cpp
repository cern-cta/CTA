/******************************************************************************
 *                      castor/BaseAddressCInt.cpp
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
#include "castor/BaseAddress.hpp"
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // C_BaseAddress_create
  //----------------------------------------------------------------------------
  int C_BaseAddress_create(castor::BaseAddress** obj) {
    *obj = new castor::BaseAddress();
    return 0;
  }
  //----------------------------------------------------------------------------
  // C_BaseAddress_delete
  //----------------------------------------------------------------------------
  int C_BaseAddress_delete(castor::BaseAddress* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_getIAddress
  //----------------------------------------------------------------------------
  castor::IAddress* C_BaseAddress_getIAddress(castor::BaseAddress* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_fromIAddress
  //----------------------------------------------------------------------------
  castor::BaseAddress* C_BaseAddress_fromIAddress(castor::IAddress* obj) {
    return dynamic_cast<castor::BaseAddress*>(obj);
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* C_BaseAddress_getIObject(castor::BaseAddress* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_fromIObject
  //----------------------------------------------------------------------------
  castor::BaseAddress* C_BaseAddress_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::BaseAddress*>(obj);
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_print
  //----------------------------------------------------------------------------
  int C_BaseAddress_print(castor::BaseAddress* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_TYPE
  //----------------------------------------------------------------------------
  int C_BaseAddress_TYPE(int* ret) {
    *ret = castor::BaseAddress::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_type
  //----------------------------------------------------------------------------
  int C_BaseAddress_type(castor::BaseAddress* instance,
                         int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_clone
  //----------------------------------------------------------------------------
  int C_BaseAddress_clone(castor::BaseAddress* instance,
                          castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_objType
  //----------------------------------------------------------------------------
  int C_BaseAddress_objType(castor::BaseAddress* instance, unsigned int* var) {
    *var = instance->objType();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_setObjType
  //----------------------------------------------------------------------------
  int C_BaseAddress_setObjType(castor::BaseAddress* instance, unsigned int new_var) {
    instance->setObjType(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_cnvSvcName
  //----------------------------------------------------------------------------
  int C_BaseAddress_cnvSvcName(castor::BaseAddress* instance, const char** var) {
    *var = instance->cnvSvcName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_setCnvSvcName
  //----------------------------------------------------------------------------
  int C_BaseAddress_setCnvSvcName(castor::BaseAddress* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setCnvSvcName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_cnvSvcType
  //----------------------------------------------------------------------------
  int C_BaseAddress_cnvSvcType(castor::BaseAddress* instance, unsigned int* var) {
    *var = instance->cnvSvcType();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_setCnvSvcType
  //----------------------------------------------------------------------------
  int C_BaseAddress_setCnvSvcType(castor::BaseAddress* instance, unsigned int new_var) {
    instance->setCnvSvcType(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_target
  //----------------------------------------------------------------------------
  int C_BaseAddress_target(castor::BaseAddress* instance, u_signed64* var) {
    *var = instance->target();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_setTarget
  //----------------------------------------------------------------------------
  int C_BaseAddress_setTarget(castor::BaseAddress* instance, u_signed64 new_var) {
    instance->setTarget(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_id
  //----------------------------------------------------------------------------
  int C_BaseAddress_id(castor::BaseAddress* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_setId
  //----------------------------------------------------------------------------
  int C_BaseAddress_setId(castor::BaseAddress* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
