/******************************************************************************
 *                      castor/stager/ReqIdCInt.cpp
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
 * @(#)$RCSfile: ReqIdCInt.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/04 10:26:11 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/ReqId.hpp"
#include "castor/stager/ReqIdRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_ReqId_create
  //----------------------------------------------------------------------------
  int Cstager_ReqId_create(castor::stager::ReqId** obj) {
    *obj = new castor::stager::ReqId();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_ReqId_delete
  //----------------------------------------------------------------------------
  int Cstager_ReqId_delete(castor::stager::ReqId* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_ReqId_getIObject(castor::stager::ReqId* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::ReqId* Cstager_ReqId_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::ReqId*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_print
  //----------------------------------------------------------------------------
  int Cstager_ReqId_print(castor::stager::ReqId* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_TYPE
  //----------------------------------------------------------------------------
  int Cstager_ReqId_TYPE(int* ret) {
    *ret = castor::stager::ReqId::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_setId
  //----------------------------------------------------------------------------
  int Cstager_ReqId_setId(castor::stager::ReqId* instance,
                          u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_id
  //----------------------------------------------------------------------------
  int Cstager_ReqId_id(castor::stager::ReqId* instance,
                       u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_type
  //----------------------------------------------------------------------------
  int Cstager_ReqId_type(castor::stager::ReqId* instance,
                         int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_value
  //----------------------------------------------------------------------------
  int Cstager_ReqId_value(castor::stager::ReqId* instance, const char** var) {
    *var = instance->value().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_setValue
  //----------------------------------------------------------------------------
  int Cstager_ReqId_setValue(castor::stager::ReqId* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setValue(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_request
  //----------------------------------------------------------------------------
  int Cstager_ReqId_request(castor::stager::ReqId* instance, castor::stager::ReqIdRequest** var) {
    *var = instance->request();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqId_setRequest
  //----------------------------------------------------------------------------
  int Cstager_ReqId_setRequest(castor::stager::ReqId* instance, castor::stager::ReqIdRequest* new_var) {
    instance->setRequest(new_var);
    return 0;
  }

} // End of extern "C"
