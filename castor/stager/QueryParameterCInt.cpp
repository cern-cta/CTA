/******************************************************************************
 *                      castor/stager/QueryParameterCInt.cpp
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
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_create
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_create(castor::stager::QueryParameter** obj) {
    *obj = new castor::stager::QueryParameter();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_delete
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_delete(castor::stager::QueryParameter* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_QueryParameter_getIObject(castor::stager::QueryParameter* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::QueryParameter* Cstager_QueryParameter_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::QueryParameter*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_print
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_print(castor::stager::QueryParameter* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_TYPE
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_TYPE(int* ret) {
    *ret = castor::stager::QueryParameter::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_type
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_type(castor::stager::QueryParameter* instance,
                                  int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_clone
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_clone(castor::stager::QueryParameter* instance,
                                   castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_value
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_value(castor::stager::QueryParameter* instance, const char** var) {
    *var = instance->value().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_setValue
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_setValue(castor::stager::QueryParameter* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setValue(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_id
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_id(castor::stager::QueryParameter* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_setId
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_setId(castor::stager::QueryParameter* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_queryType
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_queryType(castor::stager::QueryParameter* instance, castor::stager::RequestQueryType* var) {
    *var = instance->queryType();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QueryParameter_setQueryType
  //----------------------------------------------------------------------------
  int Cstager_QueryParameter_setQueryType(castor::stager::QueryParameter* instance, castor::stager::RequestQueryType new_var) {
    instance->setQueryType(new_var);
    return 0;
  }

} // End of extern "C"
