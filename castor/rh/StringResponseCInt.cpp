/******************************************************************************
 *                      castor/rh/StringResponseCInt.cpp
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
#include "castor/rh/Response.hpp"
#include "castor/rh/StringResponse.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_StringResponse_create
  //----------------------------------------------------------------------------
  int Crh_StringResponse_create(castor::rh::StringResponse** obj) {
    *obj = new castor::rh::StringResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_StringResponse_delete
  //----------------------------------------------------------------------------
  int Crh_StringResponse_delete(castor::rh::StringResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_StringResponse_getResponse(castor::rh::StringResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::StringResponse* Crh_StringResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::StringResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_StringResponse_getIObject(castor::rh::StringResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::StringResponse* Crh_StringResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::StringResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_print
  //----------------------------------------------------------------------------
  int Crh_StringResponse_print(castor::rh::StringResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_StringResponse_TYPE(int* ret) {
    *ret = castor::rh::StringResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_setId
  //----------------------------------------------------------------------------
  int Crh_StringResponse_setId(castor::rh::StringResponse* instance,
                               u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_id
  //----------------------------------------------------------------------------
  int Crh_StringResponse_id(castor::rh::StringResponse* instance,
                            u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_type
  //----------------------------------------------------------------------------
  int Crh_StringResponse_type(castor::rh::StringResponse* instance,
                              int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_clone
  //----------------------------------------------------------------------------
  int Crh_StringResponse_clone(castor::rh::StringResponse* instance,
                               castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_content
  //----------------------------------------------------------------------------
  int Crh_StringResponse_content(castor::rh::StringResponse* instance, const char** var) {
    *var = instance->content().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StringResponse_setContent
  //----------------------------------------------------------------------------
  int Crh_StringResponse_setContent(castor::rh::StringResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setContent(snew_var);
    return 0;
  }

} // End of extern "C"
