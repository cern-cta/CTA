/******************************************************************************
 *                      castor/rh/FindReqResponseCInt.cpp
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
#include "castor/rh/FindReqResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_create
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_create(castor::rh::FindReqResponse** obj) {
    *obj = new castor::rh::FindReqResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_delete
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_delete(castor::rh::FindReqResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_FindReqResponse_getResponse(castor::rh::FindReqResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::FindReqResponse* Crh_FindReqResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::FindReqResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_FindReqResponse_getIObject(castor::rh::FindReqResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::FindReqResponse* Crh_FindReqResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::FindReqResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_print
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_print(castor::rh::FindReqResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_TYPE(int* ret) {
    *ret = castor::rh::FindReqResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_setId
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_setId(castor::rh::FindReqResponse* instance,
                                u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_id
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_id(castor::rh::FindReqResponse* instance,
                             u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_type
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_type(castor::rh::FindReqResponse* instance,
                               int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_reqId
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_reqId(castor::rh::FindReqResponse* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FindReqResponse_setReqId
  //----------------------------------------------------------------------------
  int Crh_FindReqResponse_setReqId(castor::rh::FindReqResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

} // End of extern "C"
