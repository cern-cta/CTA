/******************************************************************************
 *                      castor/rh/AbortResponseCInt.cpp
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
#include "castor/rh/AbortResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_create
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_create(castor::rh::AbortResponse** obj) {
    *obj = new castor::rh::AbortResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_AbortResponse_delete
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_delete(castor::rh::AbortResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_AbortResponse_getResponse(castor::rh::AbortResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::AbortResponse* Crh_AbortResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::AbortResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_AbortResponse_getIObject(castor::rh::AbortResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::AbortResponse* Crh_AbortResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::AbortResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_print
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_print(castor::rh::AbortResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_TYPE(int* ret) {
    *ret = castor::rh::AbortResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_errorCode(castor::rh::AbortResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_setErrorCode(castor::rh::AbortResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_errorMessage(castor::rh::AbortResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_setErrorMessage(castor::rh::AbortResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_setId
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_setId(castor::rh::AbortResponse* instance,
                              u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_id
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_id(castor::rh::AbortResponse* instance,
                           u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_type
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_type(castor::rh::AbortResponse* instance,
                             int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_clone
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_clone(castor::rh::AbortResponse* instance,
                              castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_aborted
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_aborted(castor::rh::AbortResponse* instance, bool* var) {
    *var = instance->aborted();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_AbortResponse_setAborted
  //----------------------------------------------------------------------------
  int Crh_AbortResponse_setAborted(castor::rh::AbortResponse* instance, bool new_var) {
    instance->setAborted(new_var);
    return 0;
  }

} // End of extern "C"
