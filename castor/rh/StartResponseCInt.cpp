/******************************************************************************
 *                      castor/rh/StartResponseCInt.cpp
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
#include "castor/rh/StartResponse.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_StartResponse_create
  //----------------------------------------------------------------------------
  int Crh_StartResponse_create(castor::rh::StartResponse** obj) {
    *obj = new castor::rh::StartResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_StartResponse_delete
  //----------------------------------------------------------------------------
  int Crh_StartResponse_delete(castor::rh::StartResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_StartResponse_getResponse(castor::rh::StartResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::StartResponse* Crh_StartResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::StartResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_StartResponse_getIObject(castor::rh::StartResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::StartResponse* Crh_StartResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::StartResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_print
  //----------------------------------------------------------------------------
  int Crh_StartResponse_print(castor::rh::StartResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_StartResponse_TYPE(int* ret) {
    *ret = castor::rh::StartResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_StartResponse_errorCode(castor::rh::StartResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_StartResponse_setErrorCode(castor::rh::StartResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_StartResponse_errorMessage(castor::rh::StartResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_StartResponse_setErrorMessage(castor::rh::StartResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_type
  //----------------------------------------------------------------------------
  int Crh_StartResponse_type(castor::rh::StartResponse* instance,
                             int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_clone
  //----------------------------------------------------------------------------
  int Crh_StartResponse_clone(castor::rh::StartResponse* instance,
                              castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_id
  //----------------------------------------------------------------------------
  int Crh_StartResponse_id(castor::rh::StartResponse* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_setId
  //----------------------------------------------------------------------------
  int Crh_StartResponse_setId(castor::rh::StartResponse* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_diskCopy
  //----------------------------------------------------------------------------
  int Crh_StartResponse_diskCopy(castor::rh::StartResponse* instance, castor::stager::DiskCopy** var) {
    *var = instance->diskCopy();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_StartResponse_setDiskCopy
  //----------------------------------------------------------------------------
  int Crh_StartResponse_setDiskCopy(castor::rh::StartResponse* instance, castor::stager::DiskCopy* new_var) {
    instance->setDiskCopy(new_var);
    return 0;
  }

} // End of extern "C"
