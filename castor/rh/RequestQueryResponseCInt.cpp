/******************************************************************************
 *                      castor/rh/RequestQueryResponseCInt.cpp
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
#include "castor/rh/RequestQueryResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_create
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_create(castor::rh::RequestQueryResponse** obj) {
    *obj = new castor::rh::RequestQueryResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_delete
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_delete(castor::rh::RequestQueryResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_RequestQueryResponse_getResponse(castor::rh::RequestQueryResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::RequestQueryResponse* Crh_RequestQueryResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::RequestQueryResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_RequestQueryResponse_getIObject(castor::rh::RequestQueryResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::RequestQueryResponse* Crh_RequestQueryResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::RequestQueryResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_print
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_print(castor::rh::RequestQueryResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_TYPE(int* ret) {
    *ret = castor::rh::RequestQueryResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_errorCode(castor::rh::RequestQueryResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setErrorCode(castor::rh::RequestQueryResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_errorMessage(castor::rh::RequestQueryResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setErrorMessage(castor::rh::RequestQueryResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_type
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_type(castor::rh::RequestQueryResponse* instance,
                                    int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_clone
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_clone(castor::rh::RequestQueryResponse* instance,
                                     castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_reqId
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_reqId(castor::rh::RequestQueryResponse* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setReqId
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setReqId(castor::rh::RequestQueryResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_status
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_status(castor::rh::RequestQueryResponse* instance, unsigned int* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setStatus
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setStatus(castor::rh::RequestQueryResponse* instance, unsigned int new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_creationTime
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_creationTime(castor::rh::RequestQueryResponse* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setCreationTime
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setCreationTime(castor::rh::RequestQueryResponse* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_modificationTime
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_modificationTime(castor::rh::RequestQueryResponse* instance, u_signed64* var) {
    *var = instance->modificationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setModificationTime
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setModificationTime(castor::rh::RequestQueryResponse* instance, u_signed64 new_var) {
    instance->setModificationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_id
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_id(castor::rh::RequestQueryResponse* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_RequestQueryResponse_setId
  //----------------------------------------------------------------------------
  int Crh_RequestQueryResponse_setId(castor::rh::RequestQueryResponse* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
