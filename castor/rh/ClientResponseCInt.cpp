/******************************************************************************
 *                      castor/rh/ClientResponseCInt.cpp
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
 * @(#)$RCSfile: ClientResponseCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/12/02 17:56:04 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/rh/ClientResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_create
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_create(castor::rh::ClientResponse** obj) {
    *obj = new castor::rh::ClientResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_ClientResponse_delete
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_delete(castor::rh::ClientResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_ClientResponse_getResponse(castor::rh::ClientResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::ClientResponse* Crh_ClientResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::ClientResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_ClientResponse_getIObject(castor::rh::ClientResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::ClientResponse* Crh_ClientResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::ClientResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_print
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_print(castor::rh::ClientResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_TYPE(int* ret) {
    *ret = castor::rh::ClientResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_errorCode(castor::rh::ClientResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_setErrorCode(castor::rh::ClientResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_errorMessage(castor::rh::ClientResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_setErrorMessage(castor::rh::ClientResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_type
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_type(castor::rh::ClientResponse* instance,
                              int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_clone
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_clone(castor::rh::ClientResponse* instance,
                               castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_id
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_id(castor::rh::ClientResponse* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_setId
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_setId(castor::rh::ClientResponse* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_client
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_client(castor::rh::ClientResponse* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ClientResponse_setClient
  //----------------------------------------------------------------------------
  int Crh_ClientResponse_setClient(castor::rh::ClientResponse* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

} // End of extern "C"
