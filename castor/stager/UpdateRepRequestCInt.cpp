/******************************************************************************
 *                      castor/stager/UpdateRepRequestCInt.cpp
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
 * @(#)$RCSfile: UpdateRepRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/30 11:28:01 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/UpdateRepRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_create
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_create(castor::stager::UpdateRepRequest** obj) {
    *obj = new castor::stager::UpdateRepRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_delete(castor::stager::UpdateRepRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_UpdateRepRequest_getRequest(castor::stager::UpdateRepRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::UpdateRepRequest* Cstager_UpdateRepRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::UpdateRepRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_UpdateRepRequest_getIObject(castor::stager::UpdateRepRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::UpdateRepRequest* Cstager_UpdateRepRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::UpdateRepRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_print
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_print(castor::stager::UpdateRepRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_TYPE(int* ret) {
    *ret = castor::stager::UpdateRepRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_flags(castor::stager::UpdateRepRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setFlags(castor::stager::UpdateRepRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_userName(castor::stager::UpdateRepRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setUserName(castor::stager::UpdateRepRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_euid(castor::stager::UpdateRepRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setEuid(castor::stager::UpdateRepRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_egid(castor::stager::UpdateRepRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setEgid(castor::stager::UpdateRepRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_mask(castor::stager::UpdateRepRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setMask(castor::stager::UpdateRepRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_pid(castor::stager::UpdateRepRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setPid(castor::stager::UpdateRepRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_machine(castor::stager::UpdateRepRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setMachine(castor::stager::UpdateRepRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_svcClassName(castor::stager::UpdateRepRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setSvcClassName(castor::stager::UpdateRepRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_userTag(castor::stager::UpdateRepRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setUserTag(castor::stager::UpdateRepRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_reqId(castor::stager::UpdateRepRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setReqId(castor::stager::UpdateRepRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_svcClass(castor::stager::UpdateRepRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setSvcClass(castor::stager::UpdateRepRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_client
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_client(castor::stager::UpdateRepRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setClient(castor::stager::UpdateRepRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_type
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_type(castor::stager::UpdateRepRequest* instance,
                                    int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_clone(castor::stager::UpdateRepRequest* instance,
                                     castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_id
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_id(castor::stager::UpdateRepRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setId(castor::stager::UpdateRepRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_object
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_object(castor::stager::UpdateRepRequest* instance, castor::IObject** var) {
    *var = instance->object();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setObject
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setObject(castor::stager::UpdateRepRequest* instance, castor::IObject* new_var) {
    instance->setObject(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_address
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_address(castor::stager::UpdateRepRequest* instance, castor::IAddress** var) {
    *var = instance->address();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_UpdateRepRequest_setAddress
  //----------------------------------------------------------------------------
  int Cstager_UpdateRepRequest_setAddress(castor::stager::UpdateRepRequest* instance, castor::IAddress* new_var) {
    instance->setAddress(new_var);
    return 0;
  }

} // End of extern "C"
