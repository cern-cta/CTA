/******************************************************************************
 *                      castor/stager/StageFindRequestRequestCInt.cpp
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
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StageFindRequestRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_create(castor::stager::StageFindRequestRequest** obj) {
    *obj = new castor::stager::StageFindRequestRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_delete(castor::stager::StageFindRequestRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_getQryRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_StageFindRequestRequest_getQryRequest(castor::stager::StageFindRequestRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_fromQryRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFindRequestRequest* Cstager_StageFindRequestRequest_fromQryRequest(castor::stager::QryRequest* obj) {
    return dynamic_cast<castor::stager::StageFindRequestRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageFindRequestRequest_getRequest(castor::stager::StageFindRequestRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFindRequestRequest* Cstager_StageFindRequestRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageFindRequestRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageFindRequestRequest_getIObject(castor::stager::StageFindRequestRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageFindRequestRequest* Cstager_StageFindRequestRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageFindRequestRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_print(castor::stager::StageFindRequestRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFindRequestRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_flags(castor::stager::StageFindRequestRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setFlags(castor::stager::StageFindRequestRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_userName(castor::stager::StageFindRequestRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setUserName(castor::stager::StageFindRequestRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_euid(castor::stager::StageFindRequestRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setEuid(castor::stager::StageFindRequestRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_egid(castor::stager::StageFindRequestRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setEgid(castor::stager::StageFindRequestRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_mask(castor::stager::StageFindRequestRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setMask(castor::stager::StageFindRequestRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_pid(castor::stager::StageFindRequestRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setPid(castor::stager::StageFindRequestRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_machine(castor::stager::StageFindRequestRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setMachine(castor::stager::StageFindRequestRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_svcClassName(castor::stager::StageFindRequestRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setSvcClassName(castor::stager::StageFindRequestRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_userTag(castor::stager::StageFindRequestRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setUserTag(castor::stager::StageFindRequestRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_reqId(castor::stager::StageFindRequestRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setReqId(castor::stager::StageFindRequestRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_svcClass(castor::stager::StageFindRequestRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setSvcClass(castor::stager::StageFindRequestRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_client(castor::stager::StageFindRequestRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setClient(castor::stager::StageFindRequestRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setId(castor::stager::StageFindRequestRequest* instance,
                                            u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_id(castor::stager::StageFindRequestRequest* instance,
                                         u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_type(castor::stager::StageFindRequestRequest* instance,
                                           int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_clone(castor::stager::StageFindRequestRequest* instance,
                                            castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

} // End of extern "C"
