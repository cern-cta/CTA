/******************************************************************************
 *                      castor/stager/StageRequestQueryRequestCInt.cpp
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
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_create(castor::stager::StageRequestQueryRequest** obj) {
    *obj = new castor::stager::StageRequestQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_delete(castor::stager::StageRequestQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_getQryRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_StageRequestQueryRequest_getQryRequest(castor::stager::StageRequestQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_fromQryRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRequestQueryRequest* Cstager_StageRequestQueryRequest_fromQryRequest(castor::stager::QryRequest* obj) {
    return dynamic_cast<castor::stager::StageRequestQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageRequestQueryRequest_getRequest(castor::stager::StageRequestQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRequestQueryRequest* Cstager_StageRequestQueryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageRequestQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageRequestQueryRequest_getIObject(castor::stager::StageRequestQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageRequestQueryRequest* Cstager_StageRequestQueryRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageRequestQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_print(castor::stager::StageRequestQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageRequestQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_flags(castor::stager::StageRequestQueryRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setFlags(castor::stager::StageRequestQueryRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_userName(castor::stager::StageRequestQueryRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setUserName(castor::stager::StageRequestQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_euid(castor::stager::StageRequestQueryRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setEuid(castor::stager::StageRequestQueryRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_egid(castor::stager::StageRequestQueryRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setEgid(castor::stager::StageRequestQueryRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_mask(castor::stager::StageRequestQueryRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setMask(castor::stager::StageRequestQueryRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_pid(castor::stager::StageRequestQueryRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setPid(castor::stager::StageRequestQueryRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_machine(castor::stager::StageRequestQueryRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setMachine(castor::stager::StageRequestQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_svcClassName(castor::stager::StageRequestQueryRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setSvcClassName(castor::stager::StageRequestQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_userTag(castor::stager::StageRequestQueryRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setUserTag(castor::stager::StageRequestQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_reqId(castor::stager::StageRequestQueryRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setReqId(castor::stager::StageRequestQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_svcClass(castor::stager::StageRequestQueryRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setSvcClass(castor::stager::StageRequestQueryRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_client(castor::stager::StageRequestQueryRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setClient(castor::stager::StageRequestQueryRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setId(castor::stager::StageRequestQueryRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_id(castor::stager::StageRequestQueryRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_type(castor::stager::StageRequestQueryRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
