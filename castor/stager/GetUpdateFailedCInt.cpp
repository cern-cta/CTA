/******************************************************************************
 *                      castor/stager/GetUpdateFailedCInt.cpp
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
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_create
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_create(castor::stager::GetUpdateFailed** obj) {
    *obj = new castor::stager::GetUpdateFailed();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_delete
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_delete(castor::stager::GetUpdateFailed* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_GetUpdateFailed_getRequest(castor::stager::GetUpdateFailed* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::GetUpdateFailed* Cstager_GetUpdateFailed_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::GetUpdateFailed*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_GetUpdateFailed_getIObject(castor::stager::GetUpdateFailed* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::GetUpdateFailed* Cstager_GetUpdateFailed_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::GetUpdateFailed*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_print
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_print(castor::stager::GetUpdateFailed* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_TYPE
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_TYPE(int* ret) {
    *ret = castor::stager::GetUpdateFailed::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_flags
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_flags(castor::stager::GetUpdateFailed* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setFlags
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setFlags(castor::stager::GetUpdateFailed* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_userName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_userName(castor::stager::GetUpdateFailed* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setUserName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setUserName(castor::stager::GetUpdateFailed* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_euid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_euid(castor::stager::GetUpdateFailed* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setEuid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setEuid(castor::stager::GetUpdateFailed* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_egid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_egid(castor::stager::GetUpdateFailed* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setEgid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setEgid(castor::stager::GetUpdateFailed* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_mask
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_mask(castor::stager::GetUpdateFailed* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setMask
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setMask(castor::stager::GetUpdateFailed* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_pid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_pid(castor::stager::GetUpdateFailed* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setPid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setPid(castor::stager::GetUpdateFailed* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_machine
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_machine(castor::stager::GetUpdateFailed* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setMachine
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setMachine(castor::stager::GetUpdateFailed* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_svcClassName(castor::stager::GetUpdateFailed* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setSvcClassName(castor::stager::GetUpdateFailed* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_userTag
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_userTag(castor::stager::GetUpdateFailed* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setUserTag(castor::stager::GetUpdateFailed* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_reqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_reqId(castor::stager::GetUpdateFailed* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setReqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setReqId(castor::stager::GetUpdateFailed* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_creationTime
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_creationTime(castor::stager::GetUpdateFailed* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setCreationTime
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setCreationTime(castor::stager::GetUpdateFailed* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_lastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_lastModificationTime(castor::stager::GetUpdateFailed* instance, u_signed64* var) {
    *var = instance->lastModificationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setLastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setLastModificationTime(castor::stager::GetUpdateFailed* instance, u_signed64 new_var) {
    instance->setLastModificationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_svcClass
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_svcClass(castor::stager::GetUpdateFailed* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setSvcClass(castor::stager::GetUpdateFailed* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_client
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_client(castor::stager::GetUpdateFailed* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setClient
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setClient(castor::stager::GetUpdateFailed* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_type
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_type(castor::stager::GetUpdateFailed* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_clone
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_clone(castor::stager::GetUpdateFailed* instance,
                                    castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_subReqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_subReqId(castor::stager::GetUpdateFailed* instance, u_signed64* var) {
    *var = instance->subReqId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setSubReqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setSubReqId(castor::stager::GetUpdateFailed* instance, u_signed64 new_var) {
    instance->setSubReqId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_id
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_id(castor::stager::GetUpdateFailed* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateFailed_setId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateFailed_setId(castor::stager::GetUpdateFailed* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
