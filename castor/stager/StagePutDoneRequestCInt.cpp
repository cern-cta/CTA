/******************************************************************************
 *                      castor/stager/StagePutDoneRequestCInt.cpp
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
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_create(castor::stager::StagePutDoneRequest** obj) {
    *obj = new castor::stager::StagePutDoneRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_delete(castor::stager::StagePutDoneRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePutDoneRequest_getFileRequest(castor::stager::StagePutDoneRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutDoneRequest* Cstager_StagePutDoneRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePutDoneRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePutDoneRequest_getRequest(castor::stager::StagePutDoneRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutDoneRequest* Cstager_StagePutDoneRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePutDoneRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StagePutDoneRequest_getIObject(castor::stager::StagePutDoneRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StagePutDoneRequest* Cstager_StagePutDoneRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StagePutDoneRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_print(castor::stager::StagePutDoneRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutDoneRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_addSubRequests(castor::stager::StagePutDoneRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_removeSubRequests(castor::stager::StagePutDoneRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_subRequests(castor::stager::StagePutDoneRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_flags(castor::stager::StagePutDoneRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setFlags(castor::stager::StagePutDoneRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_userName(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setUserName(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_euid(castor::stager::StagePutDoneRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setEuid(castor::stager::StagePutDoneRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_egid(castor::stager::StagePutDoneRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setEgid(castor::stager::StagePutDoneRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_mask(castor::stager::StagePutDoneRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setMask(castor::stager::StagePutDoneRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_pid(castor::stager::StagePutDoneRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setPid(castor::stager::StagePutDoneRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_machine(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setMachine(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_svcClassName(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setSvcClassName(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_userTag(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setUserTag(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_reqId(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setReqId(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_svcClass(castor::stager::StagePutDoneRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setSvcClass(castor::stager::StagePutDoneRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_client(castor::stager::StagePutDoneRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setClient(castor::stager::StagePutDoneRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_type(castor::stager::StagePutDoneRequest* instance,
                                       int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_clone(castor::stager::StagePutDoneRequest* instance,
                                        castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_parentUuid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_parentUuid(castor::stager::StagePutDoneRequest* instance, const char** var) {
    *var = instance->parentUuid().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setParentUuid
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setParentUuid(castor::stager::StagePutDoneRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setParentUuid(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_id(castor::stager::StagePutDoneRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setId(castor::stager::StagePutDoneRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_parent(castor::stager::StagePutDoneRequest* instance, castor::stager::FileRequest** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setParent(castor::stager::StagePutDoneRequest* instance, castor::stager::FileRequest* new_var) {
    instance->setParent(new_var);
    return 0;
  }

} // End of extern "C"
