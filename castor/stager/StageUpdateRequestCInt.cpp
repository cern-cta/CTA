/******************************************************************************
 *                      castor/stager/StageUpdateRequestCInt.cpp
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
#include "castor/stager/StageUpdateRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_create(castor::stager::StageUpdateRequest** obj) {
    *obj = new castor::stager::StageUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_delete(castor::stager::StageUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageUpdateRequest_getFileRequest(castor::stager::StageUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateRequest* Cstager_StageUpdateRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateRequest_getRequest(castor::stager::StageUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateRequest* Cstager_StageUpdateRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageUpdateRequest_getIObject(castor::stager::StageUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateRequest* Cstager_StageUpdateRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_print(castor::stager::StageUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_addSubRequests(castor::stager::StageUpdateRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_removeSubRequests(castor::stager::StageUpdateRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_subRequests(castor::stager::StageUpdateRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_flags(castor::stager::StageUpdateRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setFlags(castor::stager::StageUpdateRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_userName(castor::stager::StageUpdateRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setUserName(castor::stager::StageUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_euid(castor::stager::StageUpdateRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setEuid(castor::stager::StageUpdateRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_egid(castor::stager::StageUpdateRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setEgid(castor::stager::StageUpdateRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_mask(castor::stager::StageUpdateRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setMask(castor::stager::StageUpdateRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_pid(castor::stager::StageUpdateRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setPid(castor::stager::StageUpdateRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_machine(castor::stager::StageUpdateRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setMachine(castor::stager::StageUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_svcClassName(castor::stager::StageUpdateRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setSvcClassName(castor::stager::StageUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_userTag(castor::stager::StageUpdateRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setUserTag(castor::stager::StageUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_reqId(castor::stager::StageUpdateRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setReqId(castor::stager::StageUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_svcClass(castor::stager::StageUpdateRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setSvcClass(castor::stager::StageUpdateRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_client(castor::stager::StageUpdateRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setClient(castor::stager::StageUpdateRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setId(castor::stager::StageUpdateRequest* instance,
                                       u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_id(castor::stager::StageUpdateRequest* instance,
                                    u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_type(castor::stager::StageUpdateRequest* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
