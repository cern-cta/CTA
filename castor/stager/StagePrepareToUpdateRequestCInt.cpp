/******************************************************************************
 *                      castor/stager/StagePrepareToUpdateRequestCInt.cpp
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
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_create(castor::stager::StagePrepareToUpdateRequest** obj) {
    *obj = new castor::stager::StagePrepareToUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_delete(castor::stager::StagePrepareToUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToUpdateRequest_getFileRequest(castor::stager::StagePrepareToUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToUpdateRequest* Cstager_StagePrepareToUpdateRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToUpdateRequest_getRequest(castor::stager::StagePrepareToUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToUpdateRequest* Cstager_StagePrepareToUpdateRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StagePrepareToUpdateRequest_getIObject(castor::stager::StagePrepareToUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToUpdateRequest* Cstager_StagePrepareToUpdateRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StagePrepareToUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_print(castor::stager::StagePrepareToUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_addSubRequests(castor::stager::StagePrepareToUpdateRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_removeSubRequests(castor::stager::StagePrepareToUpdateRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_subRequests(castor::stager::StagePrepareToUpdateRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_flags(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setFlags(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_userName(castor::stager::StagePrepareToUpdateRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setUserName(castor::stager::StagePrepareToUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_euid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setEuid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_egid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setEgid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_mask(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setMask(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_pid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setPid(castor::stager::StagePrepareToUpdateRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_machine(castor::stager::StagePrepareToUpdateRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setMachine(castor::stager::StagePrepareToUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_svcClassName(castor::stager::StagePrepareToUpdateRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setSvcClassName(castor::stager::StagePrepareToUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_userTag(castor::stager::StagePrepareToUpdateRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setUserTag(castor::stager::StagePrepareToUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_reqId(castor::stager::StagePrepareToUpdateRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setReqId(castor::stager::StagePrepareToUpdateRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_creationTime
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_creationTime(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setCreationTime
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setCreationTime(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_lastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_lastModificationTime(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64* var) {
    *var = instance->lastModificationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setLastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setLastModificationTime(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64 new_var) {
    instance->setLastModificationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_svcClass(castor::stager::StagePrepareToUpdateRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setSvcClass(castor::stager::StagePrepareToUpdateRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_client(castor::stager::StagePrepareToUpdateRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setClient(castor::stager::StagePrepareToUpdateRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_type(castor::stager::StagePrepareToUpdateRequest* instance,
                                               int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_clone(castor::stager::StagePrepareToUpdateRequest* instance,
                                                castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_id(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setId(castor::stager::StagePrepareToUpdateRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
