/******************************************************************************
 *                      castor/stager/StagePutRequestCInt.cpp
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
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_create(castor::stager::StagePutRequest** obj) {
    *obj = new castor::stager::StagePutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_delete(castor::stager::StagePutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePutRequest_getFileRequest(castor::stager::StagePutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutRequest* Cstager_StagePutRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePutRequest_getRequest(castor::stager::StagePutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutRequest* Cstager_StagePutRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StagePutRequest_getIObject(castor::stager::StagePutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StagePutRequest* Cstager_StagePutRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StagePutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_print(castor::stager::StagePutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_addSubRequests(castor::stager::StagePutRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_removeSubRequests(castor::stager::StagePutRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_subRequests(castor::stager::StagePutRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_flags(castor::stager::StagePutRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setFlags(castor::stager::StagePutRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_userName(castor::stager::StagePutRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setUserName(castor::stager::StagePutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_euid(castor::stager::StagePutRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setEuid(castor::stager::StagePutRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_egid(castor::stager::StagePutRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setEgid(castor::stager::StagePutRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_mask(castor::stager::StagePutRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setMask(castor::stager::StagePutRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_pid(castor::stager::StagePutRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setPid(castor::stager::StagePutRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_machine(castor::stager::StagePutRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setMachine(castor::stager::StagePutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_svcClassName(castor::stager::StagePutRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setSvcClassName(castor::stager::StagePutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_userTag(castor::stager::StagePutRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setUserTag(castor::stager::StagePutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_reqId(castor::stager::StagePutRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setReqId(castor::stager::StagePutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_svcClass(castor::stager::StagePutRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setSvcClass(castor::stager::StagePutRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_client(castor::stager::StagePutRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setClient(castor::stager::StagePutRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setId(castor::stager::StagePutRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_id(castor::stager::StagePutRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_type(castor::stager::StagePutRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
