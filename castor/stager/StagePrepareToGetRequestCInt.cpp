/******************************************************************************
 *                      castor/stager/StagePrepareToGetRequestCInt.cpp
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
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_create(castor::stager::StagePrepareToGetRequest** obj) {
    *obj = new castor::stager::StagePrepareToGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_delete(castor::stager::StagePrepareToGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToGetRequest_getFileRequest(castor::stager::StagePrepareToGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToGetRequest* Cstager_StagePrepareToGetRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToGetRequest_getRequest(castor::stager::StagePrepareToGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToGetRequest* Cstager_StagePrepareToGetRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StagePrepareToGetRequest_getIObject(castor::stager::StagePrepareToGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToGetRequest* Cstager_StagePrepareToGetRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StagePrepareToGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_print(castor::stager::StagePrepareToGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_addSubRequests(castor::stager::StagePrepareToGetRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_removeSubRequests(castor::stager::StagePrepareToGetRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_subRequests(castor::stager::StagePrepareToGetRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_flags(castor::stager::StagePrepareToGetRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setFlags(castor::stager::StagePrepareToGetRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_userName(castor::stager::StagePrepareToGetRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setUserName(castor::stager::StagePrepareToGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_euid(castor::stager::StagePrepareToGetRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setEuid(castor::stager::StagePrepareToGetRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_egid(castor::stager::StagePrepareToGetRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setEgid(castor::stager::StagePrepareToGetRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_mask(castor::stager::StagePrepareToGetRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setMask(castor::stager::StagePrepareToGetRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_pid(castor::stager::StagePrepareToGetRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setPid(castor::stager::StagePrepareToGetRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_machine(castor::stager::StagePrepareToGetRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setMachine(castor::stager::StagePrepareToGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_svcClassName(castor::stager::StagePrepareToGetRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setSvcClassName(castor::stager::StagePrepareToGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_userTag(castor::stager::StagePrepareToGetRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setUserTag(castor::stager::StagePrepareToGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_reqId(castor::stager::StagePrepareToGetRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setReqId(castor::stager::StagePrepareToGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_svcClass(castor::stager::StagePrepareToGetRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setSvcClass(castor::stager::StagePrepareToGetRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_client(castor::stager::StagePrepareToGetRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setClient(castor::stager::StagePrepareToGetRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setId(castor::stager::StagePrepareToGetRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_id(castor::stager::StagePrepareToGetRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_type(castor::stager::StagePrepareToGetRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_clone(castor::stager::StagePrepareToGetRequest* instance,
                                             castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

} // End of extern "C"
