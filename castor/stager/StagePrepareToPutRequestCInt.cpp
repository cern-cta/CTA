/******************************************************************************
 *                      castor/stager/StagePrepareToPutRequestCInt.cpp
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
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_create(castor::stager::StagePrepareToPutRequest** obj) {
    *obj = new castor::stager::StagePrepareToPutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_delete(castor::stager::StagePrepareToPutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToPutRequest_getFileRequest(castor::stager::StagePrepareToPutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToPutRequest* Cstager_StagePrepareToPutRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToPutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToPutRequest_getRequest(castor::stager::StagePrepareToPutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToPutRequest* Cstager_StagePrepareToPutRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToPutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StagePrepareToPutRequest_getIObject(castor::stager::StagePrepareToPutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToPutRequest* Cstager_StagePrepareToPutRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StagePrepareToPutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_print(castor::stager::StagePrepareToPutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToPutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_addSubRequests(castor::stager::StagePrepareToPutRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_removeSubRequests(castor::stager::StagePrepareToPutRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_subRequests(castor::stager::StagePrepareToPutRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_flags(castor::stager::StagePrepareToPutRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setFlags(castor::stager::StagePrepareToPutRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_userName(castor::stager::StagePrepareToPutRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setUserName(castor::stager::StagePrepareToPutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_euid(castor::stager::StagePrepareToPutRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setEuid(castor::stager::StagePrepareToPutRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_egid(castor::stager::StagePrepareToPutRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setEgid(castor::stager::StagePrepareToPutRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_mask(castor::stager::StagePrepareToPutRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setMask(castor::stager::StagePrepareToPutRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_pid(castor::stager::StagePrepareToPutRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setPid(castor::stager::StagePrepareToPutRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_machine(castor::stager::StagePrepareToPutRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setMachine(castor::stager::StagePrepareToPutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_svcClassName(castor::stager::StagePrepareToPutRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setSvcClassName(castor::stager::StagePrepareToPutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_userTag(castor::stager::StagePrepareToPutRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setUserTag(castor::stager::StagePrepareToPutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_reqId(castor::stager::StagePrepareToPutRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setReqId(castor::stager::StagePrepareToPutRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_svcClass(castor::stager::StagePrepareToPutRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setSvcClass(castor::stager::StagePrepareToPutRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_client(castor::stager::StagePrepareToPutRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setClient(castor::stager::StagePrepareToPutRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_type(castor::stager::StagePrepareToPutRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_clone(castor::stager::StagePrepareToPutRequest* instance,
                                             castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_id(castor::stager::StagePrepareToPutRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setId(castor::stager::StagePrepareToPutRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
