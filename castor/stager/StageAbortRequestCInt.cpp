/******************************************************************************
 *                      castor/stager/StageAbortRequestCInt.cpp
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
#include "castor/stager/StageAbortRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_create(castor::stager::StageAbortRequest** obj) {
    *obj = new castor::stager::StageAbortRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_delete(castor::stager::StageAbortRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageAbortRequest_getFileRequest(castor::stager::StageAbortRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageAbortRequest* Cstager_StageAbortRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageAbortRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageAbortRequest_getRequest(castor::stager::StageAbortRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageAbortRequest* Cstager_StageAbortRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageAbortRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageAbortRequest_getIObject(castor::stager::StageAbortRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageAbortRequest* Cstager_StageAbortRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageAbortRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_print(castor::stager::StageAbortRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_TYPE(int* ret) {
    *ret = castor::stager::StageAbortRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_addSubRequests(castor::stager::StageAbortRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_removeSubRequests(castor::stager::StageAbortRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_subRequests(castor::stager::StageAbortRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_flags(castor::stager::StageAbortRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setFlags(castor::stager::StageAbortRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_userName(castor::stager::StageAbortRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setUserName(castor::stager::StageAbortRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_euid(castor::stager::StageAbortRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setEuid(castor::stager::StageAbortRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_egid(castor::stager::StageAbortRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setEgid(castor::stager::StageAbortRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_mask(castor::stager::StageAbortRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setMask(castor::stager::StageAbortRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_pid(castor::stager::StageAbortRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setPid(castor::stager::StageAbortRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_machine(castor::stager::StageAbortRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setMachine(castor::stager::StageAbortRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_svcClassName(castor::stager::StageAbortRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setSvcClassName(castor::stager::StageAbortRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_userTag(castor::stager::StageAbortRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setUserTag(castor::stager::StageAbortRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_reqId(castor::stager::StageAbortRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setReqId(castor::stager::StageAbortRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_svcClass(castor::stager::StageAbortRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setSvcClass(castor::stager::StageAbortRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_client(castor::stager::StageAbortRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setClient(castor::stager::StageAbortRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setId(castor::stager::StageAbortRequest* instance,
                                      u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_id(castor::stager::StageAbortRequest* instance,
                                   u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_type(castor::stager::StageAbortRequest* instance,
                                     int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
