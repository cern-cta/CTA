/******************************************************************************
 *                      castor/stager/StageGetRequestCInt.cpp
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
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_create(castor::stager::StageGetRequest** obj) {
    *obj = new castor::stager::StageGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_delete(castor::stager::StageGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageGetRequest_getFileRequest(castor::stager::StageGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetRequest* Cstager_StageGetRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_print(castor::stager::StageGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StageGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_addSubRequests(castor::stager::StageGetRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_removeSubRequests(castor::stager::StageGetRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_subRequests(castor::stager::StageGetRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_flags(castor::stager::StageGetRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setFlags(castor::stager::StageGetRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_userName(castor::stager::StageGetRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setUserName(castor::stager::StageGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_euid(castor::stager::StageGetRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setEuid(castor::stager::StageGetRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_egid(castor::stager::StageGetRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setEgid(castor::stager::StageGetRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_mask(castor::stager::StageGetRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setMask(castor::stager::StageGetRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_pid(castor::stager::StageGetRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setPid(castor::stager::StageGetRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_machine(castor::stager::StageGetRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setMachine(castor::stager::StageGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_svcClassName(castor::stager::StageGetRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setSvcClassName(castor::stager::StageGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_userTag(castor::stager::StageGetRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setUserTag(castor::stager::StageGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_reqId(castor::stager::StageGetRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setReqId(castor::stager::StageGetRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_svcClass(castor::stager::StageGetRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setSvcClass(castor::stager::StageGetRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_client(castor::stager::StageGetRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setClient(castor::stager::StageGetRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setId(castor::stager::StageGetRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_id(castor::stager::StageGetRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_type(castor::stager::StageGetRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
