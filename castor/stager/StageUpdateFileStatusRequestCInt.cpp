/******************************************************************************
 *                      castor/stager/StageUpdateFileStatusRequestCInt.cpp
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
 * @(#)$RCSfile: StageUpdateFileStatusRequestCInt.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2005/02/01 17:45:23 $ $Author: sponcec3 $
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
#include "castor/stager/StageUpdateFileStatusRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_create(castor::stager::StageUpdateFileStatusRequest** obj) {
    *obj = new castor::stager::StageUpdateFileStatusRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_delete(castor::stager::StageUpdateFileStatusRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageUpdateFileStatusRequest_getFileRequest(castor::stager::StageUpdateFileStatusRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateFileStatusRequest* Cstager_StageUpdateFileStatusRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageUpdateFileStatusRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateFileStatusRequest_getRequest(castor::stager::StageUpdateFileStatusRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateFileStatusRequest* Cstager_StageUpdateFileStatusRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateFileStatusRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageUpdateFileStatusRequest_getIObject(castor::stager::StageUpdateFileStatusRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateFileStatusRequest* Cstager_StageUpdateFileStatusRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageUpdateFileStatusRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_print(castor::stager::StageUpdateFileStatusRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateFileStatusRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_addSubRequests(castor::stager::StageUpdateFileStatusRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_removeSubRequests(castor::stager::StageUpdateFileStatusRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_subRequests(castor::stager::StageUpdateFileStatusRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*>& result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_flags(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setFlags(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_userName(castor::stager::StageUpdateFileStatusRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setUserName(castor::stager::StageUpdateFileStatusRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_euid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setEuid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_egid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setEgid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_mask(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setMask(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_pid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setPid(castor::stager::StageUpdateFileStatusRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_machine(castor::stager::StageUpdateFileStatusRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setMachine(castor::stager::StageUpdateFileStatusRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_svcClassName(castor::stager::StageUpdateFileStatusRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setSvcClassName(castor::stager::StageUpdateFileStatusRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_userTag(castor::stager::StageUpdateFileStatusRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setUserTag(castor::stager::StageUpdateFileStatusRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_reqId(castor::stager::StageUpdateFileStatusRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setReqId(castor::stager::StageUpdateFileStatusRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_creationTime
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_creationTime(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setCreationTime
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setCreationTime(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_lastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_lastModificationTime(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64* var) {
    *var = instance->lastModificationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setLastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setLastModificationTime(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64 new_var) {
    instance->setLastModificationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_svcClass(castor::stager::StageUpdateFileStatusRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setSvcClass(castor::stager::StageUpdateFileStatusRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_client(castor::stager::StageUpdateFileStatusRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setClient(castor::stager::StageUpdateFileStatusRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_type(castor::stager::StageUpdateFileStatusRequest* instance,
                                                int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_clone(castor::stager::StageUpdateFileStatusRequest* instance,
                                                 castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_id(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setId(castor::stager::StageUpdateFileStatusRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
