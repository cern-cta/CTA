/******************************************************************************
 *                      castor/stager/StageGetNextRequestCInt.cpp
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
#include "castor/stager/ReqIdRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StageGetNextRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_create(castor::stager::StageGetNextRequest** obj) {
    *obj = new castor::stager::StageGetNextRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_delete(castor::stager::StageGetNextRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_getReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_StageGetNextRequest_getReqIdRequest(castor::stager::StageGetNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_fromReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetNextRequest* Cstager_StageGetNextRequest_fromReqIdRequest(castor::stager::ReqIdRequest* obj) {
    return dynamic_cast<castor::stager::StageGetNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageGetNextRequest_getRequest(castor::stager::StageGetNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetNextRequest* Cstager_StageGetNextRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageGetNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageGetNextRequest_getIObject(castor::stager::StageGetNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageGetNextRequest* Cstager_StageGetNextRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageGetNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_print(castor::stager::StageGetNextRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_TYPE(int* ret) {
    *ret = castor::stager::StageGetNextRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_parentUuid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_parentUuid(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->parentUuid().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setParentUuid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setParentUuid(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setParentUuid(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_parent(castor::stager::StageGetNextRequest* instance, castor::stager::FileRequest** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setParent(castor::stager::StageGetNextRequest* instance, castor::stager::FileRequest* new_var) {
    instance->setParent(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_flags(castor::stager::StageGetNextRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setFlags(castor::stager::StageGetNextRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_userName(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setUserName(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_euid(castor::stager::StageGetNextRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setEuid(castor::stager::StageGetNextRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_egid(castor::stager::StageGetNextRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setEgid(castor::stager::StageGetNextRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_mask(castor::stager::StageGetNextRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setMask(castor::stager::StageGetNextRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_pid(castor::stager::StageGetNextRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setPid(castor::stager::StageGetNextRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_machine(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setMachine(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_svcClassName(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setSvcClassName(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_userTag(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setUserTag(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_reqId(castor::stager::StageGetNextRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setReqId(castor::stager::StageGetNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_svcClass(castor::stager::StageGetNextRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setSvcClass(castor::stager::StageGetNextRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_client(castor::stager::StageGetNextRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setClient(castor::stager::StageGetNextRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_type(castor::stager::StageGetNextRequest* instance,
                                       int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_clone(castor::stager::StageGetNextRequest* instance,
                                        castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_id(castor::stager::StageGetNextRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setId(castor::stager::StageGetNextRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
