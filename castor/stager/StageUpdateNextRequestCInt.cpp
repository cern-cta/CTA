/******************************************************************************
 *                      castor/stager/StageUpdateNextRequestCInt.cpp
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
#include "castor/stager/StageUpdateNextRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_create(castor::stager::StageUpdateNextRequest** obj) {
    *obj = new castor::stager::StageUpdateNextRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_delete(castor::stager::StageUpdateNextRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_getReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_StageUpdateNextRequest_getReqIdRequest(castor::stager::StageUpdateNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_fromReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateNextRequest* Cstager_StageUpdateNextRequest_fromReqIdRequest(castor::stager::ReqIdRequest* obj) {
    return dynamic_cast<castor::stager::StageUpdateNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateNextRequest_getRequest(castor::stager::StageUpdateNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateNextRequest* Cstager_StageUpdateNextRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageUpdateNextRequest_getIObject(castor::stager::StageUpdateNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateNextRequest* Cstager_StageUpdateNextRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageUpdateNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_print(castor::stager::StageUpdateNextRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateNextRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_parent(castor::stager::StageUpdateNextRequest* instance, castor::stager::FileRequest** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setParent(castor::stager::StageUpdateNextRequest* instance, castor::stager::FileRequest* new_var) {
    instance->setParent(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_flags(castor::stager::StageUpdateNextRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setFlags(castor::stager::StageUpdateNextRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_userName(castor::stager::StageUpdateNextRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setUserName(castor::stager::StageUpdateNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_euid(castor::stager::StageUpdateNextRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setEuid(castor::stager::StageUpdateNextRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_egid(castor::stager::StageUpdateNextRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setEgid(castor::stager::StageUpdateNextRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_mask(castor::stager::StageUpdateNextRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setMask(castor::stager::StageUpdateNextRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_pid(castor::stager::StageUpdateNextRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setPid(castor::stager::StageUpdateNextRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_machine(castor::stager::StageUpdateNextRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setMachine(castor::stager::StageUpdateNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_svcClassName(castor::stager::StageUpdateNextRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setSvcClassName(castor::stager::StageUpdateNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_userTag(castor::stager::StageUpdateNextRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setUserTag(castor::stager::StageUpdateNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_reqId(castor::stager::StageUpdateNextRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setReqId(castor::stager::StageUpdateNextRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_svcClass(castor::stager::StageUpdateNextRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setSvcClass(castor::stager::StageUpdateNextRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_client(castor::stager::StageUpdateNextRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setClient(castor::stager::StageUpdateNextRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setId(castor::stager::StageUpdateNextRequest* instance,
                                           u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_id(castor::stager::StageUpdateNextRequest* instance,
                                        u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_type(castor::stager::StageUpdateNextRequest* instance,
                                          int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
