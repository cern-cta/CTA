/******************************************************************************
 *                      castor/stager/ReqIdRequestCInt.cpp
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
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_delete(castor::stager::ReqIdRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_ReqIdRequest_getRequest(castor::stager::ReqIdRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_ReqIdRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::ReqIdRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_ReqIdRequest_getIObject(castor::stager::ReqIdRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_ReqIdRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::ReqIdRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_print
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_print(castor::stager::ReqIdRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_flags(castor::stager::ReqIdRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setFlags(castor::stager::ReqIdRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_userName(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setUserName(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_euid(castor::stager::ReqIdRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setEuid(castor::stager::ReqIdRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_egid(castor::stager::ReqIdRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setEgid(castor::stager::ReqIdRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_mask(castor::stager::ReqIdRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setMask(castor::stager::ReqIdRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_pid(castor::stager::ReqIdRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setPid(castor::stager::ReqIdRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_machine(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setMachine(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_svcClassName(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setSvcClassName(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_userTag(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setUserTag(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_reqId(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setReqId(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_svcClass(castor::stager::ReqIdRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setSvcClass(castor::stager::ReqIdRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_client
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_client(castor::stager::ReqIdRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setClient(castor::stager::ReqIdRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_parentUuid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_parentUuid(castor::stager::ReqIdRequest* instance, const char** var) {
    *var = instance->parentUuid().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setParentUuid
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setParentUuid(castor::stager::ReqIdRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setParentUuid(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_parent(castor::stager::ReqIdRequest* instance, castor::stager::FileRequest** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_setParent(castor::stager::ReqIdRequest* instance, castor::stager::FileRequest* new_var) {
    instance->setParent(new_var);
    return 0;
  }

} // End of extern "C"
