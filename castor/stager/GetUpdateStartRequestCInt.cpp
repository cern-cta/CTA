/******************************************************************************
 *                      castor/stager/GetUpdateStartRequestCInt.cpp
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
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StartRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_create
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_create(castor::stager::GetUpdateStartRequest** obj) {
    *obj = new castor::stager::GetUpdateStartRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_delete(castor::stager::GetUpdateStartRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_getStartRequest
  //----------------------------------------------------------------------------
  castor::stager::StartRequest* Cstager_GetUpdateStartRequest_getStartRequest(castor::stager::GetUpdateStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_fromStartRequest
  //----------------------------------------------------------------------------
  castor::stager::GetUpdateStartRequest* Cstager_GetUpdateStartRequest_fromStartRequest(castor::stager::StartRequest* obj) {
    return dynamic_cast<castor::stager::GetUpdateStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_GetUpdateStartRequest_getRequest(castor::stager::GetUpdateStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::GetUpdateStartRequest* Cstager_GetUpdateStartRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::GetUpdateStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_GetUpdateStartRequest_getIObject(castor::stager::GetUpdateStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::GetUpdateStartRequest* Cstager_GetUpdateStartRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::GetUpdateStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_print
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_print(castor::stager::GetUpdateStartRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_TYPE(int* ret) {
    *ret = castor::stager::GetUpdateStartRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_subreqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_subreqId(castor::stager::GetUpdateStartRequest* instance, u_signed64* var) {
    *var = instance->subreqId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setSubreqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setSubreqId(castor::stager::GetUpdateStartRequest* instance, u_signed64 new_var) {
    instance->setSubreqId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_diskServer
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_diskServer(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setDiskServer(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_fileSystem
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_fileSystem(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->fileSystem().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setFileSystem
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setFileSystem(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileSystem(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_flags(castor::stager::GetUpdateStartRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setFlags(castor::stager::GetUpdateStartRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_userName(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setUserName(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_euid(castor::stager::GetUpdateStartRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setEuid(castor::stager::GetUpdateStartRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_egid(castor::stager::GetUpdateStartRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setEgid(castor::stager::GetUpdateStartRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_mask(castor::stager::GetUpdateStartRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setMask(castor::stager::GetUpdateStartRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_pid(castor::stager::GetUpdateStartRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setPid(castor::stager::GetUpdateStartRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_machine(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setMachine(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_svcClassName(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setSvcClassName(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_userTag(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setUserTag(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_reqId(castor::stager::GetUpdateStartRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setReqId(castor::stager::GetUpdateStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_svcClass(castor::stager::GetUpdateStartRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setSvcClass(castor::stager::GetUpdateStartRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_client
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_client(castor::stager::GetUpdateStartRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setClient(castor::stager::GetUpdateStartRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_type
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_type(castor::stager::GetUpdateStartRequest* instance,
                                         int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_clone(castor::stager::GetUpdateStartRequest* instance,
                                          castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_id
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_id(castor::stager::GetUpdateStartRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_GetUpdateStartRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_GetUpdateStartRequest_setId(castor::stager::GetUpdateStartRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
