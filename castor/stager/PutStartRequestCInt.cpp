/******************************************************************************
 *                      castor/stager/PutStartRequestCInt.cpp
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
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StartRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_create
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_create(castor::stager::PutStartRequest** obj) {
    *obj = new castor::stager::PutStartRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_delete(castor::stager::PutStartRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_getStartRequest
  //----------------------------------------------------------------------------
  castor::stager::StartRequest* Cstager_PutStartRequest_getStartRequest(castor::stager::PutStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_fromStartRequest
  //----------------------------------------------------------------------------
  castor::stager::PutStartRequest* Cstager_PutStartRequest_fromStartRequest(castor::stager::StartRequest* obj) {
    return dynamic_cast<castor::stager::PutStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_PutStartRequest_getRequest(castor::stager::PutStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::PutStartRequest* Cstager_PutStartRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::PutStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_PutStartRequest_getIObject(castor::stager::PutStartRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::PutStartRequest* Cstager_PutStartRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::PutStartRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_print
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_print(castor::stager::PutStartRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_TYPE(int* ret) {
    *ret = castor::stager::PutStartRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_subreqId
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_subreqId(castor::stager::PutStartRequest* instance, u_signed64* var) {
    *var = instance->subreqId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setSubreqId
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setSubreqId(castor::stager::PutStartRequest* instance, u_signed64 new_var) {
    instance->setSubreqId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_diskServer
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_diskServer(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setDiskServer(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_fileSystem
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_fileSystem(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->fileSystem().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setFileSystem
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setFileSystem(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileSystem(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_flags(castor::stager::PutStartRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setFlags(castor::stager::PutStartRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_userName(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setUserName(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_euid(castor::stager::PutStartRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setEuid(castor::stager::PutStartRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_egid(castor::stager::PutStartRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setEgid(castor::stager::PutStartRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_mask(castor::stager::PutStartRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setMask(castor::stager::PutStartRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_pid(castor::stager::PutStartRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setPid(castor::stager::PutStartRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_machine(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setMachine(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_svcClassName(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setSvcClassName(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_userTag(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setUserTag(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_reqId(castor::stager::PutStartRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setReqId(castor::stager::PutStartRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_svcClass(castor::stager::PutStartRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setSvcClass(castor::stager::PutStartRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_client
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_client(castor::stager::PutStartRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setClient(castor::stager::PutStartRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_type
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_type(castor::stager::PutStartRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_clone(castor::stager::PutStartRequest* instance,
                                    castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_id
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_id(castor::stager::PutStartRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_PutStartRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_PutStartRequest_setId(castor::stager::PutStartRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
