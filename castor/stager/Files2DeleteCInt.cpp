/******************************************************************************
 *                      castor/stager/Files2DeleteCInt.cpp
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
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_create
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_create(castor::stager::Files2Delete** obj) {
    *obj = new castor::stager::Files2Delete();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_delete
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_delete(castor::stager::Files2Delete* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_Files2Delete_getRequest(castor::stager::Files2Delete* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::Files2Delete* Cstager_Files2Delete_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::Files2Delete*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_Files2Delete_getIObject(castor::stager::Files2Delete* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::Files2Delete* Cstager_Files2Delete_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::Files2Delete*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_print
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_print(castor::stager::Files2Delete* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_TYPE
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_TYPE(int* ret) {
    *ret = castor::stager::Files2Delete::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_flags
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_flags(castor::stager::Files2Delete* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setFlags
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setFlags(castor::stager::Files2Delete* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_userName
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_userName(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setUserName
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setUserName(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_euid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_euid(castor::stager::Files2Delete* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setEuid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setEuid(castor::stager::Files2Delete* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_egid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_egid(castor::stager::Files2Delete* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setEgid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setEgid(castor::stager::Files2Delete* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_mask
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_mask(castor::stager::Files2Delete* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setMask
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setMask(castor::stager::Files2Delete* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_pid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_pid(castor::stager::Files2Delete* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setPid
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setPid(castor::stager::Files2Delete* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_machine
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_machine(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setMachine
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setMachine(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_svcClassName(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setSvcClassName(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_userTag
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_userTag(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setUserTag(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_reqId
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_reqId(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setReqId
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setReqId(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_creationTime
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_creationTime(castor::stager::Files2Delete* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setCreationTime
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setCreationTime(castor::stager::Files2Delete* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_lastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_lastModificationTime(castor::stager::Files2Delete* instance, u_signed64* var) {
    *var = instance->lastModificationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setLastModificationTime
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setLastModificationTime(castor::stager::Files2Delete* instance, u_signed64 new_var) {
    instance->setLastModificationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_svcClass
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_svcClass(castor::stager::Files2Delete* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setSvcClass(castor::stager::Files2Delete* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_client
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_client(castor::stager::Files2Delete* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setClient
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setClient(castor::stager::Files2Delete* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_type
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_type(castor::stager::Files2Delete* instance,
                                int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_clone
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_clone(castor::stager::Files2Delete* instance,
                                 castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_diskServer
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_diskServer(castor::stager::Files2Delete* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setDiskServer(castor::stager::Files2Delete* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_id
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_id(castor::stager::Files2Delete* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Files2Delete_setId
  //----------------------------------------------------------------------------
  int Cstager_Files2Delete_setId(castor::stager::Files2Delete* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
