/******************************************************************************
 *                      castor/stager/ScheduleSubReqRequestCInt.cpp
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
 * @(#)$RCSfile: ScheduleSubReqRequestCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/30 08:55:29 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/ScheduleSubReqRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_create
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_create(castor::stager::ScheduleSubReqRequest** obj) {
    *obj = new castor::stager::ScheduleSubReqRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_delete(castor::stager::ScheduleSubReqRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_ScheduleSubReqRequest_getRequest(castor::stager::ScheduleSubReqRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::ScheduleSubReqRequest* Cstager_ScheduleSubReqRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::ScheduleSubReqRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_ScheduleSubReqRequest_getIObject(castor::stager::ScheduleSubReqRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::ScheduleSubReqRequest* Cstager_ScheduleSubReqRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::ScheduleSubReqRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_print
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_print(castor::stager::ScheduleSubReqRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_TYPE(int* ret) {
    *ret = castor::stager::ScheduleSubReqRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_flags(castor::stager::ScheduleSubReqRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setFlags(castor::stager::ScheduleSubReqRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_userName(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setUserName(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_euid(castor::stager::ScheduleSubReqRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setEuid(castor::stager::ScheduleSubReqRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_egid(castor::stager::ScheduleSubReqRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setEgid(castor::stager::ScheduleSubReqRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_mask(castor::stager::ScheduleSubReqRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setMask(castor::stager::ScheduleSubReqRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_pid(castor::stager::ScheduleSubReqRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setPid(castor::stager::ScheduleSubReqRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_machine(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setMachine(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_svcClassName(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setSvcClassName(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_userTag(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setUserTag(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_reqId(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setReqId(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_svcClass(castor::stager::ScheduleSubReqRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setSvcClass(castor::stager::ScheduleSubReqRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_client
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_client(castor::stager::ScheduleSubReqRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setClient(castor::stager::ScheduleSubReqRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_type
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_type(castor::stager::ScheduleSubReqRequest* instance,
                                         int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_clone(castor::stager::ScheduleSubReqRequest* instance,
                                          castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_subreqId
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_subreqId(castor::stager::ScheduleSubReqRequest* instance, u_signed64* var) {
    *var = instance->subreqId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setSubreqId
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setSubreqId(castor::stager::ScheduleSubReqRequest* instance, u_signed64 new_var) {
    instance->setSubreqId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_diskServer
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_diskServer(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setDiskServer(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_fileSystem
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_fileSystem(castor::stager::ScheduleSubReqRequest* instance, const char** var) {
    *var = instance->fileSystem().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setFileSystem
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setFileSystem(castor::stager::ScheduleSubReqRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileSystem(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_id
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_id(castor::stager::ScheduleSubReqRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ScheduleSubReqRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_ScheduleSubReqRequest_setId(castor::stager::ScheduleSubReqRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
