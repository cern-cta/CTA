/******************************************************************************
 *                      castor/stager/QryRequestCInt.cpp
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
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_delete(castor::stager::QryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_QryRequest_getRequest(castor::stager::QryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_QryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::QryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_QryRequest_getIObject(castor::stager::QryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_QryRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::QryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_print(castor::stager::QryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_flags(castor::stager::QryRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setFlags(castor::stager::QryRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_userName(castor::stager::QryRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setUserName(castor::stager::QryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_euid(castor::stager::QryRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setEuid(castor::stager::QryRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_egid(castor::stager::QryRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setEgid(castor::stager::QryRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_mask(castor::stager::QryRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setMask(castor::stager::QryRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_pid(castor::stager::QryRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setPid(castor::stager::QryRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_machine(castor::stager::QryRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setMachine(castor::stager::QryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_svcClassName(castor::stager::QryRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setSvcClassName(castor::stager::QryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_userTag(castor::stager::QryRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setUserTag(castor::stager::QryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_reqId(castor::stager::QryRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setReqId(castor::stager::QryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_svcClass(castor::stager::QryRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setSvcClass(castor::stager::QryRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_client
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_client(castor::stager::QryRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_setClient(castor::stager::QryRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

} // End of extern "C"
