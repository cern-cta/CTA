/******************************************************************************
 *                      castor/stager/StageFileQueryRequestCInt.cpp
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
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_create(castor::stager::StageFileQueryRequest** obj) {
    *obj = new castor::stager::StageFileQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_delete(castor::stager::StageFileQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_getQryRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_StageFileQueryRequest_getQryRequest(castor::stager::StageFileQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fromQryRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFileQueryRequest* Cstager_StageFileQueryRequest_fromQryRequest(castor::stager::QryRequest* obj) {
    return dynamic_cast<castor::stager::StageFileQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageFileQueryRequest_getRequest(castor::stager::StageFileQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFileQueryRequest* Cstager_StageFileQueryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageFileQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_StageFileQueryRequest_getIObject(castor::stager::StageFileQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::StageFileQueryRequest* Cstager_StageFileQueryRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::StageFileQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_print(castor::stager::StageFileQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFileQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_addParameters
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_addParameters(castor::stager::StageFileQueryRequest* instance, castor::stager::QueryParameter* obj) {
    instance->addParameters(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_removeParameters
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_removeParameters(castor::stager::StageFileQueryRequest* instance, castor::stager::QueryParameter* obj) {
    instance->removeParameters(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_parameters
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_parameters(castor::stager::StageFileQueryRequest* instance, castor::stager::QueryParameter*** var, int* len) {
    std::vector<castor::stager::QueryParameter*>& result = instance->parameters();
    *len = result.size();
    *var = (castor::stager::QueryParameter**) malloc((*len) * sizeof(castor::stager::QueryParameter*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_flags
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_flags(castor::stager::StageFileQueryRequest* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setFlags
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setFlags(castor::stager::StageFileQueryRequest* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_userName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_userName(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setUserName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setUserName(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_euid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_euid(castor::stager::StageFileQueryRequest* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setEuid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setEuid(castor::stager::StageFileQueryRequest* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_egid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_egid(castor::stager::StageFileQueryRequest* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setEgid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setEgid(castor::stager::StageFileQueryRequest* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_mask
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_mask(castor::stager::StageFileQueryRequest* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setMask
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setMask(castor::stager::StageFileQueryRequest* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_pid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_pid(castor::stager::StageFileQueryRequest* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setPid
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setPid(castor::stager::StageFileQueryRequest* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_machine
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_machine(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setMachine
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setMachine(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_svcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_svcClassName(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->svcClassName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setSvcClassName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setSvcClassName(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSvcClassName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_userTag
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_userTag(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->userTag().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setUserTag
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setUserTag(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserTag(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_reqId
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_reqId(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->reqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setReqId
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setReqId(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setReqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_svcClass
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_svcClass(castor::stager::StageFileQueryRequest* instance, castor::stager::SvcClass** var) {
    *var = instance->svcClass();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setSvcClass
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setSvcClass(castor::stager::StageFileQueryRequest* instance, castor::stager::SvcClass* new_var) {
    instance->setSvcClass(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_client
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_client(castor::stager::StageFileQueryRequest* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setClient
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setClient(castor::stager::StageFileQueryRequest* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_type(castor::stager::StageFileQueryRequest* instance,
                                         int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_clone
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_clone(castor::stager::StageFileQueryRequest* instance,
                                          castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fileName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_fileName(castor::stager::StageFileQueryRequest* instance, const char** var) {
    *var = instance->fileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setFileName
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setFileName(castor::stager::StageFileQueryRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_id(castor::stager::StageFileQueryRequest* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setId(castor::stager::StageFileQueryRequest* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
