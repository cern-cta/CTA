/******************************************************************************
 *                      castor/stager/RequestCInt.cpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/RequestStatusCodes.hpp"
#include "castor/stager/SubRequest.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_Request_delete
  //----------------------------------------------------------------------------
  int Cstager_Request_delete(castor::stager::Request* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_Request_getIObject(castor::stager::Request* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_Request_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::Request*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_print
  //----------------------------------------------------------------------------
  int Cstager_Request_print(castor::stager::Request* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_flags
  //----------------------------------------------------------------------------
  int Cstager_Request_flags(castor::stager::Request* instance, u_signed64* var) {
    *var = instance->flags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setFlags
  //----------------------------------------------------------------------------
  int Cstager_Request_setFlags(castor::stager::Request* instance, u_signed64 new_var) {
    instance->setFlags(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_userName
  //----------------------------------------------------------------------------
  int Cstager_Request_userName(castor::stager::Request* instance, const char** var) {
    *var = instance->userName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setUserName
  //----------------------------------------------------------------------------
  int Cstager_Request_setUserName(castor::stager::Request* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setUserName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_euid
  //----------------------------------------------------------------------------
  int Cstager_Request_euid(castor::stager::Request* instance, unsigned long* var) {
    *var = instance->euid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setEuid
  //----------------------------------------------------------------------------
  int Cstager_Request_setEuid(castor::stager::Request* instance, unsigned long new_var) {
    instance->setEuid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_egid
  //----------------------------------------------------------------------------
  int Cstager_Request_egid(castor::stager::Request* instance, unsigned long* var) {
    *var = instance->egid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setEgid
  //----------------------------------------------------------------------------
  int Cstager_Request_setEgid(castor::stager::Request* instance, unsigned long new_var) {
    instance->setEgid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_mask
  //----------------------------------------------------------------------------
  int Cstager_Request_mask(castor::stager::Request* instance, unsigned long* var) {
    *var = instance->mask();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setMask
  //----------------------------------------------------------------------------
  int Cstager_Request_setMask(castor::stager::Request* instance, unsigned long new_var) {
    instance->setMask(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_pid
  //----------------------------------------------------------------------------
  int Cstager_Request_pid(castor::stager::Request* instance, unsigned long* var) {
    *var = instance->pid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setPid
  //----------------------------------------------------------------------------
  int Cstager_Request_setPid(castor::stager::Request* instance, unsigned long new_var) {
    instance->setPid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_machine
  //----------------------------------------------------------------------------
  int Cstager_Request_machine(castor::stager::Request* instance, const char** var) {
    *var = instance->machine().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setMachine
  //----------------------------------------------------------------------------
  int Cstager_Request_setMachine(castor::stager::Request* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMachine(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_projectName
  //----------------------------------------------------------------------------
  int Cstager_Request_projectName(castor::stager::Request* instance, const char** var) {
    *var = instance->projectName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setProjectName
  //----------------------------------------------------------------------------
  int Cstager_Request_setProjectName(castor::stager::Request* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setProjectName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_Request_addSubRequests(castor::stager::Request* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_Request_removeSubRequests(castor::stager::Request* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_subRequests
  //----------------------------------------------------------------------------
  int Cstager_Request_subRequests(castor::stager::Request* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_client
  //----------------------------------------------------------------------------
  int Cstager_Request_client(castor::stager::Request* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setClient
  //----------------------------------------------------------------------------
  int Cstager_Request_setClient(castor::stager::Request* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_status
  //----------------------------------------------------------------------------
  int Cstager_Request_status(castor::stager::Request* instance, castor::stager::RequestStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Request_setStatus
  //----------------------------------------------------------------------------
  int Cstager_Request_setStatus(castor::stager::Request* instance, castor::stager::RequestStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
