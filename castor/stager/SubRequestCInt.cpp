/******************************************************************************
 *                      castor/stager/SubRequestCInt.cpp
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
#include "castor/IObject.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_create
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_create(castor::stager::SubRequest** obj) {
    *obj = new castor::stager::SubRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_SubRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_delete(castor::stager::SubRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_SubRequest_getIObject(castor::stager::SubRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::SubRequest* Cstager_SubRequest_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::SubRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_print
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_print(castor::stager::SubRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_TYPE(int* ret) {
    *ret = castor::stager::SubRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setId(castor::stager::SubRequest* instance,
                               u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_id
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_id(castor::stager::SubRequest* instance,
                            u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_type
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_type(castor::stager::SubRequest* instance,
                              int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_retryCounter
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_retryCounter(castor::stager::SubRequest* instance, unsigned int* var) {
    *var = instance->retryCounter();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setRetryCounter
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setRetryCounter(castor::stager::SubRequest* instance, unsigned int new_var) {
    instance->setRetryCounter(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_fileName
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_fileName(castor::stager::SubRequest* instance, const char** var) {
    *var = instance->fileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setFileName
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setFileName(castor::stager::SubRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_protocol
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_protocol(castor::stager::SubRequest* instance, const char** var) {
    *var = instance->protocol().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setProtocol
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setProtocol(castor::stager::SubRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setProtocol(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_poolName
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_poolName(castor::stager::SubRequest* instance, const char** var) {
    *var = instance->poolName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setPoolName
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setPoolName(castor::stager::SubRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setPoolName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_xsize
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_xsize(castor::stager::SubRequest* instance, u_signed64* var) {
    *var = instance->xsize();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setXsize
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setXsize(castor::stager::SubRequest* instance, u_signed64 new_var) {
    instance->setXsize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_priority
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_priority(castor::stager::SubRequest* instance, unsigned int* var) {
    *var = instance->priority();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setPriority
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setPriority(castor::stager::SubRequest* instance, unsigned int new_var) {
    instance->setPriority(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_subreqId
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_subreqId(castor::stager::SubRequest* instance, const char** var) {
    *var = instance->subreqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setSubreqId
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setSubreqId(castor::stager::SubRequest* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSubreqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_diskcopy
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_diskcopy(castor::stager::SubRequest* instance, castor::stager::DiskCopy** var) {
    *var = instance->diskcopy();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setDiskcopy
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setDiskcopy(castor::stager::SubRequest* instance, castor::stager::DiskCopy* new_var) {
    instance->setDiskcopy(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_castorFile
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_castorFile(castor::stager::SubRequest* instance, castor::stager::CastorFile** var) {
    *var = instance->castorFile();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setCastorFile
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setCastorFile(castor::stager::SubRequest* instance, castor::stager::CastorFile* new_var) {
    instance->setCastorFile(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_parent(castor::stager::SubRequest* instance, castor::stager::SubRequest** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setParent(castor::stager::SubRequest* instance, castor::stager::SubRequest* new_var) {
    instance->setParent(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_addChild
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_addChild(castor::stager::SubRequest* instance, castor::stager::SubRequest* obj) {
    instance->addChild(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_removeChild
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_removeChild(castor::stager::SubRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeChild(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_child
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_child(castor::stager::SubRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->child();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_status
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_status(castor::stager::SubRequest* instance, castor::stager::SubRequestStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setStatus
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setStatus(castor::stager::SubRequest* instance, castor::stager::SubRequestStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_request
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_request(castor::stager::SubRequest* instance, castor::stager::FileRequest** var) {
    *var = instance->request();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_SubRequest_setRequest
  //----------------------------------------------------------------------------
  int Cstager_SubRequest_setRequest(castor::stager::SubRequest* instance, castor::stager::FileRequest* new_var) {
    instance->setRequest(new_var);
    return 0;
  }

} // End of extern "C"
