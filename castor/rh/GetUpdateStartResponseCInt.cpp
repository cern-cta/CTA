/******************************************************************************
 *                      castor/rh/GetUpdateStartResponseCInt.cpp
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
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_create
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_create(castor::rh::GetUpdateStartResponse** obj) {
    *obj = new castor::rh::GetUpdateStartResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_delete
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_delete(castor::rh::GetUpdateStartResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_getStartResponse
  //----------------------------------------------------------------------------
  castor::rh::StartResponse* Crh_GetUpdateStartResponse_getStartResponse(castor::rh::GetUpdateStartResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_fromStartResponse
  //----------------------------------------------------------------------------
  castor::rh::GetUpdateStartResponse* Crh_GetUpdateStartResponse_fromStartResponse(castor::rh::StartResponse* obj) {
    return dynamic_cast<castor::rh::GetUpdateStartResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_GetUpdateStartResponse_getResponse(castor::rh::GetUpdateStartResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::GetUpdateStartResponse* Crh_GetUpdateStartResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::GetUpdateStartResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_GetUpdateStartResponse_getIObject(castor::rh::GetUpdateStartResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::GetUpdateStartResponse* Crh_GetUpdateStartResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::GetUpdateStartResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_print
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_print(castor::rh::GetUpdateStartResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_TYPE(int* ret) {
    *ret = castor::rh::GetUpdateStartResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_id
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_id(castor::rh::GetUpdateStartResponse* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_setId
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_setId(castor::rh::GetUpdateStartResponse* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_client
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_client(castor::rh::GetUpdateStartResponse* instance, castor::IClient** var) {
    *var = instance->client();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_setClient
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_setClient(castor::rh::GetUpdateStartResponse* instance, castor::IClient* new_var) {
    instance->setClient(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_diskCopy
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_diskCopy(castor::rh::GetUpdateStartResponse* instance, castor::stager::DiskCopy** var) {
    *var = instance->diskCopy();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_setDiskCopy
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_setDiskCopy(castor::rh::GetUpdateStartResponse* instance, castor::stager::DiskCopy* new_var) {
    instance->setDiskCopy(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_errorCode(castor::rh::GetUpdateStartResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_setErrorCode(castor::rh::GetUpdateStartResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_errorMessage(castor::rh::GetUpdateStartResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_setErrorMessage(castor::rh::GetUpdateStartResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_type
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_type(castor::rh::GetUpdateStartResponse* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_clone
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_clone(castor::rh::GetUpdateStartResponse* instance,
                                       castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_addSources
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_addSources(castor::rh::GetUpdateStartResponse* instance, castor::stager::DiskCopyForRecall* obj) {
    instance->addSources(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_removeSources
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_removeSources(castor::rh::GetUpdateStartResponse* instance, castor::stager::DiskCopyForRecall* obj) {
    instance->removeSources(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_GetUpdateStartResponse_sources
  //----------------------------------------------------------------------------
  int Crh_GetUpdateStartResponse_sources(castor::rh::GetUpdateStartResponse* instance, castor::stager::DiskCopyForRecall*** var, int* len) {
    std::vector<castor::stager::DiskCopyForRecall*>& result = instance->sources();
    *len = result.size();
    *var = (castor::stager::DiskCopyForRecall**) malloc((*len) * sizeof(castor::stager::DiskCopyForRecall*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
