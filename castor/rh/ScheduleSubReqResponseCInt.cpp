/******************************************************************************
 *                      castor/rh/ScheduleSubReqResponseCInt.cpp
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
 * @(#)$RCSfile: ScheduleSubReqResponseCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/24 11:52:24 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/ScheduleSubReqResponse.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_create
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_create(castor::rh::ScheduleSubReqResponse** obj) {
    *obj = new castor::rh::ScheduleSubReqResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_delete
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_delete(castor::rh::ScheduleSubReqResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_ScheduleSubReqResponse_getResponse(castor::rh::ScheduleSubReqResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::ScheduleSubReqResponse* Crh_ScheduleSubReqResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::ScheduleSubReqResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_ScheduleSubReqResponse_getIObject(castor::rh::ScheduleSubReqResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::ScheduleSubReqResponse* Crh_ScheduleSubReqResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::ScheduleSubReqResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_print
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_print(castor::rh::ScheduleSubReqResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_TYPE(int* ret) {
    *ret = castor::rh::ScheduleSubReqResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_errorCode(castor::rh::ScheduleSubReqResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_setErrorCode(castor::rh::ScheduleSubReqResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_errorMessage(castor::rh::ScheduleSubReqResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_setErrorMessage(castor::rh::ScheduleSubReqResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_setId
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_setId(castor::rh::ScheduleSubReqResponse* instance,
                                       u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_id
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_id(castor::rh::ScheduleSubReqResponse* instance,
                                    u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_type
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_type(castor::rh::ScheduleSubReqResponse* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_clone
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_clone(castor::rh::ScheduleSubReqResponse* instance,
                                       castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_diskCopy
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_diskCopy(castor::rh::ScheduleSubReqResponse* instance, castor::stager::DiskCopy** var) {
    *var = instance->diskCopy();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_setDiskCopy
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_setDiskCopy(castor::rh::ScheduleSubReqResponse* instance, castor::stager::DiskCopy* new_var) {
    instance->setDiskCopy(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_addSources
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_addSources(castor::rh::ScheduleSubReqResponse* instance, castor::stager::DiskCopy* obj) {
    instance->addSources(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_removeSources
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_removeSources(castor::rh::ScheduleSubReqResponse* instance, castor::stager::DiskCopy* obj) {
    instance->removeSources(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_ScheduleSubReqResponse_sources
  //----------------------------------------------------------------------------
  int Crh_ScheduleSubReqResponse_sources(castor::rh::ScheduleSubReqResponse* instance, castor::stager::DiskCopy*** var, int* len) {
    std::vector<castor::stager::DiskCopy*>& result = instance->sources();
    *len = result.size();
    *var = (castor::stager::DiskCopy**) malloc((*len) * sizeof(castor::stager::DiskCopy*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
