/******************************************************************************
 *                      castor/rh/FileQueryResponseCInt.cpp
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
#include "castor/rh/FileQueryResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_create
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_create(castor::rh::FileQueryResponse** obj) {
    *obj = new castor::rh::FileQueryResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_delete
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_delete(castor::rh::FileQueryResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_FileQueryResponse_getResponse(castor::rh::FileQueryResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::FileQueryResponse* Crh_FileQueryResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::FileQueryResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_FileQueryResponse_getIObject(castor::rh::FileQueryResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::FileQueryResponse* Crh_FileQueryResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::FileQueryResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_print
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_print(castor::rh::FileQueryResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_TYPE(int* ret) {
    *ret = castor::rh::FileQueryResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setId
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setId(castor::rh::FileQueryResponse* instance,
                                  u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_id
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_id(castor::rh::FileQueryResponse* instance,
                               u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_type
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_type(castor::rh::FileQueryResponse* instance,
                                 int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_clone
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_clone(castor::rh::FileQueryResponse* instance,
                                  castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_fileName
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_fileName(castor::rh::FileQueryResponse* instance, const char** var) {
    *var = instance->fileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setFileName
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setFileName(castor::rh::FileQueryResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_fileId
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_fileId(castor::rh::FileQueryResponse* instance, u_signed64* var) {
    *var = instance->fileId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setFileId
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setFileId(castor::rh::FileQueryResponse* instance, u_signed64 new_var) {
    instance->setFileId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_status
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_status(castor::rh::FileQueryResponse* instance, unsigned int* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setStatus
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setStatus(castor::rh::FileQueryResponse* instance, unsigned int new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_size
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_size(castor::rh::FileQueryResponse* instance, u_signed64* var) {
    *var = instance->size();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setSize
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setSize(castor::rh::FileQueryResponse* instance, u_signed64 new_var) {
    instance->setSize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_poolName
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_poolName(castor::rh::FileQueryResponse* instance, const char** var) {
    *var = instance->poolName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setPoolName
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setPoolName(castor::rh::FileQueryResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setPoolName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_creationTime
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_creationTime(castor::rh::FileQueryResponse* instance, u_signed64* var) {
    *var = instance->creationTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setCreationTime
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setCreationTime(castor::rh::FileQueryResponse* instance, u_signed64 new_var) {
    instance->setCreationTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_accessTime
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_accessTime(castor::rh::FileQueryResponse* instance, u_signed64* var) {
    *var = instance->accessTime();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setAccessTime
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setAccessTime(castor::rh::FileQueryResponse* instance, u_signed64 new_var) {
    instance->setAccessTime(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_nbAccesses
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_nbAccesses(castor::rh::FileQueryResponse* instance, unsigned int* var) {
    *var = instance->nbAccesses();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileQueryResponse_setNbAccesses
  //----------------------------------------------------------------------------
  int Crh_FileQueryResponse_setNbAccesses(castor::rh::FileQueryResponse* instance, unsigned int new_var) {
    instance->setNbAccesses(new_var);
    return 0;
  }

} // End of extern "C"
