/******************************************************************************
 *                      castor/rh/FileResponseCInt.cpp
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
#include "castor/rh/FileResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_FileResponse_create
  //----------------------------------------------------------------------------
  int Crh_FileResponse_create(castor::rh::FileResponse** obj) {
    *obj = new castor::rh::FileResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_FileResponse_delete
  //----------------------------------------------------------------------------
  int Crh_FileResponse_delete(castor::rh::FileResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_FileResponse_getResponse(castor::rh::FileResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::FileResponse* Crh_FileResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::FileResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_FileResponse_getIObject(castor::rh::FileResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::FileResponse* Crh_FileResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::FileResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_print
  //----------------------------------------------------------------------------
  int Crh_FileResponse_print(castor::rh::FileResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_FileResponse_TYPE(int* ret) {
    *ret = castor::rh::FileResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_FileResponse_errorCode(castor::rh::FileResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setErrorCode(castor::rh::FileResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_FileResponse_errorMessage(castor::rh::FileResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setErrorMessage(castor::rh::FileResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setId
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setId(castor::rh::FileResponse* instance,
                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_id
  //----------------------------------------------------------------------------
  int Crh_FileResponse_id(castor::rh::FileResponse* instance,
                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_type
  //----------------------------------------------------------------------------
  int Crh_FileResponse_type(castor::rh::FileResponse* instance,
                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_clone
  //----------------------------------------------------------------------------
  int Crh_FileResponse_clone(castor::rh::FileResponse* instance,
                             castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_status
  //----------------------------------------------------------------------------
  int Crh_FileResponse_status(castor::rh::FileResponse* instance, unsigned int* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setStatus
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setStatus(castor::rh::FileResponse* instance, unsigned int new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_castorFileName
  //----------------------------------------------------------------------------
  int Crh_FileResponse_castorFileName(castor::rh::FileResponse* instance, const char** var) {
    *var = instance->castorFileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setCastorFileName
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setCastorFileName(castor::rh::FileResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setCastorFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_fileSize
  //----------------------------------------------------------------------------
  int Crh_FileResponse_fileSize(castor::rh::FileResponse* instance, u_signed64* var) {
    *var = instance->fileSize();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setFileSize
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setFileSize(castor::rh::FileResponse* instance, u_signed64 new_var) {
    instance->setFileSize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_fileId
  //----------------------------------------------------------------------------
  int Crh_FileResponse_fileId(castor::rh::FileResponse* instance, u_signed64* var) {
    *var = instance->fileId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setFileId
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setFileId(castor::rh::FileResponse* instance, u_signed64 new_var) {
    instance->setFileId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_subreqId
  //----------------------------------------------------------------------------
  int Crh_FileResponse_subreqId(castor::rh::FileResponse* instance, const char** var) {
    *var = instance->subreqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_FileResponse_setSubreqId
  //----------------------------------------------------------------------------
  int Crh_FileResponse_setSubreqId(castor::rh::FileResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSubreqId(snew_var);
    return 0;
  }

} // End of extern "C"
