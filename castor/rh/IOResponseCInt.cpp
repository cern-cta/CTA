/******************************************************************************
 *                      castor/rh/IOResponseCInt.cpp
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
#include "castor/rh/IOResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_IOResponse_create
  //----------------------------------------------------------------------------
  int Crh_IOResponse_create(castor::rh::IOResponse** obj) {
    *obj = new castor::rh::IOResponse();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_IOResponse_delete
  //----------------------------------------------------------------------------
  int Crh_IOResponse_delete(castor::rh::IOResponse* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_getFileResponse
  //----------------------------------------------------------------------------
  castor::rh::FileResponse* Crh_IOResponse_getFileResponse(castor::rh::IOResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fromFileResponse
  //----------------------------------------------------------------------------
  castor::rh::IOResponse* Crh_IOResponse_fromFileResponse(castor::rh::FileResponse* obj) {
    return dynamic_cast<castor::rh::IOResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_getResponse
  //----------------------------------------------------------------------------
  castor::rh::Response* Crh_IOResponse_getResponse(castor::rh::IOResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fromResponse
  //----------------------------------------------------------------------------
  castor::rh::IOResponse* Crh_IOResponse_fromResponse(castor::rh::Response* obj) {
    return dynamic_cast<castor::rh::IOResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_IOResponse_getIObject(castor::rh::IOResponse* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::IOResponse* Crh_IOResponse_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::IOResponse*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_print
  //----------------------------------------------------------------------------
  int Crh_IOResponse_print(castor::rh::IOResponse* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_TYPE
  //----------------------------------------------------------------------------
  int Crh_IOResponse_TYPE(int* ret) {
    *ret = castor::rh::IOResponse::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_type
  //----------------------------------------------------------------------------
  int Crh_IOResponse_type(castor::rh::IOResponse* instance,
                          int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_status
  //----------------------------------------------------------------------------
  int Crh_IOResponse_status(castor::rh::IOResponse* instance, unsigned int* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setStatus
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setStatus(castor::rh::IOResponse* instance, unsigned int new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_castorFileName
  //----------------------------------------------------------------------------
  int Crh_IOResponse_castorFileName(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->castorFileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setCastorFileName
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setCastorFileName(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setCastorFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fileSize
  //----------------------------------------------------------------------------
  int Crh_IOResponse_fileSize(castor::rh::IOResponse* instance, u_signed64* var) {
    *var = instance->fileSize();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setFileSize
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setFileSize(castor::rh::IOResponse* instance, u_signed64 new_var) {
    instance->setFileSize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fileId
  //----------------------------------------------------------------------------
  int Crh_IOResponse_fileId(castor::rh::IOResponse* instance, u_signed64* var) {
    *var = instance->fileId();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setFileId
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setFileId(castor::rh::IOResponse* instance, u_signed64 new_var) {
    instance->setFileId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_subreqId
  //----------------------------------------------------------------------------
  int Crh_IOResponse_subreqId(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->subreqId().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setSubreqId
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setSubreqId(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSubreqId(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_errorCode
  //----------------------------------------------------------------------------
  int Crh_IOResponse_errorCode(castor::rh::IOResponse* instance, unsigned int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setErrorCode
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setErrorCode(castor::rh::IOResponse* instance, unsigned int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_errorMessage
  //----------------------------------------------------------------------------
  int Crh_IOResponse_errorMessage(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setErrorMessage
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setErrorMessage(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setId
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setId(castor::rh::IOResponse* instance,
                           u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_id
  //----------------------------------------------------------------------------
  int Crh_IOResponse_id(castor::rh::IOResponse* instance,
                        u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_clone
  //----------------------------------------------------------------------------
  int Crh_IOResponse_clone(castor::rh::IOResponse* instance,
                           castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_fileName
  //----------------------------------------------------------------------------
  int Crh_IOResponse_fileName(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->fileName().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setFileName
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setFileName(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setFileName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_server
  //----------------------------------------------------------------------------
  int Crh_IOResponse_server(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->server().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setServer
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setServer(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_port
  //----------------------------------------------------------------------------
  int Crh_IOResponse_port(castor::rh::IOResponse* instance, int* var) {
    *var = instance->port();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setPort
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setPort(castor::rh::IOResponse* instance, int new_var) {
    instance->setPort(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_protocol
  //----------------------------------------------------------------------------
  int Crh_IOResponse_protocol(castor::rh::IOResponse* instance, const char** var) {
    *var = instance->protocol().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_IOResponse_setProtocol
  //----------------------------------------------------------------------------
  int Crh_IOResponse_setProtocol(castor::rh::IOResponse* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setProtocol(snew_var);
    return 0;
  }

} // End of extern "C"
