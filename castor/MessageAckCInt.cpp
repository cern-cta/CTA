/******************************************************************************
 *                      castor/MessageAckCInt.cpp
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
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // C_MessageAck_create
  //----------------------------------------------------------------------------
  int C_MessageAck_create(castor::MessageAck** obj) {
    *obj = new castor::MessageAck();
    return 0;
  }
  //----------------------------------------------------------------------------
  // C_MessageAck_delete
  //----------------------------------------------------------------------------
  int C_MessageAck_delete(castor::MessageAck* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* C_MessageAck_getIObject(castor::MessageAck* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_fromIObject
  //----------------------------------------------------------------------------
  castor::MessageAck* C_MessageAck_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::MessageAck*>(obj);
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_print
  //----------------------------------------------------------------------------
  int C_MessageAck_print(castor::MessageAck* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_TYPE
  //----------------------------------------------------------------------------
  int C_MessageAck_TYPE(int* ret) {
    *ret = castor::MessageAck::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_setId
  //----------------------------------------------------------------------------
  int C_MessageAck_setId(castor::MessageAck* instance,
                         u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_id
  //----------------------------------------------------------------------------
  int C_MessageAck_id(castor::MessageAck* instance,
                      u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_type
  //----------------------------------------------------------------------------
  int C_MessageAck_type(castor::MessageAck* instance,
                        int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_status
  //----------------------------------------------------------------------------
  int C_MessageAck_status(castor::MessageAck* instance, bool* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_setStatus
  //----------------------------------------------------------------------------
  int C_MessageAck_setStatus(castor::MessageAck* instance, bool new_var) {
    instance->setStatus(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_errorCode
  //----------------------------------------------------------------------------
  int C_MessageAck_errorCode(castor::MessageAck* instance, int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_setErrorCode
  //----------------------------------------------------------------------------
  int C_MessageAck_setErrorCode(castor::MessageAck* instance, int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_errorMessage
  //----------------------------------------------------------------------------
  int C_MessageAck_errorMessage(castor::MessageAck* instance, const char** var) {
    *var = instance->errorMessage().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_MessageAck_setErrorMessage
  //----------------------------------------------------------------------------
  int C_MessageAck_setErrorMessage(castor::MessageAck* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrorMessage(snew_var);
    return 0;
  }

} // End of extern "C"
