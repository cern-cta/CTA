/******************************************************************************
 *                      castor/stager/StageGetNextRequestCInt.cpp
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
#include "castor/stager/Request.hpp"
#include "castor/stager/StageGetNextRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_create(castor::stager::StageGetNextRequest** obj) {
    *obj = new castor::stager::StageGetNextRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_delete(castor::stager::StageGetNextRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageGetNextRequest_getRequest(castor::stager::StageGetNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetNextRequest* Cstager_StageGetNextRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageGetNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_print(castor::stager::StageGetNextRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_TYPE(int* ret) {
    *ret = castor::stager::StageGetNextRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setId(castor::stager::StageGetNextRequest* instance,
                                        u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_id(castor::stager::StageGetNextRequest* instance,
                                     u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_type(castor::stager::StageGetNextRequest* instance,
                                       int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_parent
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_parent(castor::stager::StageGetNextRequest* instance, castor::stager::Request** var) {
    *var = instance->parent();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetNextRequest_setParent
  //----------------------------------------------------------------------------
  int Cstager_StageGetNextRequest_setParent(castor::stager::StageGetNextRequest* instance, castor::stager::Request* new_var) {
    instance->setParent(new_var);
    return 0;
  }

} // End of extern "C"
