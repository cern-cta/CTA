/******************************************************************************
 *                      castor/stager/StageUpdateRequestCInt.cpp
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
#include "castor/stager/StageUpdateRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_create(castor::stager::StageUpdateRequest** obj) {
    *obj = new castor::stager::StageUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_delete(castor::stager::StageUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateRequest_getRequest(castor::stager::StageUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateRequest* Cstager_StageUpdateRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_print(castor::stager::StageUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setId(castor::stager::StageUpdateRequest* instance,
                                       u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_id(castor::stager::StageUpdateRequest* instance,
                                    u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_type(castor::stager::StageUpdateRequest* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
