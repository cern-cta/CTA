/******************************************************************************
 *                      castor/stager/StageRmRequestCInt.cpp
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
#include "castor/stager/StageRmRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_create(castor::stager::StageRmRequest** obj) {
    *obj = new castor::stager::StageRmRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_delete(castor::stager::StageRmRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageRmRequest_getRequest(castor::stager::StageRmRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRmRequest* Cstager_StageRmRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageRmRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_print(castor::stager::StageRmRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_TYPE(int* ret) {
    *ret = castor::stager::StageRmRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_setId(castor::stager::StageRmRequest* instance,
                                   u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_id(castor::stager::StageRmRequest* instance,
                                u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_type(castor::stager::StageRmRequest* instance,
                                  int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
