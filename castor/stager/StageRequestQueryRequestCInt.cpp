/******************************************************************************
 *                      castor/stager/StageRequestQueryRequestCInt.cpp
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
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_create(castor::stager::StageRequestQueryRequest** obj) {
    *obj = new castor::stager::StageRequestQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_delete(castor::stager::StageRequestQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageRequestQueryRequest_getRequest(castor::stager::StageRequestQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRequestQueryRequest* Cstager_StageRequestQueryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageRequestQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_print(castor::stager::StageRequestQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageRequestQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setId(castor::stager::StageRequestQueryRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_id(castor::stager::StageRequestQueryRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_type(castor::stager::StageRequestQueryRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
