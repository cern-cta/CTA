/******************************************************************************
 *                      castor/stager/StageUpdateNextRequestCInt.cpp
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
#include "castor/stager/ReqIdRequest.hpp"
#include "castor/stager/StageUpdateNextRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_create(castor::stager::StageUpdateNextRequest** obj) {
    *obj = new castor::stager::StageUpdateNextRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_delete(castor::stager::StageUpdateNextRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_getReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_StageUpdateNextRequest_getReqIdRequest(castor::stager::StageUpdateNextRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_fromReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateNextRequest* Cstager_StageUpdateNextRequest_fromReqIdRequest(castor::stager::ReqIdRequest* obj) {
    return dynamic_cast<castor::stager::StageUpdateNextRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_print(castor::stager::StageUpdateNextRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateNextRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_setId(castor::stager::StageUpdateNextRequest* instance,
                                           u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_id(castor::stager::StageUpdateNextRequest* instance,
                                        u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateNextRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateNextRequest_type(castor::stager::StageUpdateNextRequest* instance,
                                          int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
