/******************************************************************************
 *                      castor/stager/StageFileQueryRequestCInt.cpp
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
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_create(castor::stager::StageFileQueryRequest** obj) {
    *obj = new castor::stager::StageFileQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_delete(castor::stager::StageFileQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_getQryRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_StageFileQueryRequest_getQryRequest(castor::stager::StageFileQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fromQryRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFileQueryRequest* Cstager_StageFileQueryRequest_fromQryRequest(castor::stager::QryRequest* obj) {
    return dynamic_cast<castor::stager::StageFileQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_print(castor::stager::StageFileQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFileQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setId(castor::stager::StageFileQueryRequest* instance,
                                          u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_id(castor::stager::StageFileQueryRequest* instance,
                                       u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_type(castor::stager::StageFileQueryRequest* instance,
                                         int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
