/******************************************************************************
 *                      castor/stager/StagePutDoneRequestCInt.cpp
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
#include "castor/stager/StagePutDoneRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_create(castor::stager::StagePutDoneRequest** obj) {
    *obj = new castor::stager::StagePutDoneRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_delete(castor::stager::StagePutDoneRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePutDoneRequest_getRequest(castor::stager::StagePutDoneRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutDoneRequest* Cstager_StagePutDoneRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePutDoneRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_print(castor::stager::StagePutDoneRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutDoneRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setId(castor::stager::StagePutDoneRequest* instance,
                                        u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_id(castor::stager::StagePutDoneRequest* instance,
                                     u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_type(castor::stager::StagePutDoneRequest* instance,
                                       int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
