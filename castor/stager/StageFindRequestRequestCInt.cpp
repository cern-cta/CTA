/******************************************************************************
 *                      castor/stager/StageFindRequestRequestCInt.cpp
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
#include "castor/stager/StageFindRequestRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_create(castor::stager::StageFindRequestRequest** obj) {
    *obj = new castor::stager::StageFindRequestRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_delete(castor::stager::StageFindRequestRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageFindRequestRequest_getRequest(castor::stager::StageFindRequestRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFindRequestRequest* Cstager_StageFindRequestRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageFindRequestRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_print(castor::stager::StageFindRequestRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFindRequestRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_setId(castor::stager::StageFindRequestRequest* instance,
                                            u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_id(castor::stager::StageFindRequestRequest* instance,
                                         u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFindRequestRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFindRequestRequest_type(castor::stager::StageFindRequestRequest* instance,
                                           int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
