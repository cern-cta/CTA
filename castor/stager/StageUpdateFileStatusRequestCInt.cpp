/******************************************************************************
 *                      castor/stager/StageUpdateFileStatusRequestCInt.cpp
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
 * @(#)$RCSfile: StageUpdateFileStatusRequestCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/04 10:26:16 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageUpdateFileStatusRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_create(castor::stager::StageUpdateFileStatusRequest** obj) {
    *obj = new castor::stager::StageUpdateFileStatusRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_delete(castor::stager::StageUpdateFileStatusRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateFileStatusRequest_getRequest(castor::stager::StageUpdateFileStatusRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateFileStatusRequest* Cstager_StageUpdateFileStatusRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateFileStatusRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_print(castor::stager::StageUpdateFileStatusRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateFileStatusRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setId(castor::stager::StageUpdateFileStatusRequest* instance,
                                                 u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_id(castor::stager::StageUpdateFileStatusRequest* instance,
                                              u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_type(castor::stager::StageUpdateFileStatusRequest* instance,
                                                int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
