/******************************************************************************
 *                      castor/stager/StageAbortRequestCInt.cpp
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
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/StageAbortRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_create(castor::stager::StageAbortRequest** obj) {
    *obj = new castor::stager::StageAbortRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_delete(castor::stager::StageAbortRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageAbortRequest_getFileRequest(castor::stager::StageAbortRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageAbortRequest* Cstager_StageAbortRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageAbortRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_print(castor::stager::StageAbortRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_TYPE(int* ret) {
    *ret = castor::stager::StageAbortRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_setId(castor::stager::StageAbortRequest* instance,
                                      u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_id(castor::stager::StageAbortRequest* instance,
                                   u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageAbortRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageAbortRequest_type(castor::stager::StageAbortRequest* instance,
                                     int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
