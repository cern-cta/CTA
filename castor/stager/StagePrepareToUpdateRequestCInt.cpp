/******************************************************************************
 *                      castor/stager/StagePrepareToUpdateRequestCInt.cpp
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
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_create(castor::stager::StagePrepareToUpdateRequest** obj) {
    *obj = new castor::stager::StagePrepareToUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_delete(castor::stager::StagePrepareToUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToUpdateRequest_getFileRequest(castor::stager::StagePrepareToUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToUpdateRequest* Cstager_StagePrepareToUpdateRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_print(castor::stager::StagePrepareToUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setId(castor::stager::StagePrepareToUpdateRequest* instance,
                                                u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_id(castor::stager::StagePrepareToUpdateRequest* instance,
                                             u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_type(castor::stager::StagePrepareToUpdateRequest* instance,
                                               int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
