/******************************************************************************
 *                      castor/stager/StagePrepareToPutRequestCInt.cpp
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
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_create(castor::stager::StagePrepareToPutRequest** obj) {
    *obj = new castor::stager::StagePrepareToPutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_delete(castor::stager::StagePrepareToPutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToPutRequest_getFileRequest(castor::stager::StagePrepareToPutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToPutRequest* Cstager_StagePrepareToPutRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToPutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_print(castor::stager::StagePrepareToPutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToPutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setId(castor::stager::StagePrepareToPutRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_id(castor::stager::StagePrepareToPutRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_type(castor::stager::StagePrepareToPutRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
