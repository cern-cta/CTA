/******************************************************************************
 *                      castor/stager/StageGetRequestCInt.cpp
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
#include "castor/stager/StageGetRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_create(castor::stager::StageGetRequest** obj) {
    *obj = new castor::stager::StageGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_delete(castor::stager::StageGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageGetRequest_getFileRequest(castor::stager::StageGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetRequest* Cstager_StageGetRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_print(castor::stager::StageGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StageGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setId(castor::stager::StageGetRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_id(castor::stager::StageGetRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_type(castor::stager::StageGetRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
