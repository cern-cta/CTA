/******************************************************************************
 *                      castor/stager/StagePutRequestCInt.cpp
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
#include "castor/stager/StagePutRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_create(castor::stager::StagePutRequest** obj) {
    *obj = new castor::stager::StagePutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_delete(castor::stager::StagePutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePutRequest_getFileRequest(castor::stager::StagePutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutRequest* Cstager_StagePutRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_print(castor::stager::StagePutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setId(castor::stager::StagePutRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_id(castor::stager::StagePutRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_type(castor::stager::StagePutRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
