/******************************************************************************
 *                      castor/stager/StagePrepareToGetRequestCInt.cpp
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
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_create(castor::stager::StagePrepareToGetRequest** obj) {
    *obj = new castor::stager::StagePrepareToGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_delete(castor::stager::StagePrepareToGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StagePrepareToGetRequest_getFileRequest(castor::stager::StagePrepareToGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToGetRequest* Cstager_StagePrepareToGetRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StagePrepareToGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_print(castor::stager::StagePrepareToGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setId(castor::stager::StagePrepareToGetRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_id(castor::stager::StagePrepareToGetRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_type(castor::stager::StagePrepareToGetRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
