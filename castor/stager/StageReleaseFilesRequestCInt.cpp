/******************************************************************************
 *                      castor/stager/StageReleaseFilesRequestCInt.cpp
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
#include "castor/stager/StageReleaseFilesRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_create(castor::stager::StageReleaseFilesRequest** obj) {
    *obj = new castor::stager::StageReleaseFilesRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_delete(castor::stager::StageReleaseFilesRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_getFileRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_StageReleaseFilesRequest_getFileRequest(castor::stager::StageReleaseFilesRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_fromFileRequest
  //----------------------------------------------------------------------------
  castor::stager::StageReleaseFilesRequest* Cstager_StageReleaseFilesRequest_fromFileRequest(castor::stager::FileRequest* obj) {
    return dynamic_cast<castor::stager::StageReleaseFilesRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_print(castor::stager::StageReleaseFilesRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_TYPE(int* ret) {
    *ret = castor::stager::StageReleaseFilesRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_setId(castor::stager::StageReleaseFilesRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_id(castor::stager::StageReleaseFilesRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageReleaseFilesRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageReleaseFilesRequest_type(castor::stager::StageReleaseFilesRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
