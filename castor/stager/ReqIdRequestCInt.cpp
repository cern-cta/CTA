/******************************************************************************
 *                      castor/stager/ReqIdRequestCInt.cpp
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
#include "castor/stager/ReqId.hpp"
#include "castor/stager/ReqIdRequest.hpp"
#include "castor/stager/Request.hpp"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_delete(castor::stager::ReqIdRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_ReqIdRequest_getRequest(castor::stager::ReqIdRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_ReqIdRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::ReqIdRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_print
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_print(castor::stager::ReqIdRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_addReqids
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_addReqids(castor::stager::ReqIdRequest* instance, castor::stager::ReqId* obj) {
    instance->addReqids(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_removeReqids
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_removeReqids(castor::stager::ReqIdRequest* instance, castor::stager::ReqId* obj) {
    instance->removeReqids(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_ReqIdRequest_reqids
  //----------------------------------------------------------------------------
  int Cstager_ReqIdRequest_reqids(castor::stager::ReqIdRequest* instance, castor::stager::ReqId*** var, int* len) {
    std::vector<castor::stager::ReqId*> result = instance->reqids();
    *len = result.size();
    *var = (castor::stager::ReqId**) malloc((*len) * sizeof(castor::stager::ReqId*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
