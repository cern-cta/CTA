/******************************************************************************
 *                      castor/stager/FileRequestCInt.cpp
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
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_FileRequest_delete(castor::stager::FileRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_FileRequest_getRequest(castor::stager::FileRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::FileRequest* Cstager_FileRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::FileRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_print
  //----------------------------------------------------------------------------
  int Cstager_FileRequest_print(castor::stager::FileRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_addSubRequests
  //----------------------------------------------------------------------------
  int Cstager_FileRequest_addSubRequests(castor::stager::FileRequest* instance, castor::stager::SubRequest* obj) {
    instance->addSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_removeSubRequests
  //----------------------------------------------------------------------------
  int Cstager_FileRequest_removeSubRequests(castor::stager::FileRequest* instance, castor::stager::SubRequest* obj) {
    instance->removeSubRequests(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileRequest_subRequests
  //----------------------------------------------------------------------------
  int Cstager_FileRequest_subRequests(castor::stager::FileRequest* instance, castor::stager::SubRequest*** var, int* len) {
    std::vector<castor::stager::SubRequest*> result = instance->subRequests();
    *len = result.size();
    *var = (castor::stager::SubRequest**) malloc((*len) * sizeof(castor::stager::SubRequest*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

} // End of extern "C"
