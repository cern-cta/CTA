/******************************************************************************
 *                      castor/stager/StageFilChgRequestCInt.cpp
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
 * @(#)$RCSfile: StageFilChgRequestCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/05 13:37:33 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageFilChgRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_create(castor::stager::StageFilChgRequest** obj) {
    *obj = new castor::stager::StageFilChgRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_delete(castor::stager::StageFilChgRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageFilChgRequest_getRequest(castor::stager::StageFilChgRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFilChgRequest* Cstager_StageFilChgRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageFilChgRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_print(castor::stager::StageFilChgRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFilChgRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_setId(castor::stager::StageFilChgRequest* instance,
                                       u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_id(castor::stager::StageFilChgRequest* instance,
                                    u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFilChgRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFilChgRequest_type(castor::stager::StageFilChgRequest* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
