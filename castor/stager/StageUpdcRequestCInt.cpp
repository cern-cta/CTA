/******************************************************************************
 *                      castor/stager/StageUpdcRequestCInt.cpp
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
 * @(#)$RCSfile: StageUpdcRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:26 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/ReqIdRequest.hpp"
#include "castor/stager/StageUpdcRequest.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_create(castor::stager::StageUpdcRequest** obj) {
    *obj = new castor::stager::StageUpdcRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_delete(castor::stager::StageUpdcRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_getReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::ReqIdRequest* Cstager_StageUpdcRequest_getReqIdRequest(castor::stager::StageUpdcRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_fromReqIdRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdcRequest* Cstager_StageUpdcRequest_fromReqIdRequest(castor::stager::ReqIdRequest* obj) {
    return dynamic_cast<castor::stager::StageUpdcRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_print(castor::stager::StageUpdcRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdcRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_setId(castor::stager::StageUpdcRequest* instance,
                                     unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_id(castor::stager::StageUpdcRequest* instance,
                                  unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdcRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdcRequest_type(castor::stager::StageUpdcRequest* instance,
                                    int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
