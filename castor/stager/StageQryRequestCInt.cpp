/******************************************************************************
 *                      castor/stager/StageQryRequestCInt.cpp
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
 * @(#)$RCSfile: StageQryRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageQryRequest.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_create(castor::stager::StageQryRequest** obj) {
    *obj = new castor::stager::StageQryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_delete(castor::stager::StageQryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageQryRequest_getRequest(castor::stager::StageQryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageQryRequest* Cstager_StageQryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageQryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_print(castor::stager::StageQryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageQryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_setId(castor::stager::StageQryRequest* instance,
                                    unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_id(castor::stager::StageQryRequest* instance,
                                 unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageQryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageQryRequest_type(castor::stager::StageQryRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
