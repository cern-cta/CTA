/******************************************************************************
 *                      castor/stager/StageClrRequestCInt.cpp
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
 * @(#)$RCSfile: StageClrRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:24 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageClrRequest.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_create(castor::stager::StageClrRequest** obj) {
    *obj = new castor::stager::StageClrRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_delete(castor::stager::StageClrRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageClrRequest_getRequest(castor::stager::StageClrRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageClrRequest* Cstager_StageClrRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageClrRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_print(castor::stager::StageClrRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_TYPE(int* ret) {
    *ret = castor::stager::StageClrRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_setId(castor::stager::StageClrRequest* instance,
                                    unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_id(castor::stager::StageClrRequest* instance,
                                 unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageClrRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageClrRequest_type(castor::stager::StageClrRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
