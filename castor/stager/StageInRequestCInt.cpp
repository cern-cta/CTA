/******************************************************************************
 *                      castor/stager/StageInRequestCInt.cpp
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
 * @(#)$RCSfile: StageInRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageInRequest.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_create(castor::stager::StageInRequest** obj) {
    *obj = new castor::stager::StageInRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_delete(castor::stager::StageInRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageInRequest_getRequest(castor::stager::StageInRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageInRequest* Cstager_StageInRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageInRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_print(castor::stager::StageInRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_TYPE(int* ret) {
    *ret = castor::stager::StageInRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_setId(castor::stager::StageInRequest* instance,
                                   unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_id(castor::stager::StageInRequest* instance,
                                unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_type(castor::stager::StageInRequest* instance,
                                  int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_openflags
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_openflags(castor::stager::StageInRequest* instance, int* var) {
    *var = instance->openflags();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageInRequest_setOpenflags
  //----------------------------------------------------------------------------
  int Cstager_StageInRequest_setOpenflags(castor::stager::StageInRequest* instance, int new_var) {
    instance->setOpenflags(new_var);
    return 0;
  }

} // End of extern "C"
