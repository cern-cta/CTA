/******************************************************************************
 *                      castor/stager/StageOutRequestCInt.cpp
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
 * @(#)$RCSfile: StageOutRequestCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageOutRequest.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_create(castor::stager::StageOutRequest** obj) {
    *obj = new castor::stager::StageOutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_delete(castor::stager::StageOutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageOutRequest_getRequest(castor::stager::StageOutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageOutRequest* Cstager_StageOutRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageOutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_print(castor::stager::StageOutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_TYPE(int* ret) {
    *ret = castor::stager::StageOutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_setId(castor::stager::StageOutRequest* instance,
                                    unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_id(castor::stager::StageOutRequest* instance,
                                 unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_type(castor::stager::StageOutRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_openmode
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_openmode(castor::stager::StageOutRequest* instance, int* var) {
    *var = instance->openmode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageOutRequest_setOpenmode
  //----------------------------------------------------------------------------
  int Cstager_StageOutRequest_setOpenmode(castor::stager::StageOutRequest* instance, int new_var) {
    instance->setOpenmode(new_var);
    return 0;
  }

} // End of extern "C"
