/******************************************************************************
 *                      castor/stager/QryRequestCInt.cpp
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
#include "castor/stager/QryRequest.hpp"
#include "castor/stager/Request.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_delete(castor::stager::QryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_QryRequest_getRequest(castor::stager::QryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::QryRequest* Cstager_QryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::QryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_QryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_QryRequest_print(castor::stager::QryRequest* instance) {
    instance->print();
    return 0;
  }

} // End of extern "C"
