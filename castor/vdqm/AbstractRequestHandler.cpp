/******************************************************************************
 *                      AbstractRequestHandler.cpp
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
 * @(#)RCSfile: AbstractRequestHandler.cpp  Revision: 1.0  Release Date: Apr 29, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
 

#include "castor/IObject.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
 
//Local Includes
#include "AbstractRequestHandler.hpp"
#include "TapeRequest.hpp"
#include "TapeDrive.hpp"


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::AbstractRequestHandler::AbstractRequestHandler() {}


//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::vdqm::AbstractRequestHandler::handleRequest
(castor::IObject* fr, Cuuid_t cuuid)
  throw (castor::exception::Exception) {
  // Stores it into Oracle
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
  try {
    svcs()->createRep(&ad, fr, false);
    
    // Store files for taoe requests
    castor::vdqm::TapeRequest *tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*>(fr);
    
    if (0 != tapeRequest) {
      svcs()->fillRep(&ad, fr, OBJ_TapeRequest, false);
    }
    
    castor::vdqm::TapeDrive *tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*>(fr);
    
    if (0 != tapeDrive) {
      svcs()->fillRep(&ad, fr, OBJ_TapeDrive, false);
    }

    svcs()->commit(&ad);
    // "Request stored in DB" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("ID", fr->id())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 12, 1, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    throw e;
  }

  // TODO: Send an UDP message to the vdqm client
}
