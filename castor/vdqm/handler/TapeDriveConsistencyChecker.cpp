/******************************************************************************
 *                      TapeDriveConsistencyChecker.cpp
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
 * @(#)RCSfile: TapeDriveConsistencyChecker.cpp  Revision: 1.0  Release Date: Jul 4, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/ClientIdentification.hpp"

#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
 
// Local Includes
#include "BaseRequestHandler.hpp"
#include "TapeDriveConsistencyChecker.hpp"


//To make the code more readable
using namespace castor::vdqm;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveConsistencyChecker::TapeDriveConsistencyChecker(
	TapeDrive* tapeDrive, 
	vdqmDrvReq_t* driveRequest, 
	Cuuid_t cuuid) throw() {
		
	m_cuuid = cuuid;
	
	if ( tapeDrive == NULL || driveRequest == NULL ) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "One of the arguments is NULL";
  	throw ex;
	}
	else {
		ptr_tapeDrive = tapeDrive;
		ptr_driveRequest = driveRequest;
	}
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveConsistencyChecker::~TapeDriveConsistencyChecker() 
	throw() {

}


//------------------------------------------------------------------------------
// checkConsistency
//------------------------------------------------------------------------------
bool castor::vdqm::handler::TapeDriveConsistencyChecker::checkConsistency() 
	throw (castor::exception::Exception) {
	return true;		
}
