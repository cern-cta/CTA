/******************************************************************************
 *                      BaseRequestHandler.cpp
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
 * @(#)RCSfile: BaseRequestHandler.cpp  Revision: 1.0  Release Date: Apr 29, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <string>
#include <vector>
//#include "occi.h"

#include "castor/IObject.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"

#include "castor/exception/Internal.hpp"

#include "castor/vdqm/ErrorHistory.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDrive.hpp"

#include "castor/stager/Tape.hpp"
 
//Local Includes
#include "BaseRequestHandler.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::BaseRequestHandler::BaseRequestHandler() 
	throw(castor::exception::Exception) {

  castor::IService* svc = NULL;

  /**
   * The IVdqmService Objects has some important fuctions
   * to handle db queries.
   */
  
  ptr_svcs = services();
	
	try {
		/**
		 * Getting DbVdqmSvc: It can be the OraVdqmSvc or the MyVdqmSvc
		 */
		svc = ptr_svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
	  if (0 == svc) {
	    castor::exception::Internal ex;
	    ex.getMessage() << "Could not get DbVdqmSvc" << std::endl;
	    
	    throw ex;
	  }
//	} catch (oracle::occi::SQLException e) {
//    castor::exception::Internal ex;
//    
//    ex.getMessage() << "Error in castor::vdqm::handler::BaseRequestHandler::BaseRequestHandler(): "
//                    << std::endl << e.what() << std::endl;
//    throw ex;
  }
	catch (...) {
		castor::exception::Internal ex;
		
		ex.getMessage() << "Error in castor::vdqm::handler::BaseRequestHandler::BaseRequestHandler(): "
                    << std::endl;
    throw ex;
	} 
  
  ptr_IVdqmService = dynamic_cast<IVdqmSvc*>(svc);
  if (0 == ptr_IVdqmService) {
    castor::exception::Internal ex;
    ex.getMessage() << "Got a bad DbVdqmSvc: "
    								<< "ID=" << svc->id()
    								<< ", Name=" << svc->name()
    								<< std::endl;

    throw ex;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::BaseRequestHandler::~BaseRequestHandler() 
	throw() {
	ptr_IVdqmService->release();
	
	//Reset the pointer
	ptr_svcs = 0;
	ptr_IVdqmService = 0;
}


//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::BaseRequestHandler::handleRequest
(castor::IObject* fr, Cuuid_t cuuid)
  throw (castor::exception::Exception) {
  
  // Stores it into the data base
  castor::BaseAddress ad;
  
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  
  
  try {
  	//Create a new entry in the table
    svcs()->createRep(&ad, fr, false);
    
    // Store files for TapeRequests
    castor::vdqm::TapeRequest *tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*>(fr);
    
    if (0 != tapeRequest) {
    	svcs()->createRep(&ad, (IObject *)tapeRequest->client(), false);
      svcs()->fillRep(&ad, fr, OBJ_ClientIdentification, false);
      
      svcs()->fillRep(&ad, fr, OBJ_Tape, false);
      svcs()->fillRep(&ad, fr, OBJ_DeviceGroupName, false);
      svcs()->fillRep(&ad, fr, OBJ_TapeAccessSpecification, false);
      svcs()->fillRep(&ad, fr, OBJ_TapeDrive, false);
    	svcs()->fillRep(&ad, fr, OBJ_TapeServer, false);      
    }
    
    // Store files for TapeDrive
    castor::vdqm::TapeDrive *tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*>(fr);
    
    if (0 != tapeDrive) {
      svcs()->updateRep(&ad, (IObject *)tapeDrive->tapeServer(), false);    	
      
      svcs()->fillRep(&ad, fr, OBJ_DeviceGroupName, false);
      
      if ( 0 != tapeDrive->tape() )
	      svcs()->fillRep(&ad, fr, OBJ_Tape, false);
  
    	svcs()->fillRep(&ad, fr, OBJ_TapeServer, false);      
    	
    	if ( tapeDrive->tapeDriveCompatibilities().size() != 0 ) 
	    	svcs()->fillRep(&ad, fr, OBJ_TapeDriveCompatibility, false);          	
    }
    
    // Store files for ErrorHistory
    castor::vdqm::ErrorHistory* errorHistory =
      dynamic_cast<castor::vdqm::ErrorHistory*>(fr);
    
    if (0 != errorHistory) {
      svcs()->fillRep(&ad, fr, OBJ_Tape, false);
      svcs()->fillRep(&ad, fr, OBJ_TapeDrive, false);
    }    
		
//	    svcs()->commit(&ad);
//	    // "Request stored in DB" message
//	    castor::dlf::Param params[] =
//	      {castor::dlf::Param("ID", fr->id())};
//	    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 12, 1, params);
  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    
    castor::vdqm::TapeRequest *tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*>(fr);
    
    if ( 0 != tapeRequest ) {
	  	castor::exception::Exception ex(EVQNOVOL);
	  	ex.getMessage() << e.getMessage().str();
	  									
	  	tapeRequest = 0;

	  	throw ex;
    }
    
    castor::vdqm::TapeDrive* tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*>(fr);
      
    if ( 0 != tapeDrive ) {
	  	castor::exception::Exception ex(EVQNODRV);
	  	ex.getMessage() << e.getMessage().str();					
	  	
	  	tapeDrive = 0;
	  	tapeRequest = 0;
	  		
	  	throw ex;    	
  	}
  	else {
  		// If we are here, we have an exception during a handling of ErrorHistory
  		tapeDrive = 0;
  		tapeRequest = 0;
  		
  		throw e;
  	}
  }
}


//------------------------------------------------------------------------------
// deleteRepresentation
//------------------------------------------------------------------------------
void castor::vdqm::handler::BaseRequestHandler::deleteRepresentation
(castor::IObject* fr, Cuuid_t cuuid)
  throw (castor::exception::Exception) {
  // Deletes it from the data base
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
  	// Deletes the table entry
    svcs()->deleteRep(&ad, fr, false);

//    // "Request deleted from DB" message
//    castor::dlf::Param params[] =
//      {castor::dlf::Param("ID", fr->id())};
//    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 29, 1, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    
    castor::exception::Exception ex(EVQREPLICA);
  	ex.getMessage() << e.getMessage().str();	
  	throw ex;
  }
} // End of deleteRepresentation


//------------------------------------------------------------------------------
// updateRepresentation
//------------------------------------------------------------------------------
void castor::vdqm::handler::BaseRequestHandler::updateRepresentation
(castor::IObject* fr, Cuuid_t cuuid)
  throw (castor::exception::Exception) {

  // Stores it into the data base
  castor::BaseAddress ad;
  
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
  	//Update entry in the table
    svcs()->updateRep(&ad, fr, false);
    
    // Update files for TapeRequest
    castor::vdqm::TapeRequest *tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*>(fr);    
    if (0 != tapeRequest) {
//      svcs()->updateRep(&ad, (IObject *)tapeRequest->tapeDrive(), false);
      svcs()->fillRep(&ad, fr, OBJ_TapeDrive, false);
    }
    
    
    // Update files for TapeDrive
    castor::vdqm::TapeDrive *tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*>(fr);
    
    if (0 != tapeDrive) {      
      if (0 != tapeDrive->runningTapeReq() )  {
	      svcs()->updateRep(&ad, (IObject *)tapeDrive->runningTapeReq(), false);
      }
      svcs()->fillRep(&ad, fr, OBJ_TapeRequest, false);
      
//      svcs()->fillRep(&ad, fr, OBJ_DeviceGroupName, false);


	   	svcs()->fillRep(&ad, fr, OBJ_Tape, false);

  
      svcs()->updateRep(&ad, (IObject *)tapeDrive->tapeServer(), false);    	
    	svcs()->fillRep(&ad, fr, OBJ_TapeServer, false);      

    	//TODO: Enable Link
//    	svcs()->fillRep(&ad, fr, OBJ_TapeDriveCompatibility, false);          	            
    }

//	    svcs()->commit(&ad);
//	    
//	    // "Update of representation in DB" message
//	    castor::dlf::Param params[] =
//	      {castor::dlf::Param("ID", fr->id())};
//	    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 45, 1, params);

  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    
    castor::vdqm::TapeRequest *tapeRequest =
      dynamic_cast<castor::vdqm::TapeRequest*>(fr);
    
    if ( 0 != tapeRequest ) {
	  	castor::exception::Exception ex(EVQNOVOL);
	  	ex.getMessage() << e.getMessage().str();
	  									
	  	tapeRequest = 0;

	  	throw ex;
    }
    
    castor::vdqm::TapeDrive* tapeDrive =
      dynamic_cast<castor::vdqm::TapeDrive*>(fr);
      
    if ( 0 != tapeDrive ) {
	  	castor::exception::Exception ex(EVQNODRV);
	  	ex.getMessage() << e.getMessage().str();					
	  	
	  	tapeDrive = 0;
	  	tapeRequest = 0;
	  		
	  	throw ex;    	
  	}
  	else {
  		// If we are here, we have an exception during a handling of ErrorHistory
  		tapeDrive = 0;
  		tapeRequest = 0;
  		
  		throw e;
  	}
  }  	
}
