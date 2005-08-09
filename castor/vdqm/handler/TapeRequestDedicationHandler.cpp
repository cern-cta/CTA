/******************************************************************************
 *                      TapeRequestDedicationHandler.cpp
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
 * @(#)RCSfile: TapeRequestDedicationHandler.cpp  Revision: 1.0  Release Date: Jul 20, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <unistd.h>
#include <rtcp_constants.h> /* RTCOPY_PORT  */

#include "castor/exception/Internal.hpp"

#include "castor/BaseAddress.hpp"
#include "castor/Services.hpp"
 
#include "castor/stager/Tape.hpp"
 
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/RTCopyDConnection.hpp"


// Local include
#include "TapeRequestDedicationHandler.hpp"


/**
 * The initial state of the Singleton instance
 */
castor::vdqm::handler::TapeRequestDedicationHandler* 
 	castor::vdqm::handler::TapeRequestDedicationHandler::_instance = 0;
 	
/**
 * This time specifies, in which time intervall the main loop should
 * run.
 */
unsigned const int
	castor::vdqm::handler::TapeRequestDedicationHandler::m_sleepTime = 2;
 	
 	
//------------------------------------------------------------------------------
// Instance
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestDedicationHandler*
	castor::vdqm::handler::TapeRequestDedicationHandler::Instance() 
	throw(castor::exception::Exception) {
	
	if ( _instance == 0 ) {
		_instance = new TapeRequestDedicationHandler;
	}
	
	return _instance;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestDedicationHandler::TapeRequestDedicationHandler() 
	throw(castor::exception::Exception) {
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestDedicationHandler::~TapeRequestDedicationHandler() throw() {
	delete _instance;
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::run() {
		
  TapeDrive* freeTapeDrive;
  TapeRequest* waitingTapeRequest;	
		
	_stopLoop = false;
	
	while ( !_stopLoop ) {
		//TODO: Algorithmus schreiben.
		freeTapeDrive = NULL;
		waitingTapeRequest = NULL;
		
		try {
			/**
			 * Look for a free tape drive, which can handle the request
			 */
			ptr_IVdqmService->matchTape2TapeDrive(&freeTapeDrive, &waitingTapeRequest);
			
			if ( freeTapeDrive == NULL || waitingTapeRequest == NULL) {
		    // "No free TapeDrive, or no TapeRequest in the db" message
//		    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 46);
		  	sleep(m_sleepTime);
			}
		  else { //If there was a free drive, start a new job
			  handleDedication(freeTapeDrive, waitingTapeRequest);
		  }
		} catch (castor::exception::Exception ex) {			
		  castor::BaseAddress ad;
  
		  ad.setCnvSvcName("DbCnvSvc");
		  ad.setCnvSvcType(castor::SVC_DBCNV);
			svcs()->rollback(&ad);
			
	    // "Exception caught in TapeRequestDedicationHandler::run()" message
	    castor::dlf::Param param[] =
	      {castor::dlf::Param("Message", ex.getMessage().str())};      
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 50, 1, param);
		}
	  	
	  if ( waitingTapeRequest ) {
	  	if ( waitingTapeRequest->tapeDrive() &&
  				 waitingTapeRequest->tapeDrive() != freeTapeDrive )
	  		delete waitingTapeRequest->tapeDrive();
	  	
	  	delete waitingTapeRequest; 	  	
	  }
	  		
	  if ( freeTapeDrive ) {
	  	if ( freeTapeDrive->tape() )
		  	delete freeTapeDrive->tape();
		  if ( freeTapeDrive->runningTapeReq() )
		  	delete freeTapeDrive->runningTapeReq();

	  	delete freeTapeDrive->tapeServer();
		  delete freeTapeDrive->deviceGroupName();	  	
	  	delete freeTapeDrive;
	  }
		
//		std::cout << "TapeRequestDedicationHandler loop" << std::endl;
		
	}
}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::stop() {
	_stopLoop = true;
}


//------------------------------------------------------------------------------
// handleTapeRequestQueue
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::handleDedication(
	castor::vdqm::TapeDrive* freeTapeDrive, 
	castor::vdqm::TapeRequest* waitingTapeRequest) 
	throw (castor::exception::Exception) {
		
	if ( freeTapeDrive->status() != UNIT_UP || 
			 freeTapeDrive->runningTapeReq() != NULL ||
			 freeTapeDrive->tape() != NULL ) {
		castor::exception::Internal ex;
		
		ex.getMessage() << "The selected TapeDrive from the db is not free or "
										<< "has still a running job!: id = "
										<< freeTapeDrive->id() << std::endl;
										
		throw ex;
	}
	
	if ( waitingTapeRequest->tapeDrive() != NULL  &&
			 waitingTapeRequest->tapeDrive()->id() != freeTapeDrive->id()) {
		castor::exception::Internal ex;
		
		ex.getMessage() << "The selected TapeRequest seems to be handled already "
										<< "by another tapeDrive!: tapeRequestID = "
										<< waitingTapeRequest->id()
										<< std::endl;
										
		throw ex;
	}
		
		
	/**
	 * Updating the informations for the data base
	 */
	freeTapeDrive->setStatus(UNIT_STARTING);
	freeTapeDrive->setJobID(0);
	freeTapeDrive->setModificationTime((int)time(NULL));
	
	waitingTapeRequest->setTapeDrive(freeTapeDrive);
	waitingTapeRequest->setModificationTime((int)time(NULL));
	
	freeTapeDrive->setRunningTapeReq(waitingTapeRequest);
	

	/**
	 * .. and of course we must also update the db!
	 */
	updateRepresentation(freeTapeDrive, false, nullCuuid);
	updateRepresentation(waitingTapeRequest, false, nullCuuid);	

	/**
	 * Now, we can send the information to the RTCPD
	 */
	RTCopyDConnection rtcpConnection(RTCOPY_PORT, 
																	 freeTapeDrive->tapeServer()->serverName());																	 
	rtcpConnection.connect();
	rtcpConnection.sendJobToRTCPD(freeTapeDrive);
	
	/**
	 * Now, we can really do a commit!
	 */
	castor::BaseAddress ad;
  
	ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV);
	svcs()->commit(&ad);
	
	// "Update of representation in DB" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("ID tapeDrive", freeTapeDrive->id()),
     castor::dlf::Param("ID tapeRequest", waitingTapeRequest->id())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 45, 2, params);	
}

