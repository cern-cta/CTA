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
 
//#include <iostream>
 
// Local include
#include "TapeRequestDedicationHandler.hpp"


castor::vdqm::handler::TapeRequestDedicationHandler* 
 	castor::vdqm::handler::TapeRequestDedicationHandler::_instance = 0;
 	
unsigned const int
	castor::vdqm::handler::TapeRequestDedicationHandler::m_sleepMilliTime = 500000;
 	
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
void castor::vdqm::handler::TapeRequestDedicationHandler::run() 
	throw(castor::exception::Exception) {
		
	_stopLoop = false;
	
	while ( !_stopLoop ) {
		//TODO: Algorithmus schreiben.
		
	//	/**
	//	 * Look for a free tape drive, which can handle the request
	//	 */
	//	freeTapeDrive = ptr_IVdqmService->getFreeTapeDrive(reqExtDevGrp);
	//	if ( freeTapeDrive == NULL ) {
	//	  castor::exception::Internal ex;
	//	  ex.getMessage() << "No free tape drive for TapeRequest "
	//	  								<< "with ExtendedDeviceGroup " 
	//	  								<< reqExtDevGrp->dgName()
	//	  								<< " and mode = "
	//	  								<< reqExtDevGrp->mode()
	//	  								<< std::endl;
	//	  throw ex;
	//	}
	//  else { //If there was a free drive, start a new job
	//	  handleTapeRequestQueue();
	//  		
		
//		std::cout << "TapeRequestDedicationHandler loop" << std::endl;
		usleep(m_sleepMilliTime);
	}
}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::stop() {
	_stopLoop = true;
}
