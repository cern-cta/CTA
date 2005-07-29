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
#include "castor/exception/Internal.hpp"
 
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
 
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
void castor::vdqm::handler::TapeRequestDedicationHandler::run() {
		
  TapeDrive *freeTapeDrive;
		
	_stopLoop = false;
	
	while ( !_stopLoop ) {
		//TODO: Algorithmus schreiben.
		try {
			/**
			 * Look for a free tape drive, which can handle the request
			 */
//			freeTapeDrive = ptr_IVdqmService->getFreeTapeDrive(reqExtDevGrp);
			if ( freeTapeDrive == NULL ) {
		    // "No free TapeDrive, or no TapeRequest in the db" message
		   // castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 46);
			}
		  else { //If there was a free drive, start a new job
			  handleTapeRequestQueue();
		  }
		   
		} catch (castor::exception::Exception ex) {
	    // "Exception caught" message
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Message", ex.getMessage().str())};      
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
		}
	  		
		
//		std::cout << "TapeRequestDedicationHandler loop" << std::endl;
		usleep(m_sleepMilliTime);
		freeTapeDrive = NULL;
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
void castor::vdqm::handler::TapeRequestDedicationHandler::handleTapeRequestQueue() 
	throw (castor::exception::Exception) {
	//TODO: Implementation
//  /* 
//   * Loop until either the volume queue is empty or
//   * there are no more suitable drives
//   */
//  for (;;) {
//      rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
//      if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
//          log(LOG_ERR,"vdqm_NewVolReq(): SelectVolAndDrv() returned rc=%d\n",
//              rc);
//          break;
//      }
//      /*
//       * Free memory allocated for previous request for this drive
//       */
//      if ( drvrec->vol != NULL ) free(drvrec->vol);
//      drvrec->vol = volrec;
//      volrec->drv = drvrec;
//      drvrec->drv.VolReqID = volrec->vol.VolReqID;
//      volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
//      drvrec->drv.jobID = 0;
//      /*
//       * Reset the drive status
//       */
//      drvrec->drv.status = drvrec->drv.status & 
//          ~(VDQM_UNIT_RELEASE | VDQM_UNIT_BUSY | VDQM_VOL_MOUNT |
//          VDQM_VOL_UNMOUNT | VDQM_UNIT_ASSIGN);
//      drvrec->drv.recvtime = (int)time(NULL);
//  
//      /*
//       * Start the job
//       */
//      rc = vdqm_StartJob(volrec);
//      if ( rc < 0 ) {
//          log(LOG_ERR,"vdqm_NewVolReq(): vdqm_StartJob() returned error\n");
//          volrec->vol.DrvReqID = 0;
//          drvrec->drv.VolReqID = 0;
//          volrec->drv = NULL;
//          drvrec->vol = NULL;
//          break;
//      }
//   } /* end of for (;;) */
}

