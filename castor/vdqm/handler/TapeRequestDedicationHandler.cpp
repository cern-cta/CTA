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
#include <string>
#include "Cpool_api.h"

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
	castor::vdqm::handler::TapeRequestDedicationHandler::Instance(
	int dedicationThreadNumber) 
	throw (castor::exception::Exception) {
	
	if ( _instance == 0 ) {
		_instance = new TapeRequestDedicationHandler(dedicationThreadNumber);
	}
	
	return _instance;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestDedicationHandler::TapeRequestDedicationHandler(
	int dedicationThreadNumber)
	throw (castor::exception::Exception): 
	m_threadPoolId(-1),
  m_dedicationThreadNumber(DEFAULT_THREAD_NUMBER) {
	
	if ( dedicationThreadNumber > 0 )
		m_dedicationThreadNumber = dedicationThreadNumber;
		
	createThreadPool();
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestDedicationHandler::~TapeRequestDedicationHandler() throw() {
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
		    
		    castor::BaseAddress ad;
		   	ad.setCnvSvcName("DbCnvSvc");
				ad.setCnvSvcType(castor::SVC_DBCNV);
				svcs()->rollback(&ad);
				
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
	    
	    try {  
		  	freeMemory(freeTapeDrive, waitingTapeRequest);		  	
	    } catch (castor::exception::Exception e) {
		    // "Exception caught in TapeRequestDedicationHandler::run()" message
		    castor::dlf::Param param[] =
		      {castor::dlf::Param("Message", e.getMessage().str())};      
		    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 50, 1, param);			
			}
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
	updateRepresentation(freeTapeDrive, nullCuuid);
	updateRepresentation(waitingTapeRequest, nullCuuid);	
	
	// Needed for the commit/rollback
	castor::BaseAddress ad;
	ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV);
	
	/**
	 * we do a commit to the db
	 */
	svcs()->commit(&ad);

	// "Update of representation in DB" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("ID tapeDrive", freeTapeDrive->id()),
     castor::dlf::Param("ID tapeRequest", waitingTapeRequest->id()),
     castor::dlf::Param("tapeDrive status", "UNIT_STARTING")};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 45, 3, params);	

  
 	/**
	 * and we send the information to the RTCPD
	 */
	threadAssign(freeTapeDrive);
}


//------------------------------------------------------------------------------
// createThreadPool
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::createThreadPool() 
	throw (castor::exception::Exception) {

  // create threads if in multithreaded mode
  int dedicateThreads, actualNbThreads;
  dedicateThreads = m_dedicationThreadNumber;
  
  m_threadPoolId = Cpool_create(dedicateThreads, &actualNbThreads);
  if (m_threadPoolId < 0) {
		castor::exception::Internal ex;
		ex.getMessage() << "TapeRequestDedicationHandler::createThreadPool(): "
										<< "Dedication Thread pool creation error: "
										<< m_threadPoolId
										<< std::endl;

    throw ex;
  } else {
  	// "Dedication thread pool created" message
		castor::dlf::Param params[] =
			{castor::dlf::Param("threadPoolId", m_threadPoolId),
			castor::dlf::Param("actualNbThreads", actualNbThreads)};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 62, 2, params);

    m_dedicationThreadNumber = actualNbThreads;
  }
}


//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
int castor::vdqm::handler::TapeRequestDedicationHandler::threadAssign(
	void *param) {

  // Initializing the arguments to pass to the static request processor
  struct dedicationRequestArgs *args = new dedicationRequestArgs();
  args->handler = this;
  args->param = param;

  
  int assign_rc = Cpool_assign(m_threadPoolId,
                               &castor::vdqm::handler::staticDedicationRequest,
                               args,
                               -1);
  if (assign_rc < 0) {
  	// "Error while assigning request to pool" message
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 60);

    return -1;
  }
  
  return 0;
}


//------------------------------------------------------------------------------
// staticDedicationRequest
//------------------------------------------------------------------------------
void *castor::vdqm::handler::staticDedicationRequest(void *param) {
  castor::vdqm::handler::TapeRequestDedicationHandler *dedicationHandler = 0;
  struct dedicationRequestArgs *args;

  if (param == NULL) {
    return 0;
  }

  // Recovering the dedicationRequestArgs
  args = (struct dedicationRequestArgs *)param;

  if (args->handler == NULL) {
    delete args;
    return 0;
  }

  dedicationHandler = 
  	dynamic_cast<castor::vdqm::handler::TapeRequestDedicationHandler *>
    (args->handler);
    
  if (dedicationHandler == 0) {
    delete args;
    return 0;
  }

  // Executing the method
  void *res = dedicationHandler->dedicationRequest(args->param);
  delete args;
  return res;
}


//------------------------------------------------------------------------------
// dedicationRequest
//------------------------------------------------------------------------------
void *castor::vdqm::handler::TapeRequestDedicationHandler::dedicationRequest(
	void *param) throw() {
  
  castor::IObject* object = 
  	(castor::IObject*) param;
  
  castor::vdqm::TapeDrive* freeTapeDrive = 
  	dynamic_cast<castor::vdqm::TapeDrive*>(object);
  	
  object = 0;
  	
  if (0 != freeTapeDrive) {
		
		try {
			RTCopyDConnection rtcpConnection(RTCOPY_PORT, 
																			 freeTapeDrive->tapeServer()->serverName());																	 
			rtcpConnection.connect();
			
			if ( !rtcpConnection.sendJobToRTCPD(freeTapeDrive) ) {
				rollback(freeTapeDrive);
			}
		} catch (castor::exception::Exception e) {
	    // "Exception caught in TapeRequestDedicationHandler::dedicationRequest()" message
	    castor::dlf::Param param[] =
	      {castor::dlf::Param("Message", e.getMessage().str())};      
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 63, 1, param);
			
			try {
				rollback(freeTapeDrive);
			} catch (castor::exception::Exception ex) {
		    // "Exception caught in TapeRequestDedicationHandler::dedicationRequest()" message
		    castor::dlf::Param param[] =
		      {castor::dlf::Param("Message", ex.getMessage().str())};      
		    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 63, 1, param);			
			}
		}
		
		
		
	  /**
	   * Deletion of the objects
	   */   
		castor::vdqm::TapeRequest* waitingTapeRequest = 
			freeTapeDrive->runningTapeReq();	 
		try {  
	  	freeMemory(freeTapeDrive, waitingTapeRequest);		  	
		} catch (castor::exception::Exception ex) {
	    // "Exception caught in TapeRequestDedicationHandler::dedicationRequest()" message
	    castor::dlf::Param param[] =
	      {castor::dlf::Param("Message", ex.getMessage().str())};      
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 63, 1, param);			
		}
  }
  else {
  	// "No TapeDrive object to commit to RTCPD" message
  	castor::dlf::Param param[] =
     {castor::dlf::Param("function", 
     	"castor::vdqm::handler::TapeRequestDedicationHandler::dedicationRequest")};
  	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 64, 1, param);
  }
  
  return 0; 
}


//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::rollback(
	castor::vdqm::TapeDrive* freeTapeDrive)
	throw (castor::exception::Exception) {
		
	// Needed for the commit
	castor::BaseAddress ad;
	ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV);	
		
	//Rollback all changes!
	freeTapeDrive->setStatus(UNIT_UP);
	freeTapeDrive->setModificationTime((int)time(NULL));
	
	castor::vdqm::TapeRequest* waitingTapeRequest 
		= freeTapeDrive->runningTapeReq();
	waitingTapeRequest->setTapeDrive(0);
	waitingTapeRequest->setModificationTime((int)time(NULL));
	
	freeTapeDrive->setRunningTapeReq(0);
	
	updateRepresentation(freeTapeDrive, nullCuuid);	
	/**
	 * We commit now just the modification time. This is necessary to don't get
	 * always the same tapeRequest out of the db (.. We get always the oldest 
	 * fitting tapeRequest).
	 */ 
	updateRepresentation(waitingTapeRequest, nullCuuid);
	
	// Commit the rollback
	svcs()->commit(&ad);
	
	// "Update of representation in DB" message
	castor::dlf::Param params[] =
	  {castor::dlf::Param("ID tapeDrive", freeTapeDrive->id()),
	   castor::dlf::Param("ID tapeRequest", waitingTapeRequest->id()),
	   castor::dlf::Param("tapeDrive status", "UNIT_UP")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 45, 3, params);				
}


//------------------------------------------------------------------------------
// freeMemory
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestDedicationHandler::freeMemory(
	castor::vdqm::TapeDrive* freeTapeDrive, 
	castor::vdqm::TapeRequest* waitingTapeRequest) 
	throw (castor::exception::Exception) {
	   
  if ( waitingTapeRequest ) {
  	if ( waitingTapeRequest->tapeDrive() &&
				 waitingTapeRequest->tapeDrive() != freeTapeDrive ) {
  		delete waitingTapeRequest->tapeDrive();
  		waitingTapeRequest->setTapeDrive(0);
		}
  	
  	delete waitingTapeRequest;
  	waitingTapeRequest = 0; 	  	
  }
  		
  if ( freeTapeDrive ) {
  	if ( freeTapeDrive->tape() ) {
	  	delete freeTapeDrive->tape();
	  	freeTapeDrive->setTape(0);
  	}
	  if ( freeTapeDrive->runningTapeReq() ) {
	  	delete freeTapeDrive->runningTapeReq();
	  	freeTapeDrive->setRunningTapeReq(0);
	  }
	  
	  delete freeTapeDrive->deviceGroupName();
	  freeTapeDrive->setDeviceGroupName(0);
	  
		//The TapeServer must be deleted after its tapeDrive!
  	castor::vdqm::TapeServer* tapeServer = freeTapeDrive->tapeServer();
	  
  	delete freeTapeDrive;
  	freeTapeDrive = 0;
  	
  	delete tapeServer;
  	tapeServer = 0;
  }		
}
