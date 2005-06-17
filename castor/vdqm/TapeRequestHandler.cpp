/******************************************************************************
 *                      TapeRequestHandler.cpp
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
 * @(#)RCSfile: TapeRequestHandler.cpp  Revision: 1.0  Release Date: Apr 19, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
 
#include <string> 
#include <time.h>
 
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include "castor/stager/ClientIdentification.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"

#define VDQMSERV 1

#include <net.h>
#include <vdqm.h>
#include <vdqm_constants.h>
#include "h/Ctape_constants.h"
#include <common.h> //for getconfent


//Local includes
#include "TapeRequestHandler.hpp"
#include "ExtendedDeviceGroup.hpp"
#include "TapeRequest.hpp"
#include "TapeDrive.hpp"
#include "TapeDriveStatusCodes.hpp"
#include "IVdqmSvc.hpp"
#include "TapeServer.hpp"
#include "AbstractRequestHandler.hpp"

#include "castor/exception/Internal.hpp"


 

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::TapeRequestHandler::TapeRequestHandler() throw()
{
		
	castor::IService* svc;
	
	/**
	 * Getting OraStagerSvc
	 */
	svc = ptr_svcs->service("OraStagerSvc", castor::SVC_DBSTAGERSVC);
  if (0 == svc) {
    castor::exception::Internal ex;
    ex.getMessage() << "Could not get OraStagerSvc" << std::endl;
    throw ex;
  }				
  
	ptr_IStagerService = dynamic_cast<castor::stager::IStagerSvc*>(svc);
  if (0 == ptr_IStagerService) {
    castor::exception::Internal ex;
    ex.getMessage() << "Got a bad OraStagerSvc: "
    								<< "ID=" << svc->id()
    								<< ", Name=" << svc->name()
    								<< std::endl;
    throw ex;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::TapeRequestHandler::~TapeRequestHandler() throw() {
	ptr_IStagerService->release();
	
	//reset pointer
	ptr_IStagerService = 0;
}

//------------------------------------------------------------------------------
// newTapeRequest
//------------------------------------------------------------------------------
void castor::vdqm::TapeRequestHandler::newTapeRequest(vdqmHdr_t *header, 
																									vdqmVolReq_t *volumeRequest,
																									Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
  
  //The db related informations
	TapeRequest *newTapeReq = NULL;
//  castor::vdqm::TapeDrive *freeTapeDrive;
  castor::stager::ClientIdentification *clientData = NULL;
  castor::stager::Tape *tape = NULL;
  
  //TODO: The connection is not realised, yet.
  TapeServer *reqTapeServer = NULL;
  ExtendedDeviceGroup *reqExtDevGrp = NULL;
  
  bool exist = false; 
  int rowNumber = 0; 
  char 					*p;


	if ( header == NULL || volumeRequest == NULL ) {
  	castor::exception::InvalidArgument ex;
    ex.getMessage() << "One of the arguments is NULL";
    throw ex;
  }
  
  
  //The parameters of the old vdqm VolReq Request
  castor::dlf::Param params[] =
  	{castor::dlf::Param("client_name", volumeRequest->client_name),
     castor::dlf::Param("clientUID", volumeRequest->clientUID),
     castor::dlf::Param("clientGID", volumeRequest->clientGID),
     castor::dlf::Param("client_host", volumeRequest->client_host),
     castor::dlf::Param("client_port", volumeRequest->client_port),
     castor::dlf::Param("volid", volumeRequest->volid),
     castor::dlf::Param("mode", volumeRequest->mode),
     castor::dlf::Param("dgn", volumeRequest->dgn),
     castor::dlf::Param("drive", (*volumeRequest->drive == '\0' ? "***" : volumeRequest->drive)),
     castor::dlf::Param("server", (*volumeRequest->server == '\0' ? "***" : volumeRequest->server))};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 23, 10, params);
  
  try {
	  //------------------------------------------------------------------------
	  //The Tape related informations
	  newTapeReq = new TapeRequest();
	 	newTapeReq->setCreationTime(time(NULL));
	 	/*
	   * We don't allow client to set priority
	   */
	 	newTapeReq->setPriority(VDQM_PRIORITY_NORMAL);
	 	
	 	//The client related informations
	 	clientData = new castor::stager::ClientIdentification();
	 	clientData->setMachine(volumeRequest->client_host);
	 	clientData->setUserName(volumeRequest->client_name);
	 	clientData->setPort(volumeRequest->client_port);
	 	clientData->setEuid(volumeRequest->clientUID);
	 	clientData->setEgid(volumeRequest->clientGID);
	 	clientData->setMagic(header->magic);
	 	
	 	/**
	 	 * Annotation: The side of the Tape is not necesserally needed
	 	 * by the vdqmDaemon. Normaly the RTCopy daemon should already 
	 	 * have created an entry to the Tape table. So, we just gove 0 as parameter 
	 	 * at this place.
	 	 */
	 	tape = ptr_IStagerService->selectTape(volumeRequest->volid, 
	 																				0, 
	 																				volumeRequest->mode);
	 																				
	  //The requested ExtendedDeviceGroup
	  reqExtDevGrp = new ExtendedDeviceGroup();
	  reqExtDevGrp->setDgName(volumeRequest->dgn);
	  reqExtDevGrp->setAccessMode(volumeRequest->mode);
	  
	  //The requested tape server
	//  reqTapeServer = ptr_IVdqmService->selectTapeServer(volumeRequest->server);
	  
	  /*
	   * Check that the requested device exists.
	   */
	  exist = ptr_IVdqmService->checkExtDevGroup(reqExtDevGrp);
	  
	  
	  if ( !exist ) {
	  	castor::exception::Internal ex;
	    ex.getMessage() << "DGN " <<  volumeRequest->dgn
	    								<< " does not exist" << std::endl;
	    throw ex;
	  }
	  
	  
	  //Connect the tapeRequest with the additional information
	  newTapeReq->setClient(clientData);
	  newTapeReq->setReqExtDevGrp(reqExtDevGrp);
	  newTapeReq->setRequestedSrv(reqTapeServer);
	  newTapeReq->setTape(tape);
	  newTapeReq->setId(volumeRequest->VolReqID);
	  
	
	  /*
	   * Set priority for tpwrite
	   */
	  if ( (reqExtDevGrp->accessMode() == WRITE_ENABLE) &&
	       ((p = getconfent("VDQM","WRITE_PRIORITY",0)) != NULL) ) {
	    if ( strcmp(p,"YES") == 0 ) {
	      newTapeReq->setPriority(VDQM_PRIORITY_MAX);
	    }
	  }
	  
	  
		// Request priority changed
	  castor::dlf::Param params2[] =
	  	{castor::dlf::Param("priority", newTapeReq->priority())};
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 24, 1, params2);
	  
	  
	  // Verify that the request doesn't exist, 
	  // by calling IVdqmSvc->checkTapeRequest
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 31);
	  /*
	   * Verify that the request doesn't (yet) exist. If it doesn't exist,
	   * the return value should be -1.
	   */
	  rowNumber = ptr_IVdqmService->checkTapeRequest(newTapeReq);
	  if ( rowNumber != -1 ) {
	    castor::exception::Internal ex;
	    ex.getMessage() << "Input request already queued: " 
	    								<< "Position = " << rowNumber << std::endl;
	    throw ex;
	  }
	  
	  // Try to store Request into the data base
 	  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 32);
	  /*
	   * Add the record to the volume queue
	   */
		handleRequest(newTapeReq, cuuid);
		
		/**
		 *  Now the newTapeReq has the id of its 
		 * row representatioon in the db table.
		 */
		volumeRequest->VolReqID = newTapeReq->id();
	   
	//------------------------------------------------------------------------------
	//------------------------------------------------------------------------------
	// TODO: This must be put in an own thread
	
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
	//  }
	  
	//------------------------------------------------------------------------------
	//------------------------------------------------------------------------------
	
	
	//  /*
	//   * Always update replica for this volume record (update status may have
	//   * been temporary reset by SelectVolAndDrv()).
	//   */
	//  newvolrec->update = 1;
	//  FreeDgnContext(&dgn_context);
  } catch(castor::exception::Exception e) {
 		if (newTapeReq)
	 		delete newTapeReq; //also deletes clientData, because of composition
 		if (tape)
	 		delete tape;
		if (reqExtDevGrp)
			delete reqExtDevGrp;
		if (reqTapeServer)
			delete reqTapeServer;
//		if (0 != freeTapeDrive)
//	delete freeTapeDrive;
  
    throw e;
  }
  
	// Delete all Objects
	delete newTapeReq; //also deletes clientData, because of composition 
	delete tape;
	delete reqExtDevGrp;
	delete reqTapeServer;
//	delete freeTapeDrive;
}


//------------------------------------------------------------------------------
// deleteTapeRequest
//------------------------------------------------------------------------------
void castor::vdqm::TapeRequestHandler::deleteTapeRequest(
																									vdqmVolReq_t *volumeRequest,
																									Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
 	//The db related informations
  TapeRequest *tapeReq = NULL;
  castor::BaseAddress ad;
  int rowNumber = -1;
	
  dgn_element_t *dgn_context;
  vdqm_volrec_t *volrec;
  vdqm_drvrec_t *drvrec;
  int rc;
	
	// Get the tapeReq from its id
	try {
    ad.setTarget(volumeRequest->VolReqID);
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    
    castor::IObject* obj = svcs()->createObj(&ad);
    
    tapeReq = dynamic_cast<TapeRequest*> (obj);
    if (0 == tapeReq) {
      castor::exception::Internal e;
      e.getMessage() << "createObj return unexpected type "
                     << obj->type() << " for id " << volumeRequest->VolReqID;
      delete obj;
      throw e;
    }
    
    //Now we get the foreign ClientIdentification object
    svcs()->fillObj(&ad, obj, castor::OBJ_ClientIdentification);
    
    obj = 0;
  } catch (castor::exception::Exception e) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to select tape for id " << volumeRequest->VolReqID  << " :"
      << std::endl << e.getMessage();
    
    throw ex;
  }
		
	//OK, now we have the tapeReq and its client :-)
	
	try {	
		/*
	   * Verify that the request is not already assigned to a TapeDrive
	   */
	  rowNumber = ptr_IVdqmService->checkTapeRequest(tapeReq);
	  if ( rowNumber == -1 ) {
	    castor::exception::Internal ex;
	    ex.getMessage() << "Can't delete TapeRequest with ID = " 
	    								<< volumeRequest->VolReqID
	    								<< ".The entry does not exist in the DB!" 
	    								<< std::endl;
	    throw ex;
	  }
	  else if ( rowNumber == 0 ) {
	  	// If we are here, the TapeRequest is already assigned to a TapeDrive
	  	//TODO:
	  	/*
		   * PROBLEM:
	     * Can only remove request if not assigned to a drive. Otherwise
	     * it should be cleanup by resetting the drive status to RELEASE + FREE.
	     * Mark it as UNKNOWN until an unit status clarifies what has happened.
	     */
	     		TapeDrive* tapeDrive;
	     
	     		//Ok, now we need the foreign TapeDrive, too
			    svcs()->fillObj(&ad, tapeReq, castor::OBJ_TapeDrive);
			    tapeDrive = tapeReq->tapeDrive();
			    
		      // Set new TapeDriveStatusCode
			    tapeDrive->setStatus(UNIT_RELEASED);
			    
			    tapeReq->setCreationTime(time(NULL));
			    
//			    svcs()->updateRep(&ad, true);
			    
//        drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//        volrec->vol.recvtime = time(NULL);
//        drvrec->update = 1;
//        volrec->update = 0;
//        vdqm_SetError(EVQREQASS);
//        rc = -1;
			// "TapeRequest is assigned to a TapeDrive. Can't delete it at the moment" message
		  castor::dlf::Param params[] =
		  	{castor::dlf::Param("TapeRequest ID", tapeReq->id())};
		  castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 30, 1, params);
	  }
	  else {
	  	// Remove the TapeRequest from the db (queue)
	  	deleteRepresentation(tapeReq, cuuid);
	  
			// "TapeRequest and its ClientIdentification removed" message
		  castor::dlf::Param params[] =
		  	{castor::dlf::Param("TapeRequest ID", tapeReq->id())};
		  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 28, 1, params);
	  }
  } catch(castor::exception::Exception e) {
 		if (tapeReq)
	 		delete tapeReq;
  
    throw e;
  }
  
  // Delete all Objects
  delete tapeReq;
}


//------------------------------------------------------------------------------
// getQueuePosition
//------------------------------------------------------------------------------
int castor::vdqm::TapeRequestHandler::getQueuePosition(
																									vdqmVolReq_t *volumeRequest,
																									Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
		
	//To store the db related informations
  castor::vdqm::TapeRequest *tapeReq = NULL;
  
  int queuePosition = -1;
  
  
  try {
	  tapeReq = new castor::vdqm::TapeRequest();
	  tapeReq->setId(volumeRequest->VolReqID);
	  
	  /**
	   * returns -1, if there is Request with this ID or the queuePosition
	   */
	  queuePosition = ptr_IVdqmService->checkTapeRequest(tapeReq);
	  
		// Queue position of TapeRequest
	  castor::dlf::Param params[] =
	  	{castor::dlf::Param("Queue position", queuePosition)};
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 26, 1, params);
	  
  } catch(castor::exception::Exception e) {
 		if (tapeReq)
	 		delete tapeReq;
  
    throw e;
  }
  // Delete the tapeRequest Object
  delete tapeReq;
  
  return queuePosition;
}


//------------------------------------------------------------------------------
// getRequest
//------------------------------------------------------------------------------
castor::IObject* castor::vdqm::TapeRequestHandler::getRequest() 
	throw (castor::exception::Exception) {
//TODO: Implementation
}


//------------------------------------------------------------------------------
// handleTapeRequestQueue
//------------------------------------------------------------------------------
void castor::vdqm::TapeRequestHandler::handleTapeRequestQueue() 
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
