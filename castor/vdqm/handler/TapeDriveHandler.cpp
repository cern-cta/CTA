/******************************************************************************
 *                      TapeDriveHandler.cpp
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
 * @(#)RCSfile: TapeDriveHandler.cpp  Revision: 1.0  Release Date: Jun 22, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
#include <net.h>
#include <vdqm_constants.h>
#include <vector>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/newVdqm.h"


// Local Includes
#include "TapeDriveHandler.hpp"
#include "TapeDriveConsistencyChecker.hpp" // We are a friend of him!
#include "TapeDriveStatusHandler.hpp" // We are a friend of him!

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::TapeDriveHandler(
	newVdqmHdr_t* header, 
	newVdqmDrvReq_t* driveRequest, 
	Cuuid_t cuuid) throw(castor::exception::Exception) {
		
	m_cuuid = cuuid;
	
	if ( header == NULL || driveRequest == NULL ) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "One of the arguments is NULL";
  	throw ex;
	}
	else {
		ptr_header = header; //The header is needed for the magic number
		ptr_driveRequest = driveRequest;
	}
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::~TapeDriveHandler() throw() {

}

//------------------------------------------------------------------------------
// newTapeDriveRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::newTapeDriveRequest() 
	throw (castor::exception::Exception) {
    
	castor::vdqm::TapeServer* tapeServer = NULL;
	castor::vdqm::TapeDrive* tapeDrive = NULL;
  

	//"The parameters of the old vdqm DrvReq Request" message
  castor::dlf::Param params[] =
  	{castor::dlf::Param("errorHistory (dedidcate)", ptr_driveRequest->errorHistory),
     castor::dlf::Param("dgn", ptr_driveRequest->dgn),
     castor::dlf::Param("drive", ptr_driveRequest->drive),
     castor::dlf::Param("errcount", ptr_driveRequest->errcount),
     castor::dlf::Param("jobID", ptr_driveRequest->jobID),
     castor::dlf::Param("MBtransf", ptr_driveRequest->MBtransf),
     castor::dlf::Param("mode", ptr_driveRequest->mode),
 		 castor::dlf::Param("recvtime", ptr_driveRequest->recvtime),
		 castor::dlf::Param("reqhost", ptr_driveRequest->reqhost),
		 castor::dlf::Param("resettime", ptr_driveRequest->resettime),
		 castor::dlf::Param("server", ptr_driveRequest->server),
		 castor::dlf::Param("status", ptr_driveRequest->status),
		 castor::dlf::Param("TotalMB", ptr_driveRequest->TotalMB),
		 castor::dlf::Param("usecount", ptr_driveRequest->usecount),
		 castor::dlf::Param("volid", ptr_driveRequest->volid),
		 castor::dlf::Param("VolReqID", ptr_driveRequest->VolReqID)};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 33, 32, params);


	try {
		tapeServer = ptr_IVdqmService->selectTapeServer(ptr_driveRequest->reqhost);

	  /**
	   * If it is an tape daemon startup status we delete all TapeDrives 
	   * on that tape server.
	   */
		if ( ptr_driveRequest->status == VDQM_TPD_STARTED ) {
			deleteAllTapeDrvsFromSrv(tapeServer);
		}
	} catch ( castor::exception::Exception ex ) {
		if ( tapeServer ) 
			delete tapeServer;
		
		throw ex;
	}

  /**
   * Gets the TapeDrive from the db or creates a new one.
   */
  tapeDrive = getTapeDrive(tapeServer);

	if ( ptr_driveRequest->status == VDQM_UNIT_QUERY ) {
		  copyTapeDriveInformations(tapeDrive);
      return;
	}

	/**
	 * Log the actual "new" status and the old one.
	 */
	printStatus(ptr_driveRequest->status,
																 tapeDrive->status());
																								
	

	/**
   * Verify that new unit status is consistent with the
   * current status of the drive.
   * In some cases a jobID can be overgiven to tapeDrive and the status
   * can already change to UNIT_STARTING or UNIT_ASSIGNED.
   */
	TapeDriveConsistencyChecker consistencyChecker(tapeDrive, ptr_driveRequest, m_cuuid);
	consistencyChecker.checkConsistency();

	//TODO implementing everything, which is commented out. ;-)


//    /*
//     * Reset UNKNOWN status if necessary. However we remember that status
//     * in case there is a RELEASE since we then cannot allow the unit
//     * to be assigned to another job until it is FREE (i.e. we must
//     * wait until volume has been unmounted). 
//     */
//    unknown = drvrec->drv.status & VDQM_UNIT_UNKNOWN;
//    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_UNKNOWN;

	/**
	 * If we have passed the consistency checker, then everything looks fine and 
	 * we are ready to handle the status from the tpdaemon.
	 */
	TapeDriveStatusHandler statusHandler(tapeDrive, ptr_driveRequest, m_cuuid);
	statusHandler.handleOldStatus();


	/**
	 * Now the last thing is to update the data base
	 */
	 updateRepresentation(tapeDrive, m_cuuid);
	 
  /**
   * Free memory for all Objects, which are not needed any more
   */
	freeMemory(tapeDrive, tapeServer);
		
	delete tapeDrive;
	
	tapeServer = 0;
	tapeDrive = 0;
}


//------------------------------------------------------------------------------
// deleteTapeDrive
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::deleteTapeDrive() 
	throw (castor::exception::Exception) {

		TapeDrive* tapeDrive = NULL; // the tape Drive which hast to be deleted
		TapeServer* tapeServer = NULL; // the tape server, where the tape drive is installed
  
  	// Get the tape server
		tapeServer = ptr_IVdqmService->selectTapeServer(ptr_driveRequest->reqhost);
			
		try {
			/**
			 * Check, if the tape drive already exists
			 */
			tapeDrive = ptr_IVdqmService->selectTapeDrive(ptr_driveRequest, tapeServer);    
		} catch (castor::exception::Exception ex) {
			delete tapeServer;
			
			throw ex;
		}
        
    if ( tapeDrive == NULL ) {
    	delete tapeServer;
  	
      castor::exception::Exception ex(EVQNOSDRV);
      ex.getMessage() << "TapeDriveHandler::deleteTapeDrive(): "
      								<< "drive record not found for drive "
      								<< ptr_driveRequest->drive << "@"
      								<< ptr_driveRequest->server << std::endl;
      throw ex;
    }
  
    /**
     * Free memory for all Objects, which are not needed any more
     */
		freeMemory(tapeDrive, tapeServer);
  
    /*
     * Don't allow to delete drives with running jobs.
     */
    if ( (tapeDrive->status() != UNIT_UP) &&
         (tapeDrive->status() != UNIT_DOWN) ) {
 			delete tapeDrive;
			tapeDrive = 0;   	
         	
			castor::exception::Exception ex(EVQREQASS);
			ex.getMessage() << "TapeDriveHandler::deleteTapeDrive(): "
											<< "Cannot remove drive record with assigned job."
											<< std::endl;
			throw ex;
    }
		
		try {
			deleteRepresentation(tapeDrive, m_cuuid);
		} 
		catch (castor::exception::Exception ex) {
			delete tapeDrive;
			tapeDrive = 0;
			
			throw ex;
		}
		
		delete tapeDrive;
		tapeServer = 0;
		tapeDrive = 0;
}


//------------------------------------------------------------------------------
// deleteAllTapeDrvsFromSrv
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::deleteAllTapeDrvsFromSrv(
	TapeServer* tapeServer) 
	throw (castor::exception::Exception) {
	
	if ( strcmp(ptr_driveRequest->reqhost, ptr_driveRequest->server) != 0 ) {
	  castor::exception::Exception ex(EPERM);
    ex.getMessage()
      << "TapeDriveHandler::deleteAllTapeDrvsFromSrv(): "
      << "unauthorized VDQM_TPD_STARTED for " 
      << ptr_driveRequest->server << " sent by "
      << ptr_driveRequest->reqhost
      << std::endl;
    
    throw ex;
  }
	
	/*
	 * Select the server and deletes all db entries of tapeDrives 
	 * (+ old TapeRequests), which are dedicated to this server.
	 */
	try {
		for (std::vector<castor::vdqm::TapeDrive*>::iterator it = tapeServer->tapeDrives().begin();
         it != tapeServer->tapeDrives().end();
         it++) 
    {
    	// The old TapeRequest. Normally it should not exist.
      TapeRequest* runningTapeReq = (*it)->runningTapeReq();   	
      
      if (runningTapeReq != 0) {
	      deleteRepresentation(runningTapeReq, m_cuuid);
      	delete runningTapeReq;
      }
      
      deleteRepresentation(*it, m_cuuid);
    }
	} catch ( castor::exception::Exception ex ) {
		if ( tapeServer ) 
			delete tapeServer;

		throw ex;
	}
	
	delete tapeServer;
}


//------------------------------------------------------------------------------
// getTapeDrive
//------------------------------------------------------------------------------
castor::vdqm::TapeDrive* 
	castor::vdqm::handler::TapeDriveHandler::getTapeDrive(TapeServer* tapeServer) 
	throw (castor::exception::Exception) {
		
		castor::vdqm::TapeDrive* tapeDrive = NULL;
		castor::vdqm::DeviceGroupName* dgName = NULL;
		
		/**
		 * Check, if the tape drive already exists
		 */
		tapeDrive = ptr_IVdqmService->selectTapeDrive(ptr_driveRequest, tapeServer);
		
		
		if ( ptr_driveRequest->status == VDQM_UNIT_QUERY ) {
      if ( tapeDrive == NULL ) {
				castor::exception::Exception ex(EVQNOSDRV);
				
				ex.getMessage() << "castor::vdqm::TapeDriveHandler::getTapeDrive(): "
												<< "Query request for unknown drive "
												<< ptr_driveRequest->drive << "@" 
												<< ptr_driveRequest->server << std::endl;

				throw ex;
      }
    }
		
		if (tapeDrive == NULL) {
			/**
			 * The tape drive does not exist, so we just create it!
			 * But first we need its device group name out of the db.
			 * If the dgn doesn't exists, we just create it.
			 */
			
			dgName = ptr_IVdqmService->selectDeviceGroupName(ptr_driveRequest->dgn);
			
      tapeDrive = new castor::vdqm::TapeDrive();

			tapeDrive->setDeviceGroupName(dgName);
      tapeDrive->setDriveName(ptr_driveRequest->drive);
      tapeDrive->setTapeServer(tapeServer);
      
      /**
       * The dedication string is not longer in use in the new VDQM.
       * We use the old field in the struct, to send error informations
       * back to the client.
       */

      
      tapeDrive->setErrcount(ptr_driveRequest->errcount);
      tapeDrive->setJobID(ptr_driveRequest->jobID);
      tapeDrive->setModificationTime(time(NULL));
      tapeDrive->setResettime(ptr_driveRequest->resettime);
      tapeDrive->setTotalMB(ptr_driveRequest->TotalMB);
      tapeDrive->setTransferredMB(ptr_driveRequest->MBtransf);
      tapeDrive->setUsecount(ptr_driveRequest->usecount);			
      
//      client = new castor::stager::ClientIdentification();
//      client->setEgid(ptr_driveRequest->gid);
//      client->setEuid(ptr_driveRequest->uid);
//      client->setMachine(ptr_driveRequest->reqhost);
//      client->setMagic(ptr_header->magic);
//      client->setUserName(ptr_driveRequest->name);
      
//      //Add the client informations to the tapeDrive
//      tapeDrive->setClient(client);
      
	    /*
	     * Make sure it is either up or down. If neither, we put it in
	     * UNKNOWN status until further status information is received.
	     * This is just for the old protocol.
	     */
	    if ( (ptr_driveRequest->status & ( VDQM_UNIT_UP|VDQM_UNIT_DOWN)) == 0 ) {
	    	ptr_driveRequest->status |= VDQM_UNIT_UP|VDQM_UNIT_UNKNOWN;
	      
	      /**
	       *  Our new tapeDrive Object is just in status UNIT_UP
	       */
	      tapeDrive->setStatus(UNIT_UP); 
	    }
	    
      /*
       * Make sure it doesn't come up with some non-persistent status
       * becasue of a previous VDQM server crash.
       */
      ptr_driveRequest->status = ptr_driveRequest->status & ( ~VDQM_VOL_MOUNT &
          ~VDQM_VOL_UNMOUNT & ~VDQM_UNIT_MBCOUNT );
          
      
			// "Create new TapeDrive in DB" message
			castor::dlf::dlf_writep(m_cuuid, DLF_LVL_USAGE, 34);
      
      
      /**
       * We don't want to commit now, because some changes can can still happen
       */
      handleRequest(tapeDrive, false, m_cuuid);
		}		
				
		return tapeDrive;
}


//------------------------------------------------------------------------------
// copyTapeDriveInformations
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::copyTapeDriveInformations(
	const TapeDrive* tapeDrive) 
	throw (castor::exception::Exception) {
	if (tapeDrive == NULL) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "The tapeDrive argument is NULL" << std::endl;
  	throw ex;
	}

//  castor::stager::ClientIdentification* client;

  ptr_driveRequest->errcount = tapeDrive->errcount();
  ptr_driveRequest->jobID = tapeDrive->jobID();
  ptr_driveRequest->resettime = tapeDrive->resettime();
  ptr_driveRequest->TotalMB = tapeDrive->totalMB();
  ptr_driveRequest->MBtransf = tapeDrive->transferredMB();
  ptr_driveRequest->usecount = tapeDrive->usecount();			
  
//	client = tapeDrive->client();
//  ptr_driveRequest->gid = client->egid();
//  ptr_driveRequest->uid = client->euid();
//  strcpy(ptr_driveRequest->reqhost, client->machine().c_str());
//  strcpy(ptr_driveRequest->name, client->userName().c_str());
}


//------------------------------------------------------------------------------
// printStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::printStatus(
	const int oldProtocolStatus, 
	const int newActStatus) 
	throw (castor::exception::Exception) {	
	
	std::string oldStatusString;
	
  int vdqm_status_values[] = VDQM_STATUS_VALUES;
  char vdqm_status_strings[][32] = VDQM_STATUS_STRINGS;
  int i;
  
  /**
   * We want first produce a string representation of the old status.
   * This is just for info reasons.
   */  
  oldStatusString = '\0';
  i=0;
  while ( *vdqm_status_strings[i] != '\0' ) {
      if ( oldProtocolStatus & vdqm_status_values[i] ) {
          oldStatusString = oldStatusString.append(vdqm_status_strings[i]);
          oldStatusString = oldStatusString.append("|");
      } 
      i++;
  }
	
  //"The desired old Protocol status of the client" message
  castor::dlf::Param param[] =
  	{castor::dlf::Param("status", oldStatusString)};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 35, 1, param);
	
	
	// "The actual \"new Protocol\" status of the client" message
	switch ( newActStatus ) {
		case UNIT_UP: 
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_UP")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}
					break;
		case UNIT_STARTING:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_STARTING")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case UNIT_ASSIGNED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_ASSIGNED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case UNIT_RELEASED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_RELEASED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case VOL_MOUNTED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "VOL_MOUNTED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case FORCED_UNMOUNT:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "FORCED_UNMOUNT")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case UNIT_DOWN:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_DOWN")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}		
					break;
		case WAIT_FOR_UNMOUNT:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "WAIT_FOR_UNMOUNT")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}	
		case STATUS_UNKNOWN:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "STATUS_UNKNOWN")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param1);
					}						
					break;
		default:
					castor::exception::InvalidArgument ex;
			  	ex.getMessage() << "The tapeDrive is in a wrong status" << std::endl;
  				throw ex;
	}
}


//------------------------------------------------------------------------------
// freeMemory
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::freeMemory(
	TapeDrive* tapeDrive, 
	TapeServer* tapeServer) {

  /**
   * Free memory for all Objects, which are not needed any more
   */
  delete tapeServer;
  
  if ( tapeDrive->tape() )
  	delete tapeDrive->tape();
	
	TapeRequest* runningTapeReq = tapeDrive->runningTapeReq();
	if ( runningTapeReq ) {
		delete runningTapeReq->tape();

		if ( runningTapeReq->requestedSrv() ) 
			delete runningTapeReq->requestedSrv();
			
		delete runningTapeReq;	 
		runningTapeReq = 0;
	}
}
