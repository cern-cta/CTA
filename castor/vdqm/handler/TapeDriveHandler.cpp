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

// Includes for VMGR
#include <sys/types.h>
#include "vmgr_api.h"
// for WRITE_ENABLE WRITE_DISABLE
#include <Ctape_constants.h>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/ClientIdentification.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/ErrorHistory.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeDriveCompatibility.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveDedication.hpp"
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
  

	// XXX: This log doesn't appear in th log!
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
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 33, 16, params);


	try {
		bool withTapeDrives = (ptr_driveRequest->status == VDQM_TPD_STARTED);
		tapeServer = 
			ptr_IVdqmService->selectTapeServer(ptr_driveRequest->reqhost, withTapeDrives);
		

	  /**
	   * If it is an tape daemon startup status we delete all TapeDrives 
	   * on that tape server.
	   */
		if ( ptr_driveRequest->status == VDQM_TPD_STARTED ) {
			deleteAllTapeDrvsFromSrv(tapeServer);
			return;
		}
	} catch ( castor::exception::Exception ex ) {
		if ( tapeServer ) {
			for (std::vector<castor::vdqm::TapeDrive*>::iterator it = tapeServer->tapeDrives().begin();
		         it != tapeServer->tapeDrives().end();
		         it++) {
		  	// The old TapeRequest. Normally it should not exist.
		    TapeRequest* runningTapeReq = (*it)->runningTapeReq();   	
		    
		    if (runningTapeReq != 0) {
		    	delete runningTapeReq;
		    	runningTapeReq = 0;
		    	(*it)->setRunningTapeReq(0);
		    }
		    
		    delete (*it);
		  } 
		  tapeServer->tapeDrives().clear();
		  delete tapeServer;
		  tapeServer = 0;			
		}
			
		
		throw ex;
	}

  /**
   * Gets the TapeDrive from the db or creates a new one.
   */
  tapeDrive = getTapeDrive(tapeServer);


	/**
	 * Log the actual "new" status and the old one.
	 */
	printStatus(ptr_driveRequest->status, tapeDrive->status());


	if ( ptr_driveRequest->status == VDQM_UNIT_QUERY ) {
		  copyTapeDriveInformations(tapeDrive);
		  
		  freeMemory(tapeDrive, tapeServer);
		  delete tapeDrive;
			tapeServer = 0;
			tapeDrive = 0;
			
      return;
	}																						

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
	 * Log the actual "new" status.
	 */
	printStatus(0, tapeDrive->status());
	 
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
		tapeServer = ptr_IVdqmService->selectTapeServer(ptr_driveRequest->reqhost, false);
			
		try {
			/**
			 * Check, if the tape drive already exists
			 */
			tapeDrive = ptr_IVdqmService->selectTapeDrive(ptr_driveRequest, tapeServer);    
		} catch (castor::exception::Exception ex) {
			delete tapeServer;
			tapeServer = 0;
			
			throw ex;
		}
        
    if ( tapeDrive == NULL ) {
    	delete tapeServer;
    	tapeServer = 0;
  	
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
      	runningTapeReq = 0;
      	(*it)->setRunningTapeReq(0);
      }
      
      deleteRepresentation(*it, m_cuuid);
      delete *it;
    }
    
    tapeServer->tapeDrives().clear();
	} catch ( castor::exception::Exception ex ) {
		if ( tapeServer ) { 
			for (std::vector<castor::vdqm::TapeDrive*>::iterator it = tapeServer->tapeDrives().begin();
         it != tapeServer->tapeDrives().end();
         it++) 
    	{
    		TapeRequest* runningTapeReq = (*it)->runningTapeReq();   	
    
		    if (runningTapeReq != 0) {
		    	delete runningTapeReq;
		    	runningTapeReq = 0;
		    	(*it)->setRunningTapeReq(0);
		    }
    
    		delete (*it);
    	}
			
			delete tapeServer;
			tapeServer = 0;
		}
		throw ex;
	}
	
	delete tapeServer;
	tapeServer = 0;
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
			 */
			
			dgName = ptr_IVdqmService->selectDeviceGroupName(ptr_driveRequest->dgn);
			
			if ( dgName == NULL ) {
				castor::exception::Exception ex(EVQDGNINVL);
				
				ex.getMessage() << "castor::vdqm::TapeDriveHandler::getTapeDrive(): "
												<< "Query request for unknown dgn: "
												<< ptr_driveRequest->dgn << std::endl;

				throw ex;				
			}
			
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
          
      
      std::string driveModel = ptr_driveRequest->tapeDriveModel;
      if ( driveModel == "" ) {
	      vmgr_list list;
				struct vmgr_tape_dgnmap *dgnmap;
				int flags;
	      
	      /**
				 * Retrieve the information about the cartridge model, if the tape drive
				 * name is not provided by the client.
				 * Please note, that this is a work around!
				 */
				flags = VMGR_LIST_BEGIN;
				while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {
					if ( std::strcmp(dgnmap->dgn, ptr_driveRequest->dgn) == 0 ) {
						driveModel = dgnmap->model;
						break;
					}
					flags = VMGR_LIST_CONTINUE;
				}
				(void) vmgr_listdgnmap (VMGR_LIST_END, &list);
      }
      
      /**
       * Looks, wheter there is already existing entries in the 
       * TapeDriveCompatibility table for that model
       */
      std::vector<castor::vdqm::TapeDriveCompatibility*> *tapeDriveCompatibilities = 
				ptr_IVdqmService->selectCompatibilitiesForDriveModel(driveModel);
          
      if ( tapeDriveCompatibilities == NULL ||
           tapeDriveCompatibilities->size() == 0 ) {
	      /**
	       * Handle the conection to the priority list of the tape Drives
	       */
	      handleTapeDriveCompatibilities(tapeDrive, driveModel);
      }
      else {
      	for (unsigned int i = 0; i < tapeDriveCompatibilities->size(); i++) {
	      	tapeDrive->addTapeDriveCompatibilities((*tapeDriveCompatibilities)[i]);
      	}
      	delete tapeDriveCompatibilities;
      	tapeDriveCompatibilities = 0;
      }
      
      
			// "Create new TapeDrive in DB" message
			castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, 34);
      /**
       * We don't want to commit now, because some changes can can still happen
       */
      handleRequest(tapeDrive, m_cuuid);
		}		
				
		return tapeDrive;
}


//------------------------------------------------------------------------------
// copyTapeDriveInformations
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::copyTapeDriveInformations(
	TapeDrive* tapeDrive) 
	throw (castor::exception::Exception) {
	if (tapeDrive == NULL) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "The tapeDrive argument is NULL" << std::endl;
  	throw ex;
	}

	castor::vdqm::TapeRequest* runningTapeReq;
	castor::stager::Tape* tape;
	castor::vdqm::TapeServer* tapeServer;
	castor::vdqm::DeviceGroupName* devGrpName;
	

//  castor::stager::ClientIdentification* client;

	switch ( tapeDrive->status() ) {
		case UNIT_UP:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_FREE;
			break;
		case UNIT_STARTING:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_BUSY;
			break;
		case UNIT_ASSIGNED:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
			break;
		case VOL_MOUNTED:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
			break;
		case FORCED_UNMOUNT:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE 
			                           | VDQM_UNIT_UNKNOWN | VDQM_FORCE_UNMOUNT;
			break;
		case UNIT_DOWN:
			ptr_driveRequest->status = VDQM_UNIT_DOWN;
			break;
		case WAIT_FOR_UNMOUNT:
			ptr_driveRequest->status = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE 
			                           | VDQM_UNIT_UNKNOWN;
			break;
		case STATUS_UNKNOWN:
			ptr_driveRequest->status = VDQM_UNIT_UNKNOWN;
			break;
	}
  
  
  ptr_driveRequest->DrvReqID  = (unsigned int)tapeDrive->id();
  
  ptr_driveRequest->jobID     = tapeDrive->jobID();
	ptr_driveRequest->recvtime  = (int)tapeDrive->modificationTime();
  ptr_driveRequest->resettime = (int)tapeDrive->resettime();
  ptr_driveRequest->usecount  = tapeDrive->usecount();			  
  ptr_driveRequest->errcount  = tapeDrive->errcount();
  ptr_driveRequest->MBtransf  = tapeDrive->transferredMB();
  ptr_driveRequest->mode      = tapeDrive->tapeAccessMode();
  ptr_driveRequest->TotalMB   = tapeDrive->totalMB();
  
  runningTapeReq = tapeDrive->runningTapeReq();
  if ( NULL != runningTapeReq ) {
  	ptr_driveRequest->VolReqID = (unsigned int)runningTapeReq->id();
  	
		castor::stager::ClientIdentification* client = runningTapeReq->client();
	  strcpy(ptr_driveRequest->reqhost, client->machine().c_str());
	  
	  client = 0;
	  runningTapeReq = 0; 
  }
  
  tape = tapeDrive->tape();
  if ( NULL != tape ) {
  	strcpy(ptr_driveRequest->volid, tape->vid().c_str());
  	
  	tape = 0;
  }
  
  tapeServer = tapeDrive->tapeServer();
  strcpy(ptr_driveRequest->server, tapeServer->serverName().c_str());
  tapeServer = 0;
  
  strcpy(ptr_driveRequest->drive, tapeDrive->driveName().c_str());
  
  devGrpName = tapeDrive->deviceGroupName();
  strcpy(ptr_driveRequest->dgn, devGrpName->dgName().c_str());
  devGrpName = 0;
  
  //TODO: Take the last occured error out of the ErrorHistoy table
//  ptr_driveRequest->errorHistory

	std::vector<castor::vdqm::TapeDriveCompatibility*> driveCompatibilities =
	  tapeDrive->tapeDriveCompatibilities();
	if ( driveCompatibilities.size() > 0 ) {
	  strcpy(ptr_driveRequest->tapeDriveModel, driveCompatibilities[0]->tapeDriveModel().c_str());
	}
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
  
  if (oldProtocolStatus != 0) {
	  /**
	   * We want first produce a string representation of the old status.
	   * This is just for info reasons.
	   */  
	  oldStatusString = "";
	  i=0;
	  while ( *vdqm_status_strings[i] != '\0' ) {
	      if ( oldProtocolStatus & vdqm_status_values[i] ) {
	      	if ( oldStatusString.length() > 0 )
		      	oldStatusString = oldStatusString.append("|");
		          
	      	oldStatusString = oldStatusString.append(vdqm_status_strings[i]);
	      } 
	      i++;
	  }
		
	  //"The desired old Protocol status of the client" message
	  castor::dlf::Param param[] =
	  	{castor::dlf::Param("status", oldStatusString)};
	  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 35, 1, param);
  }
	
	
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
					break;
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
  
  delete tapeDrive->deviceGroupName();
  tapeDrive->setDeviceGroupName(0);
  
  if ( tapeDrive->tape() ) {
  	delete tapeDrive->tape();
  	tapeDrive->setTape(0);
  }
	
	
	
	TapeRequest* runningTapeReq = tapeDrive->runningTapeReq();
	if ( runningTapeReq ) {
		delete runningTapeReq->tape();
		runningTapeReq->setTape(0);

		if ( runningTapeReq->requestedSrv() ) {
			delete runningTapeReq->requestedSrv();
			runningTapeReq->setRequestedSrv(0);
		}

		delete runningTapeReq->deviceGroupName();
		runningTapeReq->setDeviceGroupName(0);
		
		delete runningTapeReq->tapeAccessSpecification();
		runningTapeReq->setTapeAccessSpecification(0);
		
			
		delete runningTapeReq;	 
		runningTapeReq = 0;
		tapeDrive->setRunningTapeReq(0);
	}
	
  delete tapeServer;
  tapeServer = 0;
  tapeDrive->setTapeServer(0);
  
  std::vector<castor::vdqm::ErrorHistory*> errorHistoryVector = tapeDrive->errorHistory();
	for (unsigned int i = 0; i < errorHistoryVector.size(); i++) {
    if ( errorHistoryVector[i]->tape() != NULL ) {
    	delete errorHistoryVector[i]->tape();
    	errorHistoryVector[i]->setTape(0);
    }
    
    delete errorHistoryVector[i];
  }
  errorHistoryVector.clear();

	std::vector<castor::vdqm::TapeDriveDedication*> tapeDriveDedicationVector = tapeDrive->tapeDriveDedication();
  for (unsigned int i = 0; i < tapeDriveDedicationVector.size(); i++) {
    delete tapeDriveDedicationVector[i];
  }  
  tapeDriveDedicationVector.clear();
  
  std::vector<castor::vdqm::TapeDriveCompatibility*> tapeDriveCompatibilityVector = tapeDrive->tapeDriveCompatibilities();
  for (unsigned int i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
    if ( tapeDriveCompatibilityVector[i]->tapeAccessSpecification() != NULL ) {
    	delete tapeDriveCompatibilityVector[i]->tapeAccessSpecification();
    	tapeDriveCompatibilityVector[i]->setTapeAccessSpecification(0);
    }
    
    delete tapeDriveCompatibilityVector[i];
  }  
  tapeDriveCompatibilityVector.clear();
}


//------------------------------------------------------------------------------
// sendTapeDriveQueue
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::sendTapeDriveQueue(
	newVdqmVolReq_t *volumeRequest,
	castor::vdqm::OldProtocolInterpreter* oldProtInterpreter) 
	throw (castor::exception::Exception) {

	// The result of the search in the database.
  std::vector<castor::vdqm::TapeDrive*>* result = 0;
  
  std::string dgn, server;
		
	dgn = "";
  server = "";

  if ( *(ptr_driveRequest->dgn) != '\0' ) dgn = ptr_driveRequest->dgn;
  if ( *(ptr_driveRequest->server) != '\0' ) server = ptr_driveRequest->server;

	try {
		/**
		 * With this function call we get the requested queue out of the db.
		 * The result depends on the parameters. If the paramters are not specified,
		 * then we get all tapeRequests back.
		 */
		result = ptr_IVdqmService->selectTapeDriveQueue(dgn, server);

		if ( result != NULL && result->size() > 0 ) {
			//If we are here, then we got a result which we can send to the client
			for(std::vector<castor::vdqm::TapeDrive*>::iterator it = result->begin();
		      it != result->end();
		      it++) {
		    
		    ptr_driveRequest->status = translateNewStatus((*it)->status());
		    ptr_driveRequest->DrvReqID = (*it)->id();
		  
		    TapeRequest* tapeRequest = (*it)->runningTapeReq();
		    if ( tapeRequest != NULL ) {
			    ptr_driveRequest->VolReqID = (unsigned int)tapeRequest->id();
			    
			    delete tapeRequest;
			    tapeRequest = 0;
			    (*it)->setRunningTapeReq(0);
		    } 
		    else 
		    	ptr_driveRequest->VolReqID = 0;
		    
		    ptr_driveRequest->jobID = (*it)->jobID();
		    ptr_driveRequest->recvtime = (int)(*it)->modificationTime();
		    ptr_driveRequest->resettime = (int)(*it)->resettime();
		    ptr_driveRequest->usecount = (*it)->usecount();
		    ptr_driveRequest->errcount = (*it)->errcount();
		    ptr_driveRequest->MBtransf = (*it)->transferredMB();
		    ptr_driveRequest->mode = (*it)->tapeAccessMode();
		    ptr_driveRequest->TotalMB = (*it)->totalMB();
		    strcpy(ptr_driveRequest->reqhost, (*it)->tapeServer()->serverName().c_str());
		    
		    castor::stager::Tape* tape = (*it)->tape();
		    if ( tape != NULL ) {
		    	strcpy(ptr_driveRequest->volid, tape->vid().c_str());
		    	
		    	delete tape;
		    	tape = 0;
		    	(*it)->setTape(0);
		    }
		    else 
		    	strcpy(ptr_driveRequest->volid, "");
		    
	
		    strcpy(ptr_driveRequest->server, (*it)->tapeServer()->serverName().c_str());
		    strcpy(ptr_driveRequest->drive, (*it)->driveName().c_str());
		    strcpy(ptr_driveRequest->dgn, (*it)->deviceGroupName()->dgName().c_str());
		    
		    /**
		     * TODO: Here, we can give infomation about occured errors, which will
		     * be stored in the future in ErrorHistory. At the moment we leave it 
		     * empty, because we don't care about the old dedicate string.
		     */
		    strcpy(ptr_driveRequest->errorHistory, "");
		    
		    std::vector<TapeDriveCompatibility*> tapeDriveCompatibilities
		    	= (*it)->tapeDriveCompatibilities();
		    if ((&tapeDriveCompatibilities) != NULL &&
		        tapeDriveCompatibilities.size() > 0) {
		        		    
			    strcpy(ptr_driveRequest->tapeDriveModel, 
			           tapeDriveCompatibilities[0]->tapeDriveModel().c_str());
			           
			    for(std::vector<TapeDriveCompatibility*>::iterator it2 = tapeDriveCompatibilities.begin();
		      		it2 != tapeDriveCompatibilities.end();
		      		it2++) {
		      		delete (*it2);
		      }
		    }
		    else
			    strcpy(ptr_driveRequest->tapeDriveModel, "");	    	
		      	
		  	
		  	//free memory
		  	castor::vdqm::TapeServer* tapeServer = (*it)->tapeServer();
		  	delete (*it)->deviceGroupName();
		  	(*it)->setDeviceGroupName(0);
		  	delete (*it);
		  	delete tapeServer;
		  	tapeServer = 0;
		  	(*it) = 0;
		  	
		  	
		  	//"Send information for showqueues command" message
			  castor::dlf::Param param[] =
	  			{castor::dlf::Param("message", "TapeDrive info"),
	  			 castor::dlf::Param("TapeDrive ID", ptr_driveRequest->DrvReqID)};
	  		castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 57, 2, param);
		  	
		  	//Send informations to the client
		  	oldProtInterpreter->sendToOldClient(
		    	ptr_header, volumeRequest, ptr_driveRequest);
		  }
		}
	} catch (castor::exception::Exception ex) {	
		//free memory
		if ( result != NULL ) {
			for(std::vector<castor::vdqm::TapeDrive*>::iterator it = result->begin();
			      it != result->end();
			      it++) {
				if ( (*it) != NULL ) {
					TapeRequest* tapeRequest = (*it)->runningTapeReq();
			    if ( tapeRequest != NULL ) {
				    delete tapeRequest;
				    tapeRequest = 0;
				    (*it)->setRunningTapeReq(0);
			    }
			    
			    castor::stager::Tape* tape = (*it)->tape();
			    if ( tape != NULL ) {
			    	delete tape;
			    	tape = 0;
			    	(*it)->setTape(0);
			    }
			    
			    std::vector<TapeDriveCompatibility*> tapeDriveCompatibilities
			    	= (*it)->tapeDriveCompatibilities();
			    if ((&tapeDriveCompatibilities) != NULL &&
			        tapeDriveCompatibilities.size() > 0) {
			      for(std::vector<TapeDriveCompatibility*>::iterator it2 = tapeDriveCompatibilities.begin();
			      		it2 != tapeDriveCompatibilities.end();
			      		it2++) {
			      		delete (*it2);
			      }
			    }
					
			  	castor::vdqm::TapeServer* tapeServer = (*it)->tapeServer();				
			  	delete (*it)->deviceGroupName();
			  	(*it)->setDeviceGroupName(0);
			  	delete (*it);
			  	delete tapeServer;
			  	tapeServer = 0;
			  	(*it) = 0;
				}
			}
			
			// deletion of the vector
			delete result;
			result = 0;
		}
		/**
		 * To inform the client about the end of the queue, we send again a 
		 * ptr_driveRequest with the VolReqID = -1
		 */
	  ptr_driveRequest->DrvReqID = -1;
  
	  oldProtInterpreter->sendToOldClient(ptr_header, volumeRequest, ptr_driveRequest);
					
		throw ex;
	}
	
	// deletion of the vector
	delete result;
	result = 0;
	
	/**
	 * To inform the client about the end of the queue, we send again a 
	 * ptr_driveRequest with the VolReqID = -1
	 */
  ptr_driveRequest->DrvReqID = -1;
  
  oldProtInterpreter->sendToOldClient(ptr_header, volumeRequest, ptr_driveRequest);
}


//------------------------------------------------------------------------------
// translateNewStatus
//------------------------------------------------------------------------------
int castor::vdqm::handler::TapeDriveHandler::translateNewStatus(
	castor::vdqm::TapeDriveStatusCodes newStatusCode) 
	throw (castor::exception::Exception) {
	
	int oldStatus = 0;
	
	switch (newStatusCode) {
		case UNIT_UP:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_FREE;
				break;
		case UNIT_STARTING:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY;
				break;
		case UNIT_ASSIGNED:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_ASSIGN | VDQM_UNIT_BUSY;
				break;
		case VOL_MOUNTED:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
				break;
		case FORCED_UNMOUNT:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE | 
				             VDQM_FORCE_UNMOUNT | VDQM_UNIT_UNKNOWN;
				break;
		case UNIT_DOWN:
				oldStatus = VDQM_UNIT_DOWN;
				break;
		case WAIT_FOR_UNMOUNT:
				oldStatus = VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE | 
				             VDQM_UNIT_UNKNOWN;
				break;
		case STATUS_UNKNOWN:
				oldStatus = VDQM_UNIT_UNKNOWN;
				break;		
		default:
				castor::exception::InvalidArgument ex;
		  	ex.getMessage() << "The tapeDrive is in a wrong status" << std::endl;
				throw ex;																				
	}	
	
	return oldStatus;
}


//------------------------------------------------------------------------------
// handleTapeDriveCompatibilities
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::handleTapeDriveCompatibilities(
	castor::vdqm::TapeDrive *newTapeDrive, std::string driveModel) 
	throw (castor::exception::Exception) {
		
	castor::vdqm::DeviceGroupName *dgName = NULL;
	castor::vdqm::TapeDriveCompatibility *driveCompatibility = NULL;
	std::vector<castor::vdqm::TapeAccessSpecification*> *tapeAccessSpecs = NULL;
	
	vmgr_list list;
	struct vmgr_tape_dgnmap *dgnmap;
	bool tdcUpdate;
	int flags, priorityLevel;
	unsigned int i = 0;
	
	// The DeviceGroupName is absolutely necessary!!!
	dgName = newTapeDrive->deviceGroupName();
	if ( dgName == NULL ) {
		castor::exception::Exception ex(EVQDGNINVL);
		ex.getMessage() << "castor::vdqm::TapeDriveHandler::handleTapeDriveCompatibilities(): "
										<< "Query request for unknown dgn."
										<< std::endl;

		throw ex;
	}
	
	priorityLevel = 0;
	
	/**
	 * Retrieve the information about the dgName of the tapeModel out of
	 */
	flags = VMGR_LIST_BEGIN;
	while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {
		if ( std::strcmp(dgnmap->dgn, dgName->dgName().c_str()) == 0 ) {
			tapeAccessSpecs =
				ptr_IVdqmService->selectTapeAccessSpecifications(dgnmap->model);
			
			if ( tapeAccessSpecs != NULL) {
				
				/**
				 * XXX
				 * In case that the tape drive model name is not sent with the
				 * client request, we have to take the same name as the tape model.
				 */
				if ( driveModel == "" ) {
					driveModel = dgnmap->model;
				}
				
				/**
				 * Add for each found specification an entry in the tapeDriveCompatibility
				 * vector. Later, we will set then right priority order
				 */
				for (i = 0; i < tapeAccessSpecs->size(); i++) {
					driveCompatibility = new castor::vdqm::TapeDriveCompatibility();
			    driveCompatibility->setPriorityLevel(priorityLevel++); //Highest Priority = 0
			    driveCompatibility->setTapeDriveModel(driveModel);
			    driveCompatibility->setTapeAccessSpecification((*tapeAccessSpecs)[i]);
			    newTapeDrive->addTapeDriveCompatibilities(driveCompatibility);
			  }
			  delete tapeAccessSpecs;
			  tapeAccessSpecs = 0;
			}
		}
		
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdgnmap (VMGR_LIST_END, &list);
	
	if ( priorityLevel == 0 ) {
		castor::exception::Exception ex(EVQDGNINVL);
		ex.getMessage() << "castor::vdqm::TapeDriveHandler::handleTapeDriveCompatibilities(): "
										<< "There are no tape models stored in the TapeAccessSpecification "
										<< "table for dgn = " << dgName->dgName()  << ". "
										<< "Please run the 'vdqmDBInit' command to solve the Problem!"
										<< std::endl;

		throw ex;
	}
	else if ( priorityLevel > 2) {
		/**
		 * If there are more than 2 entries, we have to order the priority List again:
		 * First write, then read!
		 */
		priorityLevel = 0;
		std::vector<castor::vdqm::TapeDriveCompatibility*> tapeDriveCompatibilityVector = 
			newTapeDrive->tapeDriveCompatibilities();
	  // Let's change the WRITE_ENABLE priority to 0;
	  for (i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
	    if ( tapeDriveCompatibilityVector[i]->tapeAccessSpecification()->accessMode() == WRITE_ENABLE ) {
	    	tapeDriveCompatibilityVector[i]->setPriorityLevel(priorityLevel);
	    }
	  } 
	  // ... and then WRITE_DISABLE priority to 1;
	  priorityLevel++;
	  for (i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
	    if ( tapeDriveCompatibilityVector[i]->tapeAccessSpecification()->accessMode() == WRITE_DISABLE ) {
	    	tapeDriveCompatibilityVector[i]->setPriorityLevel(priorityLevel);
	    }
	  } 
	}
	
	
	/**
	 * Finally, we have to store the new TapeDriveCompatibility objects
	 */
	std::vector<castor::vdqm::TapeDriveCompatibility*> tapeDriveCompatibilityVector = 
			newTapeDrive->tapeDriveCompatibilities();
  for (i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
    handleRequest(tapeDriveCompatibilityVector[i], m_cuuid);
  }  
}
