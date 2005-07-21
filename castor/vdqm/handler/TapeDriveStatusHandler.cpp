/******************************************************************************
 *                      TapeDriveStatusHandler.cpp
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
 * @(#)RCSfile: TapeDriveStatusHandler.cpp  Revision: 1.0  Release Date: Jul 6, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
 
#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/ClientIdentification.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/vdqm/ExtendedDeviceGroup.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeServer.hpp"

#include <net.h>
#include <vdqm.h>
#include <vdqm_constants.h>
 
// Local Includes
#include "TapeDriveStatusHandler.hpp"


//To make the code more readable
using namespace castor::vdqm;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveStatusHandler::TapeDriveStatusHandler(
	TapeDrive* tapeDrive, 
	vdqmDrvReq_t* driveRequest, 
	Cuuid_t cuuid) throw(castor::exception::Exception) {
		
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
castor::vdqm::handler::TapeDriveStatusHandler::~TapeDriveStatusHandler() 
	throw() {

}


//------------------------------------------------------------------------------
// handleOldStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveStatusHandler::handleOldStatus() 
	throw (castor::exception::Exception) {

			


	if ( ptr_tapeDrive->status() == UNIT_UP ) {

    if ( (ptr_driveRequest->status & VDQM_UNIT_MBCOUNT) ) {
        /*
         * Update TotalMB counter. Since this request is sent by
         * RTCOPY rather than the tape daemon we cannot yet reset
         * unknown status if it was previously set.
         */
        ptr_tapeDrive->setTransferredMB(ptr_driveRequest->MBtransf);
        ptr_tapeDrive->setTotalMB(ptr_tapeDrive->totalMB() + ptr_driveRequest->MBtransf);
    }
    
    if ( (ptr_driveRequest->status & VDQM_UNIT_ERROR) ) {
        /*
         * Update error counter.
         */
        ptr_tapeDrive->setErrcount(ptr_tapeDrive->errcount() + 1);
    }
    
    
		if ( (ptr_driveRequest->status & VDQM_VOL_MOUNT) ) {
			handleVolMountStatus();
		}
	  if ( (ptr_driveRequest->status & VDQM_VOL_UNMOUNT) ) {
			handleVolUnmountStatus();
	  }
		if ((ptr_driveRequest->status & VDQM_UNIT_RELEASE) &&
		    !(ptr_driveRequest->status & VDQM_UNIT_FREE) ) { 
			handleUnitReleaseStatus();
		} 
		
	  /*
	   * If unit is free, reset dynamic data in drive record
	   */
	  if ( (ptr_driveRequest->status & VDQM_UNIT_FREE) ) {
			handleUnitFreeStatus();
		}
		
// TODO: This code must be implemented in an own thread		
//------------------------------------------------------------------------------
//        /*
//         * Loop until there is no more volume requests in queue or
//         * no suitable free drive
//         */
//        for (;;) {
//            /*
//             * If volrec already assigned by AnyVolRecForMountedVol()
//             * we skip to start request
//             */
//            if ( volrec == NULL ) {
//                rc = AnyVolRecForDgn(dgn_context);
//                if ( rc < 0 ) {
//
//                    log(LOG_ERR,"vdqm_NewDrvReq(): AnyVolRecForDgn() returned error\n");
//                    break;
//                }
//                if ( rc == 0 ) break;
//                if ( rc == 1 ) {
//                    rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
//                    if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
//                        log(LOG_ERR,"vdqm_NewDrvReq(): SelectVolAndDrv() returned rc=%d\n",
//                            rc);
//                        break;
//                    } else {
//                        drvrec->vol = volrec;
//                        volrec->drv = drvrec;
//                    }
//                }
//            }
//            if ( volrec != NULL ) {
//                drvrec->drv.VolReqID = volrec->vol.VolReqID;
//                volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
//                /*
//                 * Start the job
//                 */
//                rc = vdqm_StartJob(volrec);
//                if ( rc < 0 ) {
//                    /*
//                     * Job could not be started. Mark the drive
//                     * status as unknown so that it will not be
//                     * assigned to a new request immediately. The
//                     * volume record is kept in queue. Note that
//                     * if a volume is already mounted on unit it
//                     * is inaccessible for other requests until 
//                     * drive status is updated.
//                     */
//                    log(LOG_ERR,"vdqm_NewDrvReq(): vdqm_StartJob() returned error\n");
//                    drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                    drvrec->drv.recvtime = (int)time(NULL);
//                    drvrec->vol = NULL;
//                    drvrec->drv.VolReqID = 0;
//                    volrec->vol.DrvReqID = 0;
//                    drvrec->drv.jobID = 0;
//                    volrec->drv = NULL;
//                    break;
//                } 
//            } else break;
//            volrec = NULL;
//        } /* End of for (;;) */
//------------------------------------------------------------------------------
	} 
	else { // TapeDrive is DOWN
	  /*
	   * If drive is down, report error for any requested update.
	   */
	  if ( ptr_driveRequest->status & (VDQM_UNIT_FREE | VDQM_UNIT_ASSIGN |
	      VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE | VDQM_VOL_MOUNT |
	      VDQM_VOL_UNMOUNT ) ) {
	      
			castor::exception::Exception ex(EVQUNNOTUP);
			ex.getMessage() << "TapeDriveStatusHandler::handleOldStatus(): "
											<< "TapeDrive is DOWN"
											<< std::endl;
											
			throw ex;												      
	  }
	}
}


//------------------------------------------------------------------------------
// handleVolMountStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveStatusHandler::handleVolMountStatus() 
	throw (castor::exception::Exception) {
		
	TapeRequest* tapeRequest = ptr_tapeDrive->runningTapeReq();
	TapeServer* tapeServer = ptr_tapeDrive->tapeServer();
	castor::stager::Tape* mountedTape = NULL;	
	/*
	 * A mount volume request. The unit must first have been assigned.
	 */
	if ( ptr_tapeDrive->status() != UNIT_ASSIGNED) {
		castor::exception::Exception ex(EVQNOTASS);
		ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
										<< "Mount request of "
										<< ptr_driveRequest->volid << " for jobID "
										<< ptr_driveRequest->jobID
										<< " on non-ASSIGNED unit." << std::endl;
										
		throw ex;											
	}
	
	if ( *ptr_driveRequest->volid == '\0' ) {
		castor::exception::Exception ex(EVQBADVOLID);
		ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
										<< "Mount request with empty VOLID for jobID "
										<< ptr_tapeDrive->jobID() << std::endl;
										
		throw ex;		
	}
	
	/*
	 * Make sure that requested volume and assign volume record are the same
	 */
	if ( tapeRequest != NULL && 
			 strcmp(tapeRequest->tape()->vid().c_str(), ptr_driveRequest->volid) ) {
		castor::exception::Exception ex(EVQBADVOLID);
		ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
										<< "Inconsistent mount "
										<< ptr_driveRequest->volid << " (should be "
										<< tapeRequest->tape()->vid() << ") for jobID "
										<< ptr_driveRequest->jobID << std::endl;										
		throw ex;					 				 	
	}
	
	/*
	 * If there are no assigned volume request it means that this
	 * is a local request. Make sure that server and reqhost are
	 * the same and that the volume is free.
	 */
	if ( tapeRequest == NULL ) {
	  if ( strcmp(tapeServer->serverName().c_str(), ptr_driveRequest->reqhost) != 0 ) {
			castor::exception::Exception ex(EPERM);
			ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
											<< "Can only mount a volume without an assigned volume "
											<< "request, if it is a local request." << std::endl;
			throw ex;										
	  }
	  
		//TODO: Write a method for IVdqmService!!!
 	  if ( ptr_IVdqmService->existTapeDriveWithTapeInUse(ptr_driveRequest->volid) ||
			ptr_IVdqmService->existTapeDriveWithTapeMounted(ptr_driveRequest->volid) ) {
			castor::exception::Exception ex(EBUSY);
			ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
											<< "TapeDrive is busy with another request" << std::endl;
			throw ex;				
		}
	  
	  ptr_tapeDrive->setTapeAccessMode(-1); /* Mode is unknown */
	} 
	else {
		/**
		 * Not needed any more!
		 */
		ptr_tapeDrive->setTapeAccessMode(tapeRequest->reqExtDevGrp()->accessMode());
	}
	
	//The tape, which is now in the tape drive
	mountedTape = ptr_IVdqmService->selectTapeByVid(ptr_driveRequest->volid);
	if (mountedTape != NULL)
		ptr_tapeDrive->setTape(mountedTape);
	else {
		castor::exception::Exception ex(EVQBADVOLID);
		ex.getMessage() << "TapeDriveStatusHandler::handleVolMountStatus(): "
										<< "No Tape with specified volid in db" << std::endl;
										
		throw ex;		
	}
	
	// Now we can switch from UNIT_ASSIGNED to the next status 
	ptr_tapeDrive->setStatus(VOL_MOUNTED);
	
	/*
	 * Update usage counter
	 */
	ptr_tapeDrive->setUsecount(ptr_tapeDrive->usecount() + 1);
}


//------------------------------------------------------------------------------
// handleVolUnmountStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveStatusHandler::handleVolUnmountStatus() 
	throw (castor::exception::Exception) {
	
	castor::stager::Tape* tape = ptr_tapeDrive->tape();
	delete tape;
	
	ptr_tapeDrive->setTape(NULL);
	ptr_tapeDrive->setTapeAccessMode(-1); // UNKNOWN
	
	
	if ( ptr_tapeDrive->status() == WAIT_FOR_UNMOUNT ) {
		/*
		 * Set status to FREE if there is no job assigned to the unit
		 */
		if ( ptr_tapeDrive->runningTapeReq() == NULL )
		  ptr_tapeDrive->setStatus(UNIT_UP);
		else if ( ptr_tapeDrive->jobID() == 0 )
			ptr_tapeDrive->setStatus(UNIT_STARTING);
		else
		  ptr_tapeDrive->setStatus(UNIT_ASSIGNED);
		  
	}
	else if ( ptr_tapeDrive->status() == FORCED_UNMOUNT ) {
		ptr_tapeDrive->setStatus(UNIT_UP);
	}
	else {
		/**
		 * The status of the tape drive must be WAIT_FOR_UNMOUNT or
		 * FORCED_UNMOUNT. If it is not the case, we throw an error!
		 */
		ptr_tapeDrive->setStatus(STATUS_UNKNOWN);
		
   	castor::exception::Exception ex(EVQBADSTAT);
		ex.getMessage() << "TapeDriveStatusHandler::handleVolUnmountStatus(): "
										<< "bad status from tpdaemon! Can't unmount tape in this status! "
										<< "TapeDrive is set to STATUS_UNKNOWN." 
										<< std::endl;			
		throw ex;		
	}
}


//------------------------------------------------------------------------------
// handleUnitReleaseStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveStatusHandler::handleUnitReleaseStatus() 
	throw (castor::exception::Exception) {
	
	TapeRequest* tapeRequest = NULL;
	TapeRequest* newTapeRequest = NULL; // is needed, if there is another request for the same tape
	castor::stager::Tape* tape = NULL;
					
	
	/*
	 * Reset request
	 */
	tapeRequest = ptr_tapeDrive->runningTapeReq();
	if ( tapeRequest != NULL) {
		deleteRepresentation(tapeRequest, m_cuuid);
		delete tapeRequest;
		ptr_tapeDrive->setRunningTapeReq(NULL);
	}
	
	/**
	 * Reset job ID
	 */
	ptr_tapeDrive->setJobID(0);
	
	
	tape = ptr_tapeDrive->tape();
	if ( tape != NULL ) {
	    /*
	     * Consistency check: if input request contains a volid it
	     * should correspond to the one in the drive record. This
	     * is not fatal but should be logged.
	     */
	    if ( *ptr_driveRequest->volid != '\0' && 
	         strcmp(ptr_driveRequest->volid, tape->vid().c_str()) ) {
	       
	      // "Inconsistent release on tape drive" message 
	      castor::dlf::Param params[] =
	  			{castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
	  			 castor::dlf::Param("serverName", ptr_tapeDrive->tapeServer()->serverName()),
	  			 castor::dlf::Param("volid from client", ptr_driveRequest->volid),
	  			 castor::dlf::Param("volid from tape drive", tape->vid()),
	  			 castor::dlf::Param("jobID", ptr_tapeDrive->jobID())};
				castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, 41, 5, params);
	    }
	    
	    /*
	     * Fill in current volid in case we need to request an unmount.
	     */
	    if ( *ptr_driveRequest->volid == '\0' ) 
	    	strcpy(ptr_driveRequest->volid, tape->vid().c_str());
	    
	    /*
	     * If a tape is mounted but the current job ended: check if there
	     * are any other valid requests for the same volume. The volume
	     * can only be re-used on this unit if the modes (R/W) are the 
	     * same. If client indicated an error or forced unmount the
	     * will remain with that state until a VOL_UNMOUNT is received
	     * and both drive and volume cannot be reused until then.
	     * If the drive status was UNKNOWN at entry we must force an
	     * unmount in all cases. This normally happens when a RTCOPY
	     * client has aborted and sent VDQM_DEL_VOLREQ to delete his
	     * volume request before the RTCOPY server has released the
	     * job. 
	     */
	    

	    if ( ptr_tapeDrive->status() != STATUS_UNKNOWN && 
		   		 ptr_tapeDrive->status() != FORCED_UNMOUNT) 
		   	newTapeRequest = ptr_IVdqmService->selectTapeReqForMountedTape(ptr_tapeDrive);
	    	        
	    if ( ptr_tapeDrive->status() == STATUS_UNKNOWN ||
	    		 ptr_tapeDrive->status() == FORCED_UNMOUNT ||
	         newTapeRequest == NULL) {

	        if ( ptr_tapeDrive->status() == FORCED_UNMOUNT ) {
						// "client has requested a forced unmount." message
						castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, 42);	            	          
	        }
	        else if ( ptr_tapeDrive->status() == STATUS_UNKNOWN ) {
						// "tape drive in STATUS_UNKNOWN status. Force unmount!" message
						castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, 43);	            
	        }
	        
 	        if ( newTapeRequest == NULL ) {
          	// "No tape request left for mounted tape" message
			      castor::dlf::Param params[] =
			  			{castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
			  			 castor::dlf::Param("serverName", ptr_tapeDrive->tapeServer()->serverName()),
			  			 castor::dlf::Param("tape vid", tape->vid())};          	
						castor::dlf::dlf_writep(m_cuuid, DLF_LVL_USAGE, 44, 3, params);	            
 	        }
	        	        
	        /*
	         * No, there wasn't any other job for that volume. Tell the
	         * drive to unmount the volume. Put unit in WAIT_FOR_UNMOUNT status
	         * until volume has been unmounted to prevent other
	         * request from being assigned.
	         */
	        ptr_driveRequest->status = VDQM_VOL_UNMOUNT;
	        
	        /**
	         * Switch tape Drive to status WAIT_FOR_UNMOUNT
	         */
	        ptr_tapeDrive->setStatus(WAIT_FOR_UNMOUNT); 
	        
	        if ( newTapeRequest )
	        	delete newTapeRequest;
	    } 
	    else {
	        newTapeRequest->setTapeDrive(ptr_tapeDrive);
	        ptr_tapeDrive->setRunningTapeReq(newTapeRequest);
	        
	        /**
	         * Switch tape Drive to status UNIT_STARTING
	         */
	        ptr_tapeDrive->setStatus(UNIT_STARTING); 
	    }
	} else {
	    /*
	     * There is no volume on unit. Since a RELEASE has
	     * been requested the unit is FREE (no job and no volume
	     * assigned to it. Update status accordingly.
	     * If client specified FORCE_UNMOUNT it means that there is
	     * "something" (unknown to VDQM) mounted. We have to wait
	     * for the unmount before resetting the status.
	   */
	  if ( !(ptr_driveRequest->status & VDQM_FORCE_UNMOUNT) &&
	       ptr_tapeDrive->status() != FORCED_UNMOUNT ) {
      /**
       * Switch tape Drive to status UNIT_UP
       */
      ptr_tapeDrive->setStatus(UNIT_UP);
		}
		
    /*
     * Always tell the client to unmount just in case there was an
     * error and VDQM wasn't notified.
     */
    ptr_driveRequest->status = VDQM_VOL_UNMOUNT;
	}
}


//------------------------------------------------------------------------------
// handleUnitFreeStatus
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveStatusHandler::handleUnitFreeStatus() 
	throw (castor::exception::Exception) {

	TapeRequest* tapeRequest = NULL;
	
	ptr_tapeDrive->setStatus(UNIT_UP);
	
	
	tapeRequest = ptr_tapeDrive->runningTapeReq();
	/*
	 * Dequeue request and free memory allocated for previous 
	 * volume request for this drive
	 */
	if ( tapeRequest != NULL ) {
		
			deleteRepresentation(tapeRequest, m_cuuid);
			delete tapeRequest;
			
			tapeRequest = 0;
			ptr_tapeDrive->setRunningTapeReq(NULL);
	}
  
	/*
	 * Reset job IDs
	 */
	ptr_tapeDrive->setJobID(0);		
}
