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
#include "castor/exception/Exception.hpp"
#include "castor/stager/ClientIdentification.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/vdqm/ExtendedDeviceGroup.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeServer.hpp"

#include <net.h>
#include <vdqm.h>
#include <vdqm_constants.h>
 
// Local Includes
#include "BaseRequestHandler.hpp"
#include "TapeDriveStatusHandler.hpp"


//To make the code more readable
using namespace castor::vdqm;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveStatusHandler::TapeDriveStatusHandler(
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
//        if ( (ptr_driveRequest->status & VDQM_VOL_UNMOUNT) ) {
//            *drvrec->drv.volid = '\0';
//            drvrec->drv.mode = -1;
//            /*
//             * Volume has been unmounted. Reset release status (if set).
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
//            /*
//             * If it was an forced unmount due to an error we can reset
//             * the ERROR status at this point.
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ERROR;
//            /*
//             * If the client forced an unmount with the release we can
//             * reset the FORCE_UNMOUNT here.
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_FORCE_UNMOUNT;
//            /*
//             * We should also reset UNMOUNT status in the request so that
//             * we tell the drive to unmount twice
//             */
//            ptr_driveRequest->status = ptr_driveRequest->status & ~VDQM_VOL_UNMOUNT;
//            /*
//             * Set status to FREE if there is no job assigned to the unit
//             */
//            if ( drvrec->vol == NULL ) {
//                drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_BUSY;
//                drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
//                drvrec->drv.status = drvrec->drv.status | VDQM_UNIT_FREE;
//            }
//        }
//        if ((ptr_driveRequest->status & VDQM_UNIT_RELEASE) &&
//            !(ptr_driveRequest->status & VDQM_UNIT_FREE) ) { 
//            /*
//             * Reset assign status (drive is being released!).
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
//            /*
//             * Reset request and job IDs
//             */
//            drvrec->drv.VolReqID = 0;
//            drvrec->drv.jobID = 0;
//            /*
//             * Delete from queue and free memory allocated for 
//             * previous volume request on this drive. 
//             */
//            if ( drvrec->vol != NULL ) {
//                rc = DelVolRecord(dgn_context,drvrec->vol);
//                free(drvrec->vol);
//            }
//            drvrec->vol = NULL;
//
//            if ( *drvrec->drv.volid != '\0' ) {
//                /*
//                 * Consistency check: if input request contains a volid it
//                 * should correspond to the one in the drive record. This
//                 * is not fatal but should be logged.
//                 */
//                if ( *ptr_driveRequest->volid != '\0' && 
//                     strcmp(ptr_driveRequest->volid,drvrec->drv.volid) ) {
//                   log(LOG_ERR,"vdqm_NewDrvReq(): drive %s@%s inconsistent release on %s (should be %s), jobID=%d\n",
//                       drvrec->drv.drive,drvrec->drv.server,
//                       ptr_driveRequest->volid,drvrec->drv.volid,drvrec->drv.jobID);
//                }
//                /*
//                 * Fill in current volid in case we need to request an unmount.
//                 */
//                if ( *ptr_driveRequest->volid == '\0' ) strcpy(ptr_driveRequest->volid,drvrec->drv.volid);
//                /*
//                 * If a volume is mounted but current job ended: check if there
//                 * are any other valid request for the same volume. The volume
//                 * can only be re-used on this unit if the modes (R/W) are the 
//                 * same. If client indicated an error or forced unmount the
//                 * will remain with that state until a VOL_UNMOUNT is received
//                 * and both drive and volume cannot be reused until then.
//                 * If the drive status was UNKNOWN at entry we must force an
//                 * unmount in all cases. This normally happens when a RTCOPY
//                 * client has aborted and sent VDQM_DEL_VOLREQ to delete his
//                 * volume request before the RTCOPY server has released the
//                 * job. 
//                 */
//                rc = 0;
//                if (!unknown && !(drvrec->drv.status & (VDQM_UNIT_ERROR |
//                                                        VDQM_FORCE_UNMOUNT)))
//                    rc = AnyVolRecForMountedVol(dgn_context,drvrec,&volrec);
//                if ( (drvrec->drv.status & VDQM_UNIT_ERROR) ||
//                     unknown || rc == -1 || volrec == NULL || 
//                     volrec->vol.mode != drvrec->drv.mode ) {
//                    if ( drvrec->drv.status & VDQM_UNIT_ERROR ) 
//                        log(LOG_ERR,"vdqm_NewDrvReq(): unit in error status. Force unmount!\n");
//                    if ( drvrec->drv.status & VDQM_FORCE_UNMOUNT )
//                        log(LOG_ERR,"vdqm_NewDrvReq(): client requests forced unmount\n");
//                    if ( rc == -1 ) 
//                        log(LOG_ERR,"vdqm_NewDrvReq(): AnyVolRecForVolid() returned error\n");
//                    if ( unknown )
//                        log(LOG_ERR,"vdqm_NewDrvReq(): drive in UNKNOWN status. Force unmount!\n");
//                    /*
//                     * No, there wasn't any other job for that volume. Tell the
//                     * drive to unmount the volume. Put unit in unknown status
//                     * until volume has been unmounted to prevent other
//                     * request from being assigned.
//                     */
//                    ptr_driveRequest->status  = VDQM_VOL_UNMOUNT;
//                    drvrec->drv.status |= VDQM_UNIT_UNKNOWN; 
//                    volrec = NULL;
//                } else {
//                    volrec->drv = drvrec;
//                    drvrec->vol = volrec;
//                    volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
//                    drvrec->drv.VolReqID = volrec->vol.VolReqID;
//                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
//                }
//            } else {
//                /*
//                 * There is no volume on unit. Since a RELEASE has
//                 * been requested the unit is FREE (no job and no volume
//                 * assigned to it. Update status accordingly.
//                 * If client specified FORCE_UNMOUNT it means that there is
//                 * "something" (unknown to VDQM) mounted. We have to wait
//                 * for the unmount before resetting the status.
//                 */
//                if ( !(ptr_driveRequest->status & VDQM_FORCE_UNMOUNT) &&
//                     !(drvrec->drv.status & VDQM_FORCE_UNMOUNT) ) {
//                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_BUSY;
//                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
//                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
//                    drvrec->drv.status = drvrec->drv.status | VDQM_UNIT_FREE;
//                }
//                /*
//                 * Always tell the client to unmount just in case there was an
//                 * error and VDQM wasn't notified.
//                 */
//                ptr_driveRequest->status  = VDQM_VOL_UNMOUNT;
//            }
//        } 
//        /*
//         * If unit is free, reset dynamic data in drive record
//         */
//        if ( (ptr_driveRequest->status & VDQM_UNIT_FREE) ) {
//            /*
//             * Reset status to reflect that drive is not assigned and nothing is mounted.
//             */
//            drvrec->drv.status = (drvrec->drv.status | VDQM_UNIT_FREE) &
//                (~VDQM_UNIT_ASSIGN & ~VDQM_UNIT_RELEASE & ~VDQM_UNIT_BUSY);
//            
//            /*
//             * Dequeue request and free memory allocated for previous 
//             * volume request for this drive
//             */
//            if ( drvrec->vol != NULL ) {
//                rc = DelVolRecord(dgn_context,drvrec->vol);
//                if ( rc == -1 ) {
//                    log(LOG_ERR,"vdqm_NewDrvReq(): DelVolRecord() returned error\n");
//                } else {
//                    free(drvrec->vol);
//                }
//                drvrec->vol = NULL;
//            }
//            /*
//             * Reset request and job IDs
//             */
//            drvrec->drv.VolReqID = 0;
//            drvrec->drv.jobID = 0;
//        }
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
		} 
//	else {
//        /*
//         * If drive is down, report error for any requested update.
//         */
//        if ( ptr_driveRequest->status & (VDQM_UNIT_FREE | VDQM_UNIT_ASSIGN |
//            VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE | VDQM_VOL_MOUNT |
//            VDQM_VOL_UNMOUNT ) ) {
//            log(LOG_ERR,"vdqm_NewDrvReq(): drive is DOWN\n");
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQUNNOTUP);
//            return(-1);
//        }
//    }
//    FreeDgnContext(&dgn_context);    
//    
//    /* At this point, backup the comple queue to a file */
//    if (new_drive_added) {
//         log(LOG_DEBUG,"vdqm_NewDrvReq(): Update drive config file\n");
//         if (vdqm_save_queue() < 0) {
//             log(LOG_ERR, "Could not save drive list\n");
//         }         
//    }

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
	mountedTape = ptr_IVdqmService->selectTape(ptr_driveRequest->volid);
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
