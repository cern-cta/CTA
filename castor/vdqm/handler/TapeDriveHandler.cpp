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
#include <vdqm.h>
#include <vdqm_constants.h>
#include <vector>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/ClientIdentification.hpp"

#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/TapeRequest.hpp"

// Local Includes
#include "BaseRequestHandler.hpp"
#include "TapeDriveHandler.hpp"
#include "TapeDriveConsistencyChecker.hpp" // We are a friend of him!

//To make the code more readable
using namespace castor::vdqm;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::TapeDriveHandler(vdqmHdr_t* header, 
												 vdqmDrvReq_t* driveRequest, Cuuid_t cuuid) throw() {
	m_cuuid = cuuid;
	
	if ( header == NULL || driveRequest == NULL ) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "One of the arguments is NULL";
  	throw ex;
	}
	else {
		ptr_header = header;
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
//    dgn_element_t *dgn_context = NULL;
//    vdqm_volrec_t *volrec;
//    vdqm_drvrec_t *drvrec; //The equivalent to the TapeDrive object
//    int rc,unknown;
//    char status_string[256];
//    int new_drive_added = 0;
    
	TapeServer* tapeServer = NULL;
	TapeDrive* tapeDrive = NULL;
  

	//"The parameters of the old vdqm DrvReq Request" message
  castor::dlf::Param params[] =
  	{castor::dlf::Param("dedicate", ptr_driveRequest->dedicate),
     castor::dlf::Param("dgn", ptr_driveRequest->dgn),
     castor::dlf::Param("drive", ptr_driveRequest->drive),
     castor::dlf::Param("errcount", ptr_driveRequest->errcount),
     castor::dlf::Param("gid", ptr_driveRequest->gid),
     castor::dlf::Param("is_gid", ptr_driveRequest->is_gid),
     castor::dlf::Param("is_name", ptr_driveRequest->is_name),
     castor::dlf::Param("is_uid", ptr_driveRequest->is_uid),
     castor::dlf::Param("jobID", ptr_driveRequest->jobID),
     castor::dlf::Param("MBtransf", ptr_driveRequest->MBtransf),
     castor::dlf::Param("mode", ptr_driveRequest->mode),
     castor::dlf::Param("name", ptr_driveRequest->name),
     castor::dlf::Param("newdedicate", ptr_driveRequest->newdedicate),
     castor::dlf::Param("no_age", ptr_driveRequest->no_age),
     castor::dlf::Param("no_date", ptr_driveRequest->no_date),
     castor::dlf::Param("no_gid", ptr_driveRequest->no_gid),
     castor::dlf::Param("no_host", ptr_driveRequest->no_host),
     castor::dlf::Param("no_mode", ptr_driveRequest->no_mode),
     castor::dlf::Param("no_name", ptr_driveRequest->no_name),
     castor::dlf::Param("no_time", ptr_driveRequest->no_time),
     castor::dlf::Param("no_uid", ptr_driveRequest->no_uid),
		 castor::dlf::Param("no_vid", ptr_driveRequest->no_vid),
 		 castor::dlf::Param("recvtime", ptr_driveRequest->recvtime),
		 castor::dlf::Param("reqhost", ptr_driveRequest->reqhost),
		 castor::dlf::Param("resettime", ptr_driveRequest->resettime),
		 castor::dlf::Param("server", ptr_driveRequest->server),
		 castor::dlf::Param("status", ptr_driveRequest->status),
		 castor::dlf::Param("TotalMB", ptr_driveRequest->TotalMB),
		 castor::dlf::Param("uid", ptr_driveRequest->uid),
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
  
  //TODO: Save tapeDrive into db!

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




//    /*
//     * Verify that new unit status is consistent with the
//     * current status of the drive.
//     */
//    if ( ptr_driveRequest->status & VDQM_UNIT_DOWN ) {
//        /*
//         * Unit configured down. No other status possible.
//         * For security this status can only be set from
//         * tape server.
//         */
//        if ( strcmp(ptr_driveRequest->reqhost,drvrec->drv.server) != 0 ) {
//            log(LOG_ERR,"vdqm_NewDrvRequest(): unauthorized %s@%s DOWN from %s\n",
//                ptr_driveRequest->drive,ptr_driveRequest->server,ptr_driveRequest->reqhost);
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EPERM);
//            return(-1);
//        }
//        /*
//         * Remove running request (if any). Client will normally retry
//         */
//        if ( drvrec->drv.status & VDQM_UNIT_BUSY ) {
//            volrec = drvrec->vol;
//            if ( volrec != NULL ) {
//                log(LOG_INFO,"vdqm_NewDrvRequest(): Remove old volume record, id=%d\n",
//                    volrec->vol.VolReqID);
//                volrec->drv = NULL;
//                DelVolRecord(dgn_context,volrec);
//            }
//            drvrec->vol = NULL;
//            *drvrec->drv.volid = '\0';
//            drvrec->drv.VolReqID = 0;
//            drvrec->drv.jobID = 0;
//        }
//        drvrec->drv.status = VDQM_UNIT_DOWN;
//		drvrec->update = 1;
//    } else if ( ptr_driveRequest->status & VDQM_UNIT_WAITDOWN ) {
//        /*
//         * Intermediate state until tape daemon confirms that
//         * the drive is down. If a volume request is assigned 
//         * we cannot put it back in queue until drive is confirmed
//         * down since the volume may still be stuck in the unit.
//         * First check the drive isn't already down...
//         */ 
//        if ( !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
//            log(LOG_INFO,"vdqm_NewDrvRequest(): WAIT DOWN request from %s\n",
//                ptr_driveRequest->reqhost); 
//            drvrec->drv.status = VDQM_UNIT_WAITDOWN;
//			drvrec->update = 1;
//        }
//    } else if ( ptr_driveRequest->status & VDQM_UNIT_UP ) {
//        /*
//         * Unit configured up. Make sure that "down" status is reset.
//         * We also mark drive free if input request doesn't specify
//         * otherwise.
//         * If the input request is a plain config up and the unit was not 
//         * down there is a job assigned it probably  means that the tape 
//         * server has been rebooted. It does then not make sense
//         * to keep the job because client has lost connection long ago and
//         * has normally issued a retry. We can therefore remove the 
//         * job from queue.
//         */
//        if ( (ptr_driveRequest->status == VDQM_UNIT_UP ||
//              ptr_driveRequest->status == (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
//            !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
//            if ( drvrec->vol != NULL || *drvrec->drv.volid != '\0' ) {
//                volrec = drvrec->vol;
//                if ( volrec != NULL ) {
//                    log(LOG_INFO,"vdqm_NewDrvRequest(): Remove old volume record, id=%d\n",
//                        volrec->vol.VolReqID);
//                    DelVolRecord(dgn_context,volrec);
//                    drvrec->vol = NULL;
//                }
//                *drvrec->drv.volid = '\0';
//                drvrec->drv.VolReqID = 0;
//                drvrec->drv.jobID = 0;
//            } 
//            drvrec->drv.status = VDQM_UNIT_UP | VDQM_UNIT_FREE;
//        }
//        drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_DOWN;
//        drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_WAITDOWN;
//
//        if ( ptr_driveRequest->status == VDQM_UNIT_UP )
//            drvrec->drv.status |= VDQM_UNIT_FREE;
//        drvrec->drv.status |= ptr_driveRequest->status;
//		drvrec->update = 1;
//    } else {
//        if ( drvrec->drv.status & VDQM_UNIT_DOWN ) {
//            /*
//             * Unit must be up before anything else is allowed
//             */
//            log(LOG_ERR,"vdqm_NewDrvReq(): unit is not UP\n");
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQUNNOTUP);
//            return(-1);
//        } 
//        /*
//         * If the unit is not DOWN, it must be UP! Make sure it's the case.
//         * This is to facilitate the repair after a VDQM server crash.
//         */
//        if ( (drvrec->drv.status & VDQM_UNIT_UP) == 0 ) {
//			    drvrec->drv.status |= VDQM_UNIT_UP;
//			    drvrec->update = 1;
//		    }
//
//        if ( ptr_driveRequest->status & VDQM_UNIT_BUSY ) {
//            /*
//             * Consistency check
//             */
//            if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADSTAT);
//                return(-1);
//            }
//            drvrec->drv.status = ptr_driveRequest->status;
//            /*
//             * Unit marked "busy". Reset free status.
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_FREE;
//        } else if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
//            /*
//             * Cannot free an assigned unit, it must be released first
//             */
//            if ( !(ptr_driveRequest->status & VDQM_UNIT_RELEASE) &&
//                (drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): cannot free assigned unit %s@%s, jobID=%d\n",
//                    drvrec->drv.drive,drvrec->drv.server,drvrec->drv.jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADSTAT);
//                return(-1);
//            }
//            /*
//             * Cannot free an unit with tape mounted
//             */
//            if ( !(ptr_driveRequest->status & VDQM_VOL_UNMOUNT) &&
//                (*drvrec->drv.volid != '\0') ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): cannot free unit with tape mounted, volid=%s\n",
//                    drvrec->drv.volid);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADSTAT);
//                return(-1);
//            }
//        } else {
//            /*
//             * If unit is busy and being assigned the VolReqIDs must
//             * be the same. If so, assign the jobID (normally the
//             * process ID of the RTCOPY process on the tape server).
//             */
//            if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
//                 (ptr_driveRequest->status & VDQM_UNIT_ASSIGN) ) {                
//                if (drvrec->drv.VolReqID != ptr_driveRequest->VolReqID){
//                    log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent VolReqIDs (%d,%d) on ASSIGN\n",
//                        ptr_driveRequest->VolReqID,drvrec->drv.VolReqID);
//                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                    FreeDgnContext(&dgn_context);
//                    vdqm_SetError(EVQBADID);
//                    return(-1);
//                } else {
//                    drvrec->drv.jobID = ptr_driveRequest->jobID;
//                    log(LOG_INFO,"vdqm_NewDrvReq() assign VolReqID %d to jobID %d\n",
//                        ptr_driveRequest->VolReqID,ptr_driveRequest->jobID);
//                }
//            }
//            /*
//             * If unit is busy with a running job the job IDs must be same
//             */
//            if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
//                 !(drvrec->drv.status & VDQM_UNIT_RELEASE) &&
//                (ptr_driveRequest->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
//                                   VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) &&
//                (drvrec->drv.jobID != ptr_driveRequest->jobID) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent jobIDs (%d,%d)\n",
//                    ptr_driveRequest->jobID,drvrec->drv.jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADID);
//                return(-1);
//            }
//            
//            /*
//             * Prevent operations on a free unit. A job must have been
//             * started and unit marked busy before it can be used.
//             * 22/11/1999: we change this so that a free unit can be
//             *             assigned. The reason is that a local job may
//             *             running on the tape server (e.g. tplabel) may
//             *             want to run without starting a rtcopy job.
//             */
//            if ( !(ptr_driveRequest->status & VDQM_UNIT_BUSY) &&
//                (drvrec->drv.status & VDQM_UNIT_FREE) &&
//                (ptr_driveRequest->status & (VDQM_UNIT_RELEASE |
//                VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): status 0x%x requested on FREE drive\n",
//                    ptr_driveRequest->status);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADSTAT);
//                return(-1);
//            }
//            if ( ptr_driveRequest->status & VDQM_UNIT_ASSIGN ) {
//                /*
//                 * Check whether unit was already assigned. If so, the
//                 * volume request must be identical
//                 */
//                if ( (drvrec->drv.status & VDQM_UNIT_ASSIGN) &&
//                    (drvrec->drv.jobID != ptr_driveRequest->jobID) ) {
//                    log(LOG_ERR,"vdqm_NewDrvReq(): attempt to re-assign ID=%d to an unit assigned to ID=%d\n",
//                        drvrec->drv.jobID,ptr_driveRequest->jobID);
//                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                    FreeDgnContext(&dgn_context);
//                    vdqm_SetError(EVQBADID);
//                    return(-1);
//                }
//                /*
//                 * If the unit was free, we set the new jobID. This is
//                 * local assign bypassing the normal VDQM logic (see comment
//                 * above). There is no VolReqID since VDQM is bypassed.
//                 */
//                if ( (drvrec->drv.status & VDQM_UNIT_FREE) ) {
//                    /*
//                     * We only allow this for local requests!
//                     */
//                    if ( strcmp(ptr_driveRequest->reqhost,drvrec->drv.server) != 0 ) {
//                        log(LOG_ERR,"vdqm_NewDrvRequest(): unauthorized %s@%s local assign from %s\n",
//                        ptr_driveRequest->drive,ptr_driveRequest->server,ptr_driveRequest->reqhost);
//                        if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                        FreeDgnContext(&dgn_context);
//                        vdqm_SetError(EPERM);
//                        return(-1);
//                    }
//                    log(LOG_INFO,"vdqm_NewDrvRequest() local assign %s@%s to jobID %d\n",
//                        ptr_driveRequest->drive,ptr_driveRequest->server,ptr_driveRequest->jobID);
//                    drvrec->drv.jobID = ptr_driveRequest->jobID;
//                }
//            }
//            /*
//             * VDQM_VOL_MOUNT and VDQM_VOL_UNMOUNT are not persistent unit 
//             * status values. Their purpose is twofold: 1) input - update 
//             * the volid field in the drive record (both MOUNT and UNMOUNT)
//             * and 2) output - tell client to unmount or keep volume mounted
//             * in case of deferred unmount (UNMOUNT only). 
//             *
//             * VDQM_UNIT_MBCOUNT is not a persistent unit status value.
//             * It request update of drive statistics.
//             */
//            if ( drvrec->drv.status & VDQM_UNIT_UP )
//                drvrec->drv.status |= ptr_driveRequest->status & 
//                                      (~VDQM_VOL_MOUNT & ~VDQM_VOL_UNMOUNT &
//                                       ~VDQM_UNIT_MBCOUNT );
//        }
//		drvrec->update = 1;
//    }
//    




//------------------------------------------------------------------------------



//    volrec = NULL;
//    if ( drvrec->drv.status & VDQM_UNIT_UP ) {
//        if ( ptr_driveRequest->status & VDQM_UNIT_ASSIGN ) {
//            /*
//             * Unit assigned (reserved).
//             */
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
//            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_FREE;
//            drvrec->drv.status |= VDQM_UNIT_BUSY;
//        }
//        if ( (ptr_driveRequest->status & VDQM_UNIT_MBCOUNT) ) {
//            /*
//             * Update TotalMB counter. Since this request is sent by
//             * RTCOPY rather than the tape daemon we cannot yet reset
//             * unknown status if it was previously set.
//             */
//            drvrec->drv.MBtransf = ptr_driveRequest->MBtransf;
//            drvrec->drv.TotalMB += ptr_driveRequest->MBtransf;
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//        }
//        if ( (ptr_driveRequest->status & VDQM_UNIT_ERROR) ) {
//            /*
//             * Update error counter.
//             */
//            drvrec->drv.errcount++;
//        }
//        if ( (ptr_driveRequest->status & VDQM_VOL_MOUNT) ) {
//            /*
//             * A mount volume request. The unit must first have been assigned.
//             */
//            if ( !(drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): mount request of %s for jobID %d on non-ASSIGNED unit\n",
//                    ptr_driveRequest->volid,ptr_driveRequest->jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQNOTASS);
//                return(-1);
//            }
//            if ( *ptr_driveRequest->volid == '\0' ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): mount request with empty VOLID for jobID %d\n",
//                    drvrec->drv.jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADVOLID);
//                return(-1);
//            }
//            /*
//             * Make sure that requested volume and assign volume record are the same
//             */
//            if ( drvrec->vol != NULL && strcmp(drvrec->vol->vol.volid,ptr_driveRequest->volid) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent mount %s (should be %s) for jobID %d\n",
//                    ptr_driveRequest->volid,drvrec->vol->vol.volid,ptr_driveRequest->jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADVOLID);
//                return(-1);
//            }
//            /*
//             * If there are no assigned volume request it means that this
//             * is a local request. Make sure that server and reqhost are
//             * the same and that the volume is free.
//             */
//            if ( drvrec->vol == NULL ) {
//                if ( strcmp(drvrec->drv.server,ptr_driveRequest->reqhost) != 0 ) {
//                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                    FreeDgnContext(&dgn_context);
//                    vdqm_SetError(EPERM);
//                    return(-1);
//                 }
//                 if ( VolInUse(dgn_context,ptr_driveRequest->volid) ||
//                      VolMounted(dgn_context,ptr_driveRequest->volid) ) {
//                     if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                     FreeDgnContext(&dgn_context);
//                     vdqm_SetError(EBUSY);
//                     return(-1);
//                 }
//                 drvrec->drv.mode = -1; /* Mode is unknown */
//            } else drvrec->drv.mode = drvrec->vol->vol.mode;
//            strcpy(drvrec->drv.volid,ptr_driveRequest->volid);
//            drvrec->drv.status |= VDQM_UNIT_BUSY;
//            /*
//             * Update usage counter
//             */
//            drvrec->drv.usecount++;
//        }
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
//    } else {
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
//
//    return(0);
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
		TapeDrive* tapeDrive = NULL;
		
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
			// "Create new TapeDrive in DB" message
		  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_USAGE, 34);
			
			/**
			 * The tape drive does not exist, so we just create it!
			 */
      TapeDrive* tapeDrive = new castor::vdqm::TapeDrive();
      /**
       * We need then also an object to store the client related informations
       */
      castor::stager::ClientIdentification* client;

      tapeDrive->setDriveName(ptr_driveRequest->drive);
      tapeDrive->setTapeServer(tapeServer);
      tapeDrive->setDedicate(ptr_driveRequest->dedicate);
      tapeDrive->setErrcount(ptr_driveRequest->errcount);
      tapeDrive->setIs_gid(ptr_driveRequest->is_gid);
      tapeDrive->setIs_name(ptr_driveRequest->is_name);
      tapeDrive->setIs_uid(ptr_driveRequest->is_uid);
      tapeDrive->setJobID(ptr_driveRequest->jobID);
      tapeDrive->setModificationTime(time(NULL));
      tapeDrive->setNewDedicate(ptr_driveRequest->newdedicate);
      tapeDrive->setNo_age(ptr_driveRequest->no_age);
      tapeDrive->setNo_date(ptr_driveRequest->no_date);
      tapeDrive->setNo_gid(ptr_driveRequest->no_gid);
      tapeDrive->setNo_host(ptr_driveRequest->no_host);
      tapeDrive->setNo_mode(ptr_driveRequest->no_mode);
      tapeDrive->setNo_name(ptr_driveRequest->no_name);
      tapeDrive->setNo_time(ptr_driveRequest->no_time);      
      tapeDrive->setNo_uid(ptr_driveRequest->no_uid);      
      tapeDrive->setNo_vid(ptr_driveRequest->no_vid);
      tapeDrive->setResettime(ptr_driveRequest->resettime);
      tapeDrive->setTotalMB(ptr_driveRequest->TotalMB);
      tapeDrive->setTransferredMB(ptr_driveRequest->MBtransf);
      tapeDrive->setUsecount(ptr_driveRequest->usecount);			
      
      client = new castor::stager::ClientIdentification();
      client->setEgid(ptr_driveRequest->gid);
      client->setEuid(ptr_driveRequest->uid);
      client->setMachine(ptr_driveRequest->reqhost);
      client->setMagic(ptr_header->magic);
      client->setUserName(ptr_driveRequest->name);
      
      //Add the client informations to the tapeDrive
      tapeDrive->setClient(client);
      
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

  castor::stager::ClientIdentification* client;

//  ptr_driveRequest->dedicate = (char*)tapeDrive->dedicate();
  strcpy(ptr_driveRequest->dedicate, tapeDrive->dedicate().c_str());
  ptr_driveRequest->errcount = tapeDrive->errcount();
  ptr_driveRequest->is_gid = tapeDrive->is_gid();
  ptr_driveRequest->is_name = tapeDrive->is_name();
  ptr_driveRequest->is_uid = tapeDrive->is_uid();
  ptr_driveRequest->jobID = tapeDrive->jobID();
  strcpy(ptr_driveRequest->newdedicate, tapeDrive->newDedicate().c_str());
  ptr_driveRequest->no_age = tapeDrive->no_age();
  ptr_driveRequest->no_date = tapeDrive->no_date();
  ptr_driveRequest->no_gid = tapeDrive->no_gid();
  ptr_driveRequest->no_host = tapeDrive->no_host();
  ptr_driveRequest->no_mode = tapeDrive->no_mode();
  ptr_driveRequest->no_name = tapeDrive->no_name();
  ptr_driveRequest->no_time = tapeDrive->no_time();      
  ptr_driveRequest->no_uid = tapeDrive->no_uid();      
  ptr_driveRequest->no_vid = tapeDrive->no_vid();
  ptr_driveRequest->resettime = tapeDrive->resettime();
  ptr_driveRequest->TotalMB = tapeDrive->totalMB();
  ptr_driveRequest->MBtransf = tapeDrive->transferredMB();
  ptr_driveRequest->usecount = tapeDrive->usecount();			
  
	client = tapeDrive->client();
  ptr_driveRequest->gid = client->egid();
  ptr_driveRequest->uid = client->euid();
  strcpy(ptr_driveRequest->reqhost, client->machine().c_str());
  strcpy(ptr_driveRequest->name, client->userName().c_str());
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
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}
					break;
		case UNIT_STARTING:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_STARTING")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case UNIT_ASSIGNED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_ASSIGNED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case UNIT_RELEASED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_RELEASED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case VOL_MOUNTED:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "VOL_MOUNTED")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case FORCED_UNMOUNT:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "FORCED_UNMOUNT")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case UNIT_DOWN:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "UNIT_DOWN")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}		
					break;
		case WAIT_FOR_UNMOUNT:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "WAIT_FOR_UNMOUNT")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}	
		case STATUS_UNKNOWN:
					{
						castor::dlf::Param param1[] =
	  					{castor::dlf::Param("status", "STATUS_UNKNOWN")};
  					castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, 36, 1, param);
					}						
					break;
		default:
					castor::exception::InvalidArgument ex;
			  	ex.getMessage() << "The tapeDrive is in a wrong status" << std::endl;
  				throw ex;
	}
}
