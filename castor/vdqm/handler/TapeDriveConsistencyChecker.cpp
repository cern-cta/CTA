/******************************************************************************
 *                      TapeDriveConsistencyChecker.cpp
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
 * @(#)RCSfile: TapeDriveConsistencyChecker.cpp  Revision: 1.0  Release Date: Jul 4, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/ClientIdentification.hpp"
#include "castor/stager/Tape.hpp"

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
#include "TapeDriveConsistencyChecker.hpp"
#include "TapeRequestHandler.hpp"


//To make the code more readable
using namespace castor::vdqm;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveConsistencyChecker::TapeDriveConsistencyChecker(
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
castor::vdqm::handler::TapeDriveConsistencyChecker::~TapeDriveConsistencyChecker() 
	throw() {

}


//------------------------------------------------------------------------------
// checkConsistency
//------------------------------------------------------------------------------
bool castor::vdqm::handler::TapeDriveConsistencyChecker::checkConsistency() 
	throw (castor::exception::Exception) {
	
//	castor::stager::Tape* tape = NULL;
	TapeServer* tapeServer = ptr_tapeDrive->tapeServer();
//	TapeRequest* tapeRequest = NULL;
	
	if (tapeServer == NULL) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "TapeServer is NULL";
  	throw ex;
	}
	
  /*
   * Verify that new unit status is consistent with the
   * current status of the drive.
   */
  if ( ptr_driveRequest->status & VDQM_UNIT_DOWN ) {
    /*
     * Unit configured down. No other status possible.
     * For security this status can only be set from
     * tape server.
     */
    if ( strcmp(ptr_driveRequest->reqhost, tapeServer->serverName().c_str()) != 0 ) {
    	castor::exception::Exception ex(EPERM);
			ex.getMessage() << "TapeDriveConsistencyChecker::checkConsistency(): "
											<< "unauthorized " << ptr_tapeDrive->driveName() << "@"
											<< tapeServer->serverName() << " DOWN from "
											<< ptr_driveRequest->reqhost << std::endl;			
			throw ex;
    }
    
    /*
     * Remove running request (if any). Client will normally retry
     */
    if ( ptr_tapeDrive->status() != UNIT_UP && 
    		 ptr_tapeDrive->status() != UNIT_DOWN) {
    		// If we are here, the tapeDrive info in the db is still "busy" 
        
        // We delete the old Tape request, if any
        deleteOldRequest();
    }
    
    ptr_tapeDrive->setStatus(UNIT_DOWN);
  } 
  
	else if ( ptr_driveRequest->status & VDQM_UNIT_WAITDOWN ) {
    /*
     * Intermediate state until tape daemon confirms that
     * the drive is down. If a volume request is assigned 
     * we cannot put it back in queue until drive is confirmed
     * down since the volume may still be stuck in the unit.
     * First check the drive isn't already down...
     */ 
    if ( ptr_tapeDrive->status() != UNIT_DOWN ) {
    	  // "WAIT DOWN request from tpdaemon client" message
    		castor::dlf::Param param[] =
					{castor::dlf::Param("reqhost", ptr_driveRequest->reqhost)};
				castor::dlf::dlf_writep(m_cuuid, DLF_LVL_USAGE, 38, 1, param);
    
        ptr_tapeDrive->setStatus(UNIT_WAITDOWN);
    }
  } 
  
  else if ( ptr_driveRequest->status & VDQM_UNIT_UP ) {
    /*
     * Unit configured up. Make sure that "down" status is reset.
     * We also mark drive free if input request doesn't specify
     * otherwise.
     * If the input request is a plain config up and the unit was not 
     * down there is a job assigned it probably  means that the tape 
     * server has been rebooted. It does then not make sense
     * to keep the job because client has lost connection long ago and
     * has normally issued a retry. We can therefore remove the 
     * job from queue.
     */
    if ( (ptr_driveRequest->status == VDQM_UNIT_UP ||
          ptr_driveRequest->status == (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
        	ptr_tapeDrive->status() != VDQM_UNIT_DOWN ) {
        		
	    // We delete the old Tape request, if any
      deleteOldRequest();
	    
      ptr_tapeDrive->setStatus(UNIT_UP);
    }
    
//    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_DOWN;
//    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_WAITDOWN;

    if ( ptr_driveRequest->status == VDQM_UNIT_UP )
			ptr_tapeDrive->setStatus(UNIT_UP);
  } 
  else {
  	checkBusyConsistency();
	}
    	
	
	
	return true;		
}


//------------------------------------------------------------------------------
// deleteOldRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::deleteOldRequest() 
	throw (castor::exception::Exception) {
  
  if ( ptr_tapeDrive->runningTapeReq() ) {
  	// "Remove old TapeRequest from db" message
  	castor::dlf::Param param[] =
			{castor::dlf::Param("TapeRequest ID", ptr_tapeDrive->runningTapeReq()->id())};
		castor::dlf::dlf_writep(m_cuuid, DLF_LVL_USAGE, 37, 1, param);

		//Delete the tape request from the db and from the tapeDrive Object
    deleteRepresentation(ptr_tapeDrive->runningTapeReq(), m_cuuid);
    delete (ptr_tapeDrive->runningTapeReq());
    ptr_tapeDrive->setRunningTapeReq(NULL);
  }	
  
  // The Tape is also not needed any more
  if ( ptr_tapeDrive->tape() ) {
	  delete (ptr_tapeDrive->tape());
    ptr_tapeDrive->setTape(NULL);
  }  
  
  ptr_tapeDrive->setJobID(0);
}


//------------------------------------------------------------------------------
// checkBusyConsistency
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::checkBusyConsistency() 
	throw (castor::exception::Exception) {

  if ( ptr_tapeDrive->status() == UNIT_DOWN ) {
      /*
       * Unit must be up before anything else is allowed
       */
     	castor::exception::Exception ex(EVQUNNOTUP);
			ex.getMessage() << "TapeDriveConsistencyChecker::checkConsistency(): "
											<< "unit is not UP" << std::endl;			
			throw ex;
  } 

  if ( ptr_driveRequest->status & VDQM_UNIT_BUSY ) {
      /*
       * Consistency check
       */
      if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
       	castor::exception::Exception ex(EVQBADSTAT);
				ex.getMessage() << "TapeDriveConsistencyChecker::checkConsistency(): "
												<< "bad status from tpdaemon! FREE + BUSY doesn't fit together" 
												<< std::endl;			
				throw ex;
      }

			//TODO: Umwandler schreiben!!!
//        drvrec->drv.status = ptr_driveRequest->status;
  }
    
//    else if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
//        /*
//         * Cannot free an assigned unit, it must be released first
//         */
//        if ( !(ptr_driveRequest->status & VDQM_UNIT_RELEASE) &&
//            (drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
//            log(LOG_ERR,"vdqm_NewDrvReq(): cannot free assigned unit %s@%s, jobID=%d\n",
//                drvrec->drv.drive,drvrec->drv.server,drvrec->drv.jobID);
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQBADSTAT);
//            return(-1);
//        }
//        /*
//         * Cannot free an unit with tape mounted
//         */
//        if ( !(ptr_driveRequest->status & VDQM_VOL_UNMOUNT) &&
//            (*drvrec->drv.volid != '\0') ) {
//            log(LOG_ERR,"vdqm_NewDrvReq(): cannot free unit with tape mounted, volid=%s\n",
//                drvrec->drv.volid);
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQBADSTAT);
//            return(-1);
//        }
//    	} 
//    
//    	else {
//        /*
//         * If unit is busy and being assigned the VolReqIDs must
//         * be the same. If so, assign the jobID (normally the
//         * process ID of the RTCOPY process on the tape server).
//         */
//        if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
//             (ptr_driveRequest->status & VDQM_UNIT_ASSIGN) ) {                
//            if (drvrec->drv.VolReqID != ptr_driveRequest->VolReqID){
//                log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent VolReqIDs (%d,%d) on ASSIGN\n",
//                    ptr_driveRequest->VolReqID,drvrec->drv.VolReqID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADID);
//                return(-1);
//            } else {
//                drvrec->drv.jobID = ptr_driveRequest->jobID;
//                log(LOG_INFO,"vdqm_NewDrvReq() assign VolReqID %d to jobID %d\n",
//                    ptr_driveRequest->VolReqID,ptr_driveRequest->jobID);
//            }
//        }
//        /*
//         * If unit is busy with a running job the job IDs must be same
//         */
//        if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
//             !(drvrec->drv.status & VDQM_UNIT_RELEASE) &&
//            (ptr_driveRequest->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
//                               VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) &&
//            (drvrec->drv.jobID != ptr_driveRequest->jobID) ) {
//            log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent jobIDs (%d,%d)\n",
//                ptr_driveRequest->jobID,drvrec->drv.jobID);
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQBADID);
//            return(-1);
//        }
//        
//        /*
//         * Prevent operations on a free unit. A job must have been
//         * started and unit marked busy before it can be used.
//         * 22/11/1999: we change this so that a free unit can be
//         *             assigned. The reason is that a local job may
//         *             running on the tape server (e.g. tplabel) may
//         *             want to run without starting a rtcopy job.
//         */
//        if ( !(ptr_driveRequest->status & VDQM_UNIT_BUSY) &&
//            (drvrec->drv.status & VDQM_UNIT_FREE) &&
//            (ptr_driveRequest->status & (VDQM_UNIT_RELEASE |
//            VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) ) {
//            log(LOG_ERR,"vdqm_NewDrvReq(): status 0x%x requested on FREE drive\n",
//                ptr_driveRequest->status);
//            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//            FreeDgnContext(&dgn_context);
//            vdqm_SetError(EVQBADSTAT);
//            return(-1);
//        }
//        if ( ptr_driveRequest->status & VDQM_UNIT_ASSIGN ) {
//            /*
//             * Check whether unit was already assigned. If so, the
//             * volume request must be identical
//             */
//            if ( (drvrec->drv.status & VDQM_UNIT_ASSIGN) &&
//                (drvrec->drv.jobID != ptr_driveRequest->jobID) ) {
//                log(LOG_ERR,"vdqm_NewDrvReq(): attempt to re-assign ID=%d to an unit assigned to ID=%d\n",
//                    drvrec->drv.jobID,ptr_driveRequest->jobID);
//                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                FreeDgnContext(&dgn_context);
//                vdqm_SetError(EVQBADID);
//                return(-1);
//            }
//            /*
//             * If the unit was free, we set the new jobID. This is
//             * local assign bypassing the normal VDQM logic (see comment
//             * above). There is no VolReqID since VDQM is bypassed.
//             */
//            if ( (drvrec->drv.status & VDQM_UNIT_FREE) ) {
//                /*
//                 * We only allow this for local requests!
//                 */
//                if ( strcmp(ptr_driveRequest->reqhost,drvrec->drv.server) != 0 ) {
//                    log(LOG_ERR,"vdqm_NewDrvRequest(): unauthorized %s@%s local assign from %s\n",
//                    ptr_driveRequest->drive,ptr_driveRequest->server,ptr_driveRequest->reqhost);
//                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
//                    FreeDgnContext(&dgn_context);
//                    vdqm_SetError(EPERM);
//                    return(-1);
//                }
//                log(LOG_INFO,"vdqm_NewDrvRequest() local assign %s@%s to jobID %d\n",
//                    ptr_driveRequest->drive,ptr_driveRequest->server,ptr_driveRequest->jobID);
//                drvrec->drv.jobID = ptr_driveRequest->jobID;
//            }
//        }
//        /*
//         * VDQM_VOL_MOUNT and VDQM_VOL_UNMOUNT are not persistent unit 
//         * status values. Their purpose is twofold: 1) input - update 
//         * the volid field in the drive record (both MOUNT and UNMOUNT)
//         * and 2) output - tell client to unmount or keep volume mounted
//         * in case of deferred unmount (UNMOUNT only). 
//         *
//         * VDQM_UNIT_MBCOUNT is not a persistent unit status value.
//         * It request update of drive statistics.
//         */
//        if ( drvrec->drv.status & VDQM_UNIT_UP )
//            drvrec->drv.status |= ptr_driveRequest->status & 
//                                  (~VDQM_VOL_MOUNT & ~VDQM_VOL_UNMOUNT &
//                                   ~VDQM_UNIT_MBCOUNT );
//    }
//		drvrec->update = 1;	
}
