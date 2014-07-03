/******************************************************************************
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
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmTape.hpp"
#include "castor/vdqm/handler/TapeDriveConsistencyChecker.hpp"
#include <string.h>

#include <net.h>
#include <vdqm_constants.h>
 

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveConsistencyChecker::TapeDriveConsistencyChecker(
  castor::vdqm::TapeDrive *const tapeDrive, 
  vdqmDrvReq_t *const driveRequest, const Cuuid_t cuuid)
   : ptr_tapeDrive(tapeDrive),
  ptr_driveRequest(driveRequest), m_cuuid(cuuid) {
  
  if ( tapeDrive == NULL || driveRequest == NULL ) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "One of the arguments is NULL";
    throw ex;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveConsistencyChecker::
  ~TapeDriveConsistencyChecker() throw() {

}


//------------------------------------------------------------------------------
// checkConsistency
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::checkConsistency() 
   {
  
  TapeServer* tapeServer = ptr_tapeDrive->tapeServer();
  
  if (tapeServer == NULL) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "TapeServer is NULL";
    throw ex;
  }
  
  // Verify that new unit status is consistent with the
  // current status of the drive.
  if ( ptr_driveRequest->status & VDQM_UNIT_DOWN ) {
    // Unit configured down. No other status possible.
    // For security this status can only be set from
    // tape server.
    if(strcmp(ptr_driveRequest->reqhost, tapeServer->serverName().c_str())!=0) {
      castor::exception::Exception ex(EPERM);
      ex.getMessage() << "TapeDriveConsistencyChecker::checkConsistency(): "
                      << "unauthorized " << ptr_tapeDrive->driveName() << "@"
                      << tapeServer->serverName() << " DOWN from "
                      << ptr_driveRequest->reqhost << std::endl;      
      throw ex;
    }
    
    // Remove running request (if any). Client will normally retry
    if ( ptr_tapeDrive->status() != UNIT_UP && 
         ptr_tapeDrive->status() != UNIT_DOWN) {
        // If we are here, the tapeDrive info in the db is still "busy" 
        
        // We delete the old Tape request, if any
        deleteOldRequest();
    }

    castor::dlf::Param param[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
      castor::dlf::Param("oldStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(ptr_tapeDrive->status())),
      castor::dlf::Param("newStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_DOWN))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_DRIVE_STATE_TRANSITION, 4, param);
    
    ptr_tapeDrive->setStatus(UNIT_DOWN);
  } 
  else if ( ptr_driveRequest->status & VDQM_UNIT_UP ) {
    // Unit configured up. Make sure that "down" status is reset.
    // We also mark drive free if input request doesn't specify
    // otherwise.
    // If the input request is a plain config up and the unit was not 
    // down there is a job assigned it probably  means that the tape 
    // server has been rebooted. It does then not make sense
    // to keep the job because client has lost connection long ago and
    // has normally issued a retry. We can therefore remove the 
    // job from queue.
    if ( (ptr_driveRequest->status == VDQM_UNIT_UP ||
          ptr_driveRequest->status == (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
          ptr_tapeDrive->status() != UNIT_DOWN ) {
            
      // We delete the old Tape request, if any
      deleteOldRequest();
      
      castor::dlf::Param param[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
        castor::dlf::Param("oldStatus",
          castor::vdqm::DevTools::tapeDriveStatus2Str(ptr_tapeDrive->status())),
        castor::dlf::Param("newStatus",
          castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_UP))};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        VDQM_DRIVE_STATE_TRANSITION, 4, param);

      ptr_tapeDrive->setStatus(UNIT_UP);
    }

    if ( ptr_driveRequest->status == VDQM_UNIT_UP ) {
      castor::dlf::Param param[] = {
        castor::dlf::Param("Function", __PRETTY_FUNCTION__),
        castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
        castor::dlf::Param("oldStatus",
          castor::vdqm::DevTools::tapeDriveStatus2Str(ptr_tapeDrive->status())),
        castor::dlf::Param("newStatus",
          castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_UP))};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        VDQM_DRIVE_STATE_TRANSITION, 4, param);

      ptr_tapeDrive->setStatus(UNIT_UP);
    }
  } else {
    
    if ( ptr_tapeDrive->status() == UNIT_DOWN ) {
      // Unit must be up before anything else is allowed
      castor::exception::Exception ex(EVQUNNOTUP);
      ex.getMessage() << "TapeDriveConsistencyChecker::checkConsistency(): "
                      << "unit is not UP" << std::endl;      
      throw ex;
    }    
    
    
    if ( ptr_driveRequest->status & VDQM_UNIT_BUSY ) {
      // If we are here, the tape daemon has sent a busy status 
      // which must be checked. If this method succeed, we switched to
      // UNIT_STARTING mode.
      checkBusyConsistency();
    } else if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
      checkFreeConsistency();
    } else {
      checkAssignConsistency();
    }
    
    
  }
  
  // Clean pointer;
  tapeServer = 0;
}


//------------------------------------------------------------------------------
// deleteOldRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::deleteOldRequest() 
   {

  TapeRequest* runningTapeReq = ptr_tapeDrive->runningTapeReq();

  if (runningTapeReq) {
    // "Remove old TapeRequest from db" message
    castor::dlf::Param param[] = {
      castor::dlf::Param("tapeRequestID", runningTapeReq->id())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_REMOVE_OLD_TAPE_REQUEST_FROM_DB, 1, param);

    // Delete the tape request from the db
    castor::vdqm::DatabaseHelper::deleteRepresentation(runningTapeReq, m_cuuid);

    // Free the memory used by the tape request and its child objects
    delete runningTapeReq->tape();
    runningTapeReq->setTape(NULL);

    delete runningTapeReq->requestedSrv();
    runningTapeReq->setRequestedSrv(NULL);

    delete runningTapeReq->deviceGroupName();
    runningTapeReq->setDeviceGroupName(NULL);

    delete runningTapeReq->tapeAccessSpecification();
    runningTapeReq->setTapeAccessSpecification(NULL);

    delete runningTapeReq;
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
   {

  /*
   * Consistency check
   */
  if ( ptr_driveRequest->status & VDQM_UNIT_FREE ) {
     castor::exception::Exception ex(EVQBADSTAT);
    ex.getMessage()
      << "TapeDriveConsistencyChecker::checkBusyConsistency(): "
         "bad status from tape daemon! FREE + BUSY doesn't fit together" 
      << std::endl;      
    throw ex;
  }

  if ( (ptr_tapeDrive->status() == UNIT_UP) ||
       (ptr_tapeDrive->status() == WAIT_FOR_UNMOUNT)) {
    castor::dlf::Param param[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
      castor::dlf::Param("oldStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(ptr_tapeDrive->status())),
      castor::dlf::Param("newStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_STARTING))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_DRIVE_STATE_TRANSITION, 4, param);

    //The tapeDrive is now in starting mode
    ptr_tapeDrive->setStatus(UNIT_STARTING);
  } else {
    castor::dlf::Param param[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
      castor::dlf::Param("oldStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(ptr_tapeDrive->status())),
      castor::dlf::Param("newStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(STATUS_UNKNOWN))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_DRIVE_STATE_TRANSITION, 4, param);

    ptr_tapeDrive->setStatus(STATUS_UNKNOWN);
    
    castor::exception::Exception ex(EVQBADSTAT);
    ex.getMessage() << "TapeDriveConsistencyChecker::checkBusyConsistency(): "
      "Cannot put tape drive into UNIT_STARTING mode" << std::endl;      
    throw ex;      
  }
}


//------------------------------------------------------------------------------
// checkFreeConsistency
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::checkFreeConsistency() 
   {
  
  castor::vdqm::VdqmTape* tape;
   TapeServer* tapeServer = ptr_tapeDrive->tapeServer(); 
  
  // Cannot free an assigned unit, it must be released first
  if ( !(ptr_driveRequest->status & VDQM_UNIT_RELEASE) &&
       ( ptr_tapeDrive->status() == UNIT_ASSIGNED ||
          ptr_tapeDrive->status() == VOL_MOUNTED )) {
    
    castor::exception::Exception ex(EVQBADSTAT);
    ex.getMessage() << "TapeDriveConsistencyChecker::checkFreeConsistency(): "
                    << "cannot free assigned unit" 
                    << ptr_tapeDrive->driveName() << "@"
                    << tapeServer->serverName() << std::endl;      
    throw ex;
  }
  
  // The mounted tape, if any
  tape = ptr_tapeDrive->tape();
  
  // Cannot free an unit with tape mounted
  if ( !(ptr_driveRequest->status & VDQM_VOL_UNMOUNT) &&
      (tape != NULL) ) {
      
    castor::exception::Exception ex(EVQBADSTAT);
    ex.getMessage() << "TapeDriveConsistencyChecker::checkFreeConsistency(): "
                    << "cannot free unit with tape mounted, volid=" 
                    << tape->vid() << std::endl;      
    throw ex;
  }    
  
  // Reset pointer
  tape = 0;
  tapeServer = 0;
}


//------------------------------------------------------------------------------
// checkAssignConsistency
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveConsistencyChecker::
  checkAssignConsistency()  {

  TapeRequest* tapeRequest;
  TapeServer* tapeServer = ptr_tapeDrive->tapeServer(); 
  
  // If unit is busy and being assigned the VolReqIDs must
  // be the same. If so, assign the jobID (normally the
  // process ID of the RTCOPY process on the tape server).
  if ( (ptr_tapeDrive->status() != UNIT_UP)   &&
       (ptr_tapeDrive->status() != UNIT_DOWN) &&
       (ptr_driveRequest->status & VDQM_UNIT_ASSIGN) ) { 
      
      tapeRequest = ptr_tapeDrive->runningTapeReq();
      
      if (tapeRequest == NULL) {
         castor::exception::Exception ex(EVQBADID);
        ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                        << "Inconsistent VolReqIDs ("
                        << ptr_driveRequest->VolReqID
                        << ", NULL) on ASSIGN" << std::endl;
        throw ex;
      }
      else {
        if ( ((int)tapeRequest->id() != ptr_driveRequest->VolReqID)) {
            
           castor::exception::Exception ex(EVQBADID);
          ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                          << "Inconsistent VolReqIDs ("
                          << ptr_driveRequest->VolReqID << ", "
                          << tapeRequest->id()
                          << ") on ASSIGN" << std::endl;
                          
          throw ex;
        } else {
          ptr_tapeDrive->setJobID(ptr_driveRequest->jobID);
            
          // "Assign of tapeRequest to jobID" message
          castor::dlf::Param params[] =
            {castor::dlf::Param("tapeRequestID", tapeRequest->id()),
             castor::dlf::Param("jobID", ptr_driveRequest->jobID)};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ASSIGN_TAPE_REQUEST_TO_JOB_ID, 2, params);
        }
      }
  }
  
  
  /*
   * If unit is busy with a running job the job IDs must be same
   */
  if ( ((ptr_tapeDrive->status() == UNIT_STARTING) ||
        (ptr_tapeDrive->status() == UNIT_ASSIGNED) ||
        (ptr_tapeDrive->status() == VOL_MOUNTED)) &&
       (ptr_driveRequest->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
                                     VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) &&
       (ptr_tapeDrive->jobID() != ptr_driveRequest->jobID) ) {
      
    castor::exception::Exception ex(EVQBADID);
    ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                    << "Inconsistent jobIDs ("
                    << ptr_driveRequest->jobID << ", "
                    << ptr_tapeDrive->jobID()
                    << ", NULL) on ASSIGN" << std::endl;
    throw ex;        
  }
  
  
  /*
   * Prevent operations on a free unit. A job must have been
   * started and unit marked busy before it can be used.
   * 22/11/1999: we change this so that a free unit can be
   *             assigned. The reason is that a local job may
   *             running on the tape server (e.g. tplabel) may
   *             want to run without starting a rtcopy job.
   */
  if ( !(ptr_driveRequest->status & VDQM_UNIT_BUSY) &&
      (ptr_tapeDrive->status() == UNIT_UP) &&
      (ptr_driveRequest->status & (VDQM_UNIT_RELEASE |
                                   VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) ) {
    std::stringstream status;

    castor::vdqm::DevTools::printTapeDriveStatusBitset(status,
      ptr_driveRequest->status);

    castor::exception::Exception ex(EVQBADSTAT);
    ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                    << "Drive status: " << status.str()
                    << " requested on FREE drive" << std::endl;
    throw ex;
  }
  
  
  if ( ptr_driveRequest->status & VDQM_UNIT_ASSIGN ) {
      /*
       * Check whether unit was already assigned. If so, the
       * volume request must be identical
       */
      if ( ((ptr_tapeDrive->status() == UNIT_ASSIGNED) ||
            (ptr_tapeDrive->status() == VOL_MOUNTED)) &&
           (ptr_tapeDrive->jobID() != ptr_driveRequest->jobID) ) {
          
        castor::exception::Exception ex(EVQBADID);
        ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                        << "attempt to re-assign jobID="
                        << ptr_tapeDrive->jobID() 
                        << " to an unit assigned to jobID="
                        << ptr_driveRequest->jobID
                        << std::endl;
        throw ex;            
      }
      
      
      /*
       * If the unit was free, we set the new jobID. This is
       * local assign bypassing the normal VDQM logic (see comment
       * above). There is no VolReqID since VDQM is bypassed.
       */
      if ( ptr_tapeDrive->status() == UNIT_UP ) {
        /*
         * We only allow this for local requests!
         */
        if ( strcmp(ptr_driveRequest->reqhost, 
             tapeServer->serverName().c_str()) != 0 ) {
            
          castor::exception::Exception ex(EPERM);
          ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                          << "unauthorized "
                          << ptr_driveRequest->drive << "@" 
                          << ptr_driveRequest->server
                          << " local assign from "
                          << ptr_driveRequest->reqhost 
                          << std::endl;
          throw ex;              
        }
        
        // "Local assign to jobID" message
        castor::dlf::Param params[] =
          {castor::dlf::Param("driveName", ptr_driveRequest->drive),
           castor::dlf::Param("serverName", ptr_driveRequest->server),
           castor::dlf::Param("jobID", ptr_driveRequest->jobID)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_LOCAL_ASSIGN_TO_JOB_ID, 2, params);          
        
        ptr_tapeDrive->setJobID(ptr_driveRequest->jobID);

        {        
          castor::dlf::Param param[] = {
            castor::dlf::Param("Function", __PRETTY_FUNCTION__),
            castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
            castor::dlf::Param("oldStatus",
              castor::vdqm::DevTools::tapeDriveStatus2Str(
                ptr_tapeDrive->status())),
            castor::dlf::Param("newStatus",
              castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_ASSIGNED))};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
              VDQM_DRIVE_STATE_TRANSITION, 4, param);
        }
        // Switched to unit ASSIGNED status
        ptr_tapeDrive->setStatus(UNIT_ASSIGNED);
        
      }
      else if (ptr_tapeDrive->status() == UNIT_STARTING) {
        // If we are not in UNIT_STARTING mode, we can't put the the tapeDrive
        // to UNIT_ASSIGNED mode!

        castor::dlf::Param param[] = {
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
          castor::dlf::Param("oldStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(
              ptr_tapeDrive->status())),
          castor::dlf::Param("newStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_ASSIGNED))};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            VDQM_DRIVE_STATE_TRANSITION, 4, param);
        
        // Switched to unit ASSIGNED status
        ptr_tapeDrive->setStatus(UNIT_ASSIGNED);
      } else {
        castor::dlf::Param param[] = {
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("driveName", ptr_tapeDrive->driveName()),
          castor::dlf::Param("oldStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(
              ptr_tapeDrive->status())),
          castor::dlf::Param("newStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(STATUS_UNKNOWN))};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            VDQM_DRIVE_STATE_TRANSITION, 4, param);

        ptr_tapeDrive->setStatus(STATUS_UNKNOWN);
        
        castor::exception::Exception ex(EVQBADSTAT);
        ex.getMessage() << "TapeDriveConsistencyChecker::checkAssignConsistency(): "
                        << "Cannot put tape drive into UNIT_STARTING mode" 
                        << std::endl;      
        throw ex;              
      }
      
      
  } // end of "if ( ptr_driveRequest->status & VDQM_UNIT_ASSIGN )"
  
  //TODO: This can cause a lot of problems for the whole logic!
//    /*
//   * VDQM_VOL_MOUNT and VDQM_VOL_UNMOUNT are not persistent unit 
//   * status values. Their purpose is twofold: 1) input - update 
//   * the volid field in the drive record (both MOUNT and UNMOUNT)
//   * and 2) output - tell client to unmount or keep volume mounted
//   * in case of deferred unmount (UNMOUNT only). 
//   *
//   * VDQM_UNIT_MBCOUNT is not a persistent unit status value.
//   * It request update of drive statistics.
//   */
//  if ( drvrec->drv.status & VDQM_UNIT_UP )
//      drvrec->drv.status |= DrvReq->status & 
//                            (~VDQM_VOL_MOUNT & ~VDQM_VOL_UNMOUNT &
//                             ~VDQM_UNIT_MBCOUNT );
  
  // Reset pointer
  tapeRequest = 0;
  tapeServer = 0;      
}
