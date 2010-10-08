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

#include <sys/types.h> // For VMGR
#include <string.h>

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeDriveCompatibility.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveDedication.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmTape.hpp"
#include "castor/vdqm/handler/TapeDriveHandler.hpp"
#include "castor/vdqm/handler/TapeDriveConsistencyChecker.hpp" // Friend
#include "castor/vdqm/handler/TapeDriveStatusHandler.hpp" // Friend
#include "h/Ctape_constants.h" // For WRITE_ENABLE WRITE_DISABLE
#include "h/net.h"
#include "h/vdqm_constants.h"
#include "h/vmgr_api.h" // For VMGR

#include <memory>
#include <vector>



//------------------------------------------------------------------------------
// Inner class TapeDriveAutoPtr constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::TapeDriveAutoPtr::TapeDriveAutoPtr(
  castor::vdqm::TapeDrive *const tapeDrive) throw() : m_tapeDrive(tapeDrive) {
}


//------------------------------------------------------------------------------
// Inner class TapeDriveAutoPtr get method
//------------------------------------------------------------------------------
castor::vdqm::TapeDrive
  *castor::vdqm::handler::TapeDriveHandler::TapeDriveAutoPtr::get() throw() {
  return m_tapeDrive;
}


//------------------------------------------------------------------------------
// Inner class TapeDriveAutoPtr destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::TapeDriveAutoPtr::~TapeDriveAutoPtr()
throw() {

  delete m_tapeDrive->tape();
  m_tapeDrive->setTape(0);

  TapeRequest* runningTapeReq = m_tapeDrive->runningTapeReq();
  if(runningTapeReq ) {
    delete runningTapeReq->tape();
    runningTapeReq->setTape(0);

    delete runningTapeReq->requestedSrv();
    runningTapeReq->setRequestedSrv(0);

    delete runningTapeReq->deviceGroupName();
    runningTapeReq->setDeviceGroupName(0);

    delete runningTapeReq->tapeAccessSpecification();
    runningTapeReq->setTapeAccessSpecification(0);

    delete runningTapeReq;
    m_tapeDrive->setRunningTapeReq(0);
  }

  std::vector<castor::vdqm::TapeDriveDedication*>
    tapeDriveDedicationVector = m_tapeDrive->tapeDriveDedication();

  for(unsigned int i=0; i<tapeDriveDedicationVector.size(); i++) {
    delete tapeDriveDedicationVector[i];
  }
  tapeDriveDedicationVector.clear();

  std::vector<castor::vdqm::TapeDriveCompatibility*>
    tapeDriveCompatibilityVector =
    m_tapeDrive->tapeDriveCompatibilities();
  for(unsigned int i=0; i<tapeDriveCompatibilityVector.size(); i++){
    delete
      tapeDriveCompatibilityVector[i]->tapeAccessSpecification();
    tapeDriveCompatibilityVector[i]->setTapeAccessSpecification(0);

    delete tapeDriveCompatibilityVector[i];
  }
  tapeDriveCompatibilityVector.clear();

  delete m_tapeDrive->deviceGroupName();
  m_tapeDrive->setDeviceGroupName(0);

  delete m_tapeDrive;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeDriveHandler::TapeDriveHandler(
  vdqmHdr_t *const header, vdqmDrvReq_t *const driveRequest, 
  const Cuuid_t cuuid) throw(castor::exception::Exception) :
  ptr_header(header), ptr_driveRequest(driveRequest), m_cuuid(cuuid){
    
  if ( header == NULL || driveRequest == NULL ) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "One of the arguments is NULL";
    throw ex;
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

  // XXX: This log doesn't appear in the log!
  //"The parameters of the old vdqm DrvReq Request" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("dedidcate", ptr_driveRequest->dedicate),
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
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_OLD_VDQM_DRV_REQ_PARAMS,
    16, params);

  // The VDQM2 does not support "tape daemon startup status" messages
  if ( ptr_driveRequest->status == VDQM_TPD_STARTED ) {
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      VDQM_TPD_STARTED_NOT_SUPPORTED);
    return;
  }

  std::auto_ptr<castor::vdqm::TapeServer> tapeServer(
    ptr_IVdqmService->selectOrCreateTapeServer(ptr_driveRequest->reqhost,
      false)); // withTapeDrives = false

  // Gets the TapeDrive from the db or creates a new one.
  TapeDriveAutoPtr tapeDrive(getTapeDrive(tapeServer.get()));

  // Log the actual "new" status and the old one.
  printStatus(ptr_driveRequest->status, tapeDrive.get()->status());

  if(ptr_driveRequest->status == VDQM_UNIT_QUERY ) {
    copyTapeDriveInformations(tapeDrive.get());
    return;
  }                                            

  // Verify that new unit status is consistent with the
  // current status of the drive.
  // In some cases a jobID can be overgiven to tapeDrive and the status
  // can already change to UNIT_STARTING or UNIT_ASSIGNED.
  TapeDriveConsistencyChecker consistencyChecker(tapeDrive.get(),
    ptr_driveRequest, m_cuuid);
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

  // If we have passed the consistency checker, then everything looks fine and 
  // we are ready to handle the status from the tape daemon.

  // newRequestId will get a valid request ID (value > 0) if there is
  // another request for the same tape
  u_signed64 newRequestId;
  TapeDriveStatusHandler statusHandler(tapeDrive.get(), ptr_driveRequest,
    m_cuuid, &newRequestId);
  statusHandler.handleOldStatus();

  // Now the last thing is to update the data base if neccessary.
  // Note that if an allocation was reused then the database has already been
  // updated via a PL/SQL procedure which did not keep tapeDrive in sync,
  // therefore in this case an update using tapeDrive should not be performed.
  if(newRequestId == 0) {
    castor::vdqm::DatabaseHelper::updateRepresentation(tapeDrive.get(),
      m_cuuid);
  }
   
  // Log the actual "new" status.
  printStatus(0, tapeDrive.get()->status());
}


//------------------------------------------------------------------------------
// deleteTapeDrive
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::deleteTapeDrive() 
  throw (castor::exception::Exception) {

  ptr_IVdqmService->deleteDrive(ptr_driveRequest->drive,
    ptr_driveRequest->server, ptr_driveRequest->dgn);    
}


//------------------------------------------------------------------------------
// getTapeDrive
//------------------------------------------------------------------------------
castor::vdqm::TapeDrive* 
  castor::vdqm::handler::TapeDriveHandler::getTapeDrive(TapeServer* tapeServer) 
  throw (castor::exception::Exception) {
    
    castor::vdqm::TapeDrive* tapeDrive = NULL;
    castor::vdqm::DeviceGroupName* dgName = NULL;
    
    // Check, if the tape drive already exists
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
      // The tape drive does not exist, so we just create it!
      // But first we need its device group name out of the db.
      
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
      tapeDrive->setErrcount(ptr_driveRequest->errcount);
      tapeDrive->setJobID(ptr_driveRequest->jobID);
      tapeDrive->setModificationTime(0); // Will be set by db trigger
      tapeDrive->setResettime(ptr_driveRequest->resettime);
      tapeDrive->setTotalMB(ptr_driveRequest->TotalMB);
      tapeDrive->setTransferredMB(ptr_driveRequest->MBtransf);
      tapeDrive->setUsecount(ptr_driveRequest->usecount);      

      // Make sure it is either up or down. If neither, we put it in
      // UNKNOWN status until further status information is received.
      // This is just for the old protocol.
      if ( (ptr_driveRequest->status & ( VDQM_UNIT_UP|VDQM_UNIT_DOWN)) == 0 ) {
        ptr_driveRequest->status |= VDQM_UNIT_UP|VDQM_UNIT_UNKNOWN;
        
        castor::dlf::Param param[] = {
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("driveName", tapeDrive->driveName()),
          castor::dlf::Param("oldStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(
              tapeDrive->status())),
          castor::dlf::Param("newStatus",
            castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_UP))};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            VDQM_DRIVE_STATE_TRANSITION, 4, param);

        // Our new tapeDrive Object is just in status UNIT_UP
        tapeDrive->setStatus(UNIT_UP); 
      }
      
      // Make sure it doesn't come up with some non-persistent status
      // because of a previous VDQM server crash.
      ptr_driveRequest->status = ptr_driveRequest->status & ( ~VDQM_VOL_MOUNT &
          ~VDQM_VOL_UNMOUNT & ~VDQM_UNIT_MBCOUNT );
          
      // Retrieve the information about the cartridge model.
      std::string driveModel = "";
      vmgr_list list;
      struct vmgr_tape_dgnmap *dgnmap;
      int flags;
      flags = VMGR_LIST_BEGIN;
      while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {
        if ( strcmp(dgnmap->dgn, ptr_driveRequest->dgn) == 0 ) {
          driveModel = dgnmap->model;
          break;
        }
        flags = VMGR_LIST_CONTINUE;
      }
      (void) vmgr_listdgnmap (VMGR_LIST_END, &list);
      
      // Looks whether there are already existing entries in the 
      // TapeDriveCompatibility table for that model
      ptr_IVdqmService->selectCompatibilitiesForDriveModel(tapeDrive,
        driveModel);

      if(tapeDrive->tapeDriveCompatibilities().size() == 0) {
        // Handle the connection to the priority list of the tape Drives
        handleTapeDriveCompatibilities(tapeDrive, driveModel);
      }

      // "Create new TapeDrive in DB" message
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_CREATE_DRIVE_IN_DB);

      // We don't want to commit now, because some changes can can still happen
      castor::vdqm::DatabaseHelper::storeRepresentation(tapeDrive, m_cuuid);
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
  castor::vdqm::TapeAccessSpecification* tapeAccessSpecification;
  castor::vdqm::VdqmTape* tape;
  castor::vdqm::TapeServer* tapeServer;
  castor::vdqm::DeviceGroupName* devGrpName;

  ptr_driveRequest->status    = tapeDriveStatus2Bitset(tapeDrive->status());
  ptr_driveRequest->DrvReqID  = (unsigned int)tapeDrive->id();
  ptr_driveRequest->jobID     = tapeDrive->jobID();
  ptr_driveRequest->recvtime  = (int)tapeDrive->modificationTime();
  ptr_driveRequest->resettime = (int)tapeDrive->resettime();
  ptr_driveRequest->usecount  = tapeDrive->usecount();        
  ptr_driveRequest->errcount  = tapeDrive->errcount();
  ptr_driveRequest->MBtransf  = tapeDrive->transferredMB();
  ptr_driveRequest->TotalMB   = tapeDrive->totalMB();
  
  runningTapeReq = tapeDrive->runningTapeReq();
  if ( NULL != runningTapeReq ) {
    ptr_driveRequest->VolReqID = (unsigned int)runningTapeReq->id();
    
    castor::vdqm::ClientIdentification* client = runningTapeReq->client();
    strcpy(ptr_driveRequest->reqhost, client->machine().c_str());

    tapeAccessSpecification = runningTapeReq->tapeAccessSpecification();
    if( NULL != tapeAccessSpecification ) {
      ptr_driveRequest->mode = tapeAccessSpecification->accessMode();

      tapeAccessSpecification = 0;
    }
    
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
}


//------------------------------------------------------------------------------
// tapeDriveStatus2Bitset
//------------------------------------------------------------------------------
int castor::vdqm::handler::TapeDriveHandler::tapeDriveStatus2Bitset(
  const TapeDriveStatusCodes status) throw (castor::exception::Exception) {

  switch(status) {
  case UNIT_UP:
    return VDQM_UNIT_UP | VDQM_UNIT_FREE;
    break;
  case UNIT_STARTING:
    return VDQM_UNIT_UP | VDQM_UNIT_BUSY;
    break;
  case UNIT_ASSIGNED:
    return VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
    break;
  case VOL_MOUNTED:
    return VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_ASSIGN;
    break;
  case FORCED_UNMOUNT:
    return VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE |
      VDQM_UNIT_UNKNOWN | VDQM_FORCE_UNMOUNT;
    break;
  case UNIT_DOWN:
    return VDQM_UNIT_DOWN;
    break;
  case WAIT_FOR_UNMOUNT:
    return VDQM_UNIT_UP | VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE |
      VDQM_UNIT_UNKNOWN;
    break;
  case STATUS_UNKNOWN:
    return VDQM_UNIT_UNKNOWN;
    break;
  default:
    castor::exception::Internal ie;
    ie.getMessage() << "Unknown drive status: " << status << std::endl;
    throw ie;
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
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_DESIRED_OLD_PROTOCOL_CLIENT_STATUS, 1, param);
  }
  
  
  // "The actual \"new Protocol\" status of the client" message
  switch ( newActStatus ) {
    case UNIT_UP: 
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "UNIT_UP")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }
          break;
    case UNIT_STARTING:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "UNIT_STARTING")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }    
          break;
    case UNIT_ASSIGNED:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "UNIT_ASSIGNED")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }    
          break;
    case VOL_MOUNTED:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "VOL_MOUNTED")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }    
          break;
    case FORCED_UNMOUNT:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "FORCED_UNMOUNT")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }    
          break;
    case UNIT_DOWN:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "UNIT_DOWN")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }    
          break;
    case WAIT_FOR_UNMOUNT:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "WAIT_FOR_UNMOUNT")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }  
          break;
    case STATUS_UNKNOWN:
          {
            castor::dlf::Param param1[] =
              {castor::dlf::Param("status", "STATUS_UNKNOWN")};
            castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, 1, param1);
          }            
          break;
    default:
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "The tapeDrive is in a wrong status" << std::endl;
          throw ex;
  }
}


//------------------------------------------------------------------------------
// sendTapeDriveQueue
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::sendTapeDriveQueue(
  const vdqmVolReq_t *const,
  castor::vdqm::OldProtocolInterpreter *const oldProtInterpreter) 
  throw (castor::exception::Exception) {

  std::string dgn    = "";
  std::string server = "";

  if ( *(ptr_driveRequest->dgn)    != '\0' ) dgn    = ptr_driveRequest->dgn;
  if ( *(ptr_driveRequest->server) != '\0' ) server = ptr_driveRequest->server;

  try {
    // The result of the search in the database.
    std::list<vdqmDrvReq_t> drvReqs;

    // This method call retrieves the drive queue from the database.  The
    // result depends on the parameters. If the paramters are not specified,
    // then information about all tape drives is returned.
    ptr_IVdqmService->getTapeDriveQueue(drvReqs, dgn, server);

    // If there is a result to send to the client
    if(drvReqs.size() > 0 ) {
      for(std::list<vdqmDrvReq_t>::iterator it = drvReqs.begin();
        it != drvReqs.end(); it++) {
        
        //"Send information for showqueues command" message
        castor::dlf::Param param[] = {
          castor::dlf::Param("message", "TapeDrive info"),
          castor::dlf::Param("tapeDriveID", it->DrvReqID)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
          VDQM_SEND_SHOWQUEUES_INFO, 2, param);
        
        //Send informations to the client
        oldProtInterpreter->sendToOldClient(ptr_header, NULL, &(*it));
      }
    }
  } catch (castor::exception::Exception& ex) {  
    // To inform the client about the end of the queue, we send again a 
    // ptr_driveRequest with the VolReqID = -1
    ptr_driveRequest->DrvReqID = -1;
  
    oldProtInterpreter->sendToOldClient(ptr_header, NULL, ptr_driveRequest);
          
    throw ex;
  }
  
  // To inform the client about the end of the queue, we send again a 
  // ptr_driveRequest with the VolReqID = -1
  ptr_driveRequest->DrvReqID = -1;
  
  oldProtInterpreter->sendToOldClient(ptr_header, NULL, ptr_driveRequest);
}


//------------------------------------------------------------------------------
// dedicateTapeDrive
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::dedicateTapeDrive()
  throw (castor::exception::Exception) {

  // Try to dedicate the drive
  ptr_IVdqmService->dedicateDrive(
    ptr_driveRequest->drive,
    ptr_driveRequest->server,
    ptr_driveRequest->dgn,
    ptr_driveRequest->dedicate);
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
    if ( strcmp(dgnmap->dgn, dgName->dgName().c_str()) == 0 ) {
      tapeAccessSpecs =
        ptr_IVdqmService->selectTapeAccessSpecifications(dgnmap->model);
      
      if ( tapeAccessSpecs != NULL) {
        
        // XXX
        // In case that the tape drive model name is not sent with the
        // client request, we have to take the same name as the tape model.
        if ( driveModel == "" ) {
          driveModel = dgnmap->model;
        }
        
        // Add for each found specification an entry in the
        // tapeDriveCompatibility vector. Later, we will set then right
        // priority order
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
    // If there are more than 2 entries, we have to order the priority List
    // again:
    // First write, then read!
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
  
  
  // Finally, we have to store the new TapeDriveCompatibility objects
  std::vector<castor::vdqm::TapeDriveCompatibility*>
    tapeDriveCompatibilityVector = newTapeDrive->tapeDriveCompatibilities();
  for (i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
    castor::vdqm::DatabaseHelper::storeRepresentation(
      tapeDriveCompatibilityVector[i], m_cuuid);
  }  
}
