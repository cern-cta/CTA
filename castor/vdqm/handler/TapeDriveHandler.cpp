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
#include <sys/types.h> // For VMGR
#include <vector>

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/newVdqm.h"
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
#include "h/vdqm_constants.h"
#include "h/vmgr_api.h" // For VMGR


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

#ifdef PRINT_NETWORK_MESSAGES
  castor::vdqm::DevTools::printTapeDriveStatusBitset(std::cout,
    ptr_driveRequest->status);
#endif

  castor::vdqm::TapeServer* tapeServer = NULL;
  castor::vdqm::TapeDrive* tapeDrive = NULL;
  

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
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG, VDQM_OLD_VDQM_DRV_REQ_PARAMS, 16, params);


  try {
    bool withTapeDrives = (ptr_driveRequest->status == VDQM_TPD_STARTED);
    tapeServer = 
      ptr_IVdqmService->selectTapeServer(ptr_driveRequest->reqhost, withTapeDrives);
    

    /**
     * If it is a tape daemon startup status we delete all TapeDrives 
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
  // newRequestId will get a valid request ID (value > 0) if there is
  // another request for the same tape
  u_signed64 newRequestId;
  TapeDriveStatusHandler statusHandler(tapeDrive, ptr_driveRequest, m_cuuid,
    &newRequestId);
  statusHandler.handleOldStatus();


  /**
   * Now the last thing is to update the data base if neccessary.
   * Note that if an allocation was reused then the database has already been
   * updated via a PL/SQL procedure which did not keep tapeDrive in sync,
   * therefore in this case an update using tapeDrive should not be performed.
   */
  if(newRequestId == 0) {
    castor::vdqm::DatabaseHelper::updateRepresentation(tapeDrive, m_cuuid);
  }
   
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

  ptr_IVdqmService->deleteDrive(ptr_driveRequest->drive,
    ptr_driveRequest->server, ptr_driveRequest->dgn);    
}


//------------------------------------------------------------------------------
// deleteAllTapeDrvsFromSrv
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::deleteAllTapeDrvsFromSrv(
  TapeServer* tapeServer) 
  throw (castor::exception::Exception) {

  TapeDrive* tapeDrive = NULL;

  
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
      tapeDrive = *it;

      // The old TapeRequest. Normally it should not exist.
      TapeRequest* runningTapeReq = tapeDrive->runningTapeReq();     
      
      if (runningTapeReq != 0) {
        castor::vdqm::DatabaseHelper::deleteRepresentation(runningTapeReq,
          m_cuuid);
        delete runningTapeReq;
        runningTapeReq = 0;
        tapeDrive->setRunningTapeReq(0);
      }
      
      castor::vdqm::DatabaseHelper::deleteRepresentation(tapeDrive, m_cuuid);
      delete tapeDrive;
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
      tapeDrive->setErrcount(ptr_driveRequest->errcount);
      tapeDrive->setJobID(ptr_driveRequest->jobID);
      tapeDrive->setModificationTime(0); // Will be set by db trigger
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
       * because of a previous VDQM server crash.
       */
      ptr_driveRequest->status = ptr_driveRequest->status & ( ~VDQM_VOL_MOUNT &
          ~VDQM_VOL_UNMOUNT & ~VDQM_UNIT_MBCOUNT );
          
      /**
       * Retrieve the information about the cartridge model.
       */
      std::string driveModel = "";
      vmgr_list list;
      struct vmgr_tape_dgnmap *dgnmap;
      int flags;
      flags = VMGR_LIST_BEGIN;
      while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {
        if ( std::strcmp(dgnmap->dgn, ptr_driveRequest->dgn) == 0 ) {
          driveModel = dgnmap->model;
          break;
        }
        flags = VMGR_LIST_CONTINUE;
      }
      (void) vmgr_listdgnmap (VMGR_LIST_END, &list);
      
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
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_CREATE_DRIVE_IN_DB);
      /**
       * We don't want to commit now, because some changes can can still happen
       */
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
  

//  castor::vdqm::ClientIdentification* client;

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
    default:
      castor::exception::Internal ex;
      ex.getMessage() << "Unknown drive status: "
                          << tapeDrive->status() << std::endl;
      throw ex;
  }
  
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
  std::vector<newVdqmDrvReq_t>* drvReqs = 0;
  
  std::string dgn    = "";
  std::string server = "";

  if ( *(ptr_driveRequest->dgn)    != '\0' ) dgn    = ptr_driveRequest->dgn;
  if ( *(ptr_driveRequest->server) != '\0' ) server = ptr_driveRequest->server;

  try {
    // This method call retrieves the drive queue from the database.  The
    // result depends on the parameters. If the paramters are not specified,
    // then information about all tape drives is returned.
    drvReqs = ptr_IVdqmService->selectTapeDriveQueue(dgn, server);

    // If there is a result to send to the client
    if (drvReqs != NULL && drvReqs->size() > 0 ) {
      for(std::vector<newVdqmDrvReq_t>::iterator it = drvReqs->begin();
        it != drvReqs->end(); it++) {
        
        //"Send information for showqueues command" message
        castor::dlf::Param param[] = {
          castor::dlf::Param("message", "TapeDrive info"),
          castor::dlf::Param("TapeDrive ID", it->DrvReqID)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
          VDQM_SEND_SHOWQUEUES_INFO, 2, param);
        
        //Send informations to the client
        oldProtInterpreter->sendToOldClient(ptr_header, NULL, &(*it));
      }
    }
  } catch (castor::exception::Exception ex) {  
    // Clean up the memory
    if(drvReqs != NULL) {
      delete drvReqs;
    }

    // To inform the client about the end of the queue, we send again a 
    // ptr_driveRequest with the VolReqID = -1
    ptr_driveRequest->DrvReqID = -1;
  
    oldProtInterpreter->sendToOldClient(ptr_header, NULL, ptr_driveRequest);
          
    throw ex;
  }
  
  // Clean up the memory
  if(drvReqs != NULL) {
    delete drvReqs;
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

  DriveDedications dedications;

  // Extract all possible dedications
  dedications.uid     = getDedication(ptr_driveRequest->dedicate, "uid=*,"    );
  dedications.gid     = getDedication(ptr_driveRequest->dedicate, "gid=*,"    );
  dedications.name    = getDedication(ptr_driveRequest->dedicate, "name=*,"   );
  dedications.host    = getDedication(ptr_driveRequest->dedicate, "host=*,"   );
  dedications.vid     = getDedication(ptr_driveRequest->dedicate, "vid=*,"    );
  dedications.mode    = getDedication(ptr_driveRequest->dedicate, "mode=*,"   );
  dedications.datestr = getDedication(ptr_driveRequest->dedicate, "datestr=*,");
  dedications.timestr = getDedication(ptr_driveRequest->dedicate, "timestr=*,");
  dedications.age     = getDedication(ptr_driveRequest->dedicate, "age=*"     );

  // Reject those dedications with invalid syntax and those which require
  // unsupported functionality
  rejectInvalidDedications(&dedications);

  // Try to dedicate the drive, a castor::exception::Exception will be thrown
  // if there is an error
  ptr_IVdqmService->dedicateDrive(
    ptr_driveRequest->drive,
    ptr_driveRequest->server,
    ptr_driveRequest->dgn,
    dedications.mode == "0" ? 0 : 1,
    dedications.host,
    dedications.vid);
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
  std::vector<castor::vdqm::TapeDriveCompatibility*>
    tapeDriveCompatibilityVector = newTapeDrive->tapeDriveCompatibilities();
  for (i = 0; i < tapeDriveCompatibilityVector.size(); i++) {
    castor::vdqm::DatabaseHelper::storeRepresentation(
      tapeDriveCompatibilityVector[i], m_cuuid);
  }  
}


//------------------------------------------------------------------------------
// getDedication
//------------------------------------------------------------------------------
std::string castor::vdqm::handler::TapeDriveHandler::getDedication(
const char *input, const char *format) {
  std::string        output;
  std::ostringstream oss;
  int                inputLen   = strlen(input);
  int                formatLen  = strlen(format);
  int                inputPos   = 0;
  int                formatPos  = 0;

  // For each input character
  for(inputPos=0; inputPos<inputLen; inputPos++) {

    // If the current format character is a '*'
    if(format[formatPos] == '*') {

      // If there is a format character after the '*'
      if((formatPos + 1) < formatLen) {

        // If the current input character matches the next format character
        if(input[inputPos] == format[formatPos + 1]) {

          // Finish the extraction
          break;

        } else {

          // Append the current input character to the output
          oss << input[inputPos];
        }

      // Else there is no format character after the '*'
      } else {

        // Append the current input character to the output
        oss << input[inputPos];
      }

    // Else the current format character is not a '*'
    } else {

      // If the current input character matches the current format character
      if(input[inputPos] == format[formatPos]) {

        // Move to the next format character
        formatPos++;

        // If the end of the format string has been reached
        if(formatPos == formatLen) {

          // Finish the extraction
          break;
        }

      // Else the current input character does not match the current format
      // character
      } else {

        // Reset the format character position
        formatPos = 0;
      }
    }
  }

  return oss.str();
}


//------------------------------------------------------------------------------
// allAlphanumericOrUnderscore
//------------------------------------------------------------------------------
bool castor::vdqm::handler::TapeDriveHandler::allAlphanumericOrUnderscore(
  std::string &s) {

  std::string::iterator itor;

  // For each character in the string
  for(itor=s.begin(); itor!=s.end(); itor++) {
    if(
      !((*itor >= '0') && (*itor <= '9')) && // Not a numeric
      !((*itor >= 'A') && (*itor <= 'Z')) && // Not an upper case alphabetic
      !((*itor >= 'a') && (*itor <= 'z')) && // Not a lower case alphabetic
      (*itor != '_')) {                      // Not an underscore

      return false;
    }
  }

  // Each of the characters in the string is either an alphanumeric or an
  // underscore
  return true;
}


/*
//------------------------------------------------------------------------------
// setTapeDriveHostDedication
//------------------------------------------------------------------------------
bool castor::vdqm::handler::TapeDriveHandler::setTapeDriveHostDedication(
  const char *dedicate)
  throw (castor::exception::Exception) {
}


//------------------------------------------------------------------------------
// setTapeDriveVidDedication
//------------------------------------------------------------------------------
bool castor::vdqm::handler::TapeDriveHandler::setTapeDriveVidDedication(
  const char *dedicate)
  throw (castor::exception::Exception) {
}
*/


//------------------------------------------------------------------------------
// rejectInvalidDedications
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeDriveHandler::rejectInvalidDedications(
  DriveDedications *dedications) throw (castor::exception::Exception) {

  // Reject uid, gid, name, datestr, timestr and age dedications
  if((dedications->uid != ".*") && (dedications->uid != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "uid dedications are not supported '"
      << dedications->uid << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->gid != ".*") && (dedications->gid != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "gid dedications are not supported '"
      << dedications->gid << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->name != ".*") && (dedications->name != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "name dedications are not supported '"
      << dedications->name << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->datestr != ".*") && (dedications->datestr != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "datestr dedications are not supported '"
      << dedications->datestr << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->timestr != ".*") && (dedications->timestr != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "timestr dedications are not supported '"
      << dedications->timestr << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->age != ".*") && (dedications->age != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "age dedications are not supported '"
      << dedications->age << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }

  // Reject host and vid dedications that use a regular expression other
  // than ".*"
  if((dedications->host != ".*") &&
    !allAlphanumericOrUnderscore(dedications->host)) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "Invalid host dedication '"
      << dedications->host << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
  if((dedications->vid != ".*") &&
    !allAlphanumericOrUnderscore(dedications->vid)) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "Invalid vid dedication '"
      << dedications->vid << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }

  // Reject invalid mode dedications
  if((dedications->mode != ".*") && (dedications->mode != "0") &&
    (dedications->mode != "")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "TapeDriveHandler::rejectInvalidDedications(): "
      << "Invalid mode dedication '"
      << dedications->mode << "' "
      << ptr_driveRequest->drive << "@"
      << ptr_driveRequest->server << std::endl;
    throw ex;
  }
}
