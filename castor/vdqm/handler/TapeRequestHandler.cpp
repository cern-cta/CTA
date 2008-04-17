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
 
#include <memory>
#include <string> 
#include <vector>
#include <time.h>

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/VdqmTape.hpp"

#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/newVdqm.h"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/handler/TapeRequestHandler.hpp"

#define VDQMSERV 1

#include <net.h>
#include <vdqm_constants.h>
#include "h/Ctape_constants.h"
#include <common.h> //for getconfent

/**
 * includes to talk to VMGR
 */
#include <vmgr_struct.h>
#include <sys/types.h>
#include "vmgr_api.h"
 

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestHandler::TapeRequestHandler() 
  throw(castor::exception::Exception) {
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::handler::TapeRequestHandler::~TapeRequestHandler() throw() {
}

//------------------------------------------------------------------------------
// newTapeRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestHandler::newTapeRequest(
  newVdqmHdr_t *header, newVdqmVolReq_t *volumeRequest, Cuuid_t cuuid) 
  throw (castor::exception::Exception) {
  
  struct vmgr_tape_info tape_info; // used to get information about the tape
  
  int rc = 0;
  char *p;


  if ( header == NULL || volumeRequest == NULL ) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "One of the arguments is NULL"
                    << std::endl;
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
     castor::dlf::Param("drive",
       (*volumeRequest->drive == '\0' ? "***" : volumeRequest->drive)),
     castor::dlf::Param("server",
       (*volumeRequest->server == '\0' ? "***" : volumeRequest->server))};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, VDQM_OLD_VDQM_VOLREQ_PARAMS,
    10, params);
  
  //------------------------------------------------------------------------
  //The Tape related informations
  std::auto_ptr<TapeRequest> newTapeReq(new TapeRequest());
  newTapeReq->setCreationTime(0); // Will be set by a database trigger
  // The modification time will only be changed,
  // in case that an error occures during the tape assignment
  // procedure with RTCPD
  newTapeReq->setModificationTime(newTapeReq->creationTime());

  // We don't allow client to set priority
  newTapeReq->setPriority(VDQM_PRIORITY_NORMAL);
   
  //The client related informations
  std::auto_ptr<ClientIdentification>
    clientData(new castor::vdqm::ClientIdentification());
  clientData->setMachine(volumeRequest->client_host);
  clientData->setUserName(volumeRequest->client_name);
  clientData->setPort(volumeRequest->client_port);
  clientData->setEuid(volumeRequest->clientUID);
  clientData->setEgid(volumeRequest->clientGID);
  clientData->setMagic(header->magic);
   
  // Annotation: The side of the Tape is not necesserally needed
  // by the vdqmDaemon. Normaly the RTCopy daemon should already 
  // have created an entry to the Tape table. So, we just give 0 as parameter 
  // at this place.
  std::auto_ptr<VdqmTape> tape(ptr_IVdqmService->selectTape(
    volumeRequest->volid, 0, volumeRequest->mode));                                     
  // The requested tape server: Return value is NULL if the server name is
  // empty
  std::auto_ptr<TapeServer> reqTapeServer(
    ptr_IVdqmService->selectTapeServer(volumeRequest->server, false));
  
  memset(&tape_info,'\0',sizeof(vmgr_tape_info));
  
  // "Try to get information about the tape from the VMGR daemon" message
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,VDQM_GET_TAPE_INFO_FROM_VMGR);     
  // Create a connection to vmgr daemon, to obtain more information about the
  // tape, like its density and its tapeModel.
  rc = vmgr_querytape(tape->vid().c_str(), 0, &tape_info, volumeRequest->dgn);    
  if ( rc == -1) {
    castor::exception::Exception ex(EVQDGNINVL);
    ex.getMessage() << "Errors, while using to vmgr_querytape: "
                    << "serrno = " << serrno
                    << std::endl;
    throw ex;      
  }
  
  // Validate the requested tape Access for the specific tape model.
  // In case of errors, the return value is NULL
  std::auto_ptr<TapeAccessSpecification> tapeAccessSpec(
    ptr_IVdqmService->selectTapeAccessSpecification(volumeRequest->mode,
    tape_info.density, tape_info.model));

  if (0 == tapeAccessSpec.get()) {
    castor::exception::Exception ex(EVQDGNINVL);
    ex.getMessage()
      << "The specified tape access mode doesn't exist in the db"
      << std::endl;
    throw ex;
  }
  
  // The requested device group name. If the entry doesn't yet exist, 
  // it will be created.
  std::auto_ptr<DeviceGroupName> dgName(
    ptr_IVdqmService->selectDeviceGroupName(volumeRequest->dgn));
    
  if(0 == dgName.get()) {
    castor::exception::Exception ex(EVQDGNINVL);
    ex.getMessage() << "DGN " <<  volumeRequest->dgn
                    << " does not exist in the db" << std::endl;
    throw ex;
  }

  // Connect the tapeRequest with the additional information
  newTapeReq->setClient(clientData.release()); // Tape request becomes owner
  newTapeReq->setTapeAccessSpecification(tapeAccessSpec.get());
  newTapeReq->setRequestedSrv(reqTapeServer.get());
  newTapeReq->setTape(tape.get());
  newTapeReq->setDeviceGroupName(dgName.get());
  
  // Set priority for tpwrite
  if ( (tapeAccessSpec->accessMode() == WRITE_ENABLE) &&
       ((p = getconfent("VDQM","WRITE_PRIORITY",0)) != NULL) ) {
    if ( strcmp(p,"YES") == 0 ) {
      newTapeReq->setPriority(VDQM_PRIORITY_MAX);
    }
  }
    
  // Request priority changed
  castor::dlf::Param params2[] = {
    castor::dlf::Param("priority", newTapeReq->priority())};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
    VDQM_REQUEST_PRIORITY_CHANGED, 1, params2);
  
  // Verify that the request doesn't exist, 
  // by calling IVdqmSvc->checkTapeRequest
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
    VDQM_VERIFY_REQUEST_NON_EXISTANT);

  // Verify that the request doesn't (yet) exist. If it doesn't exist,
  // the return value should be true.
  bool requestNotExists =
    ptr_IVdqmService->checkTapeRequest(newTapeReq.get());
  if ( requestNotExists == false ) {
    castor::exception::Exception ex(EVQALREADY);
    ex.getMessage() << "Input request already queued " << std::endl;
    throw ex;
  }
    
  // Try to store Request into the data base
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, VDQM_STORE_REQUEST_IN_DB);

  // Add the record to the volume queue
  castor::vdqm::DatabaseHelper::storeRepresentation(newTapeReq.get(), cuuid);
    
  // Now the newTapeReq has the id of its 
  // row representatioon in the db table.
  // 
  // Please note: Because of the restrictions of the old
  // protocol, we must convert the 64bit id into a 32bit number.
  // This is still sufficient to have a unique TapeRequest ID, because
  // the requests don't stay for long time in the db.
  volumeRequest->VolReqID = (unsigned int)newTapeReq->id();  
}


//------------------------------------------------------------------------------
// deleteTapeRequest
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestHandler::deleteTapeRequest(
  newVdqmVolReq_t *volumeRequest, Cuuid_t cuuid) 
  throw (castor::exception::Exception) {
  
  // Get the tapeReq from its id
  std::auto_ptr<TapeRequest> tapeReq(ptr_IVdqmService->selectTapeRequest(
    volumeRequest->VolReqID));
    
  if ( tapeReq.get() == NULL ) {
    // If we don't find the tapeRequest in the db it is normally not a big
    // deal, because the assigned tape drive has probably already finished
    // the transfer.

    // "Couldn't find the tape request in db. Maybe it is already deleted?"
    // message
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID", volumeRequest->VolReqID),
      castor::dlf::Param("function",
        "castor::vdqm::handler::TapeRequestHandler::deleteTapeRequest()")};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING,
      VDQM_TAPE_REQUEST_NOT_FOUND_IN_DB, 2, params);
      
    return;
  }

  // castor::db::ora::OraVdqmSvc::selectTapeRequest uses fillObj to fill the
  // member TapeRequest::m_tapeDrive, but the memory referenced by this
  // member is not owned (i.e. deleted in the destructor of TapeRequest) by
  // TapeRequest.  Use an auto_ptr to manage the TapeDrive heap object.
  std::auto_ptr<TapeDrive> tapeDrive(tapeReq->tapeDrive());
    
  // OK, now we have the tapeReq and its client :-)
  if ( tapeDrive.get() ) {
    // If we are here, the TapeRequest is already assigned to a TapeDrive
    //TODO:
    //
    // PROBLEM:
    // Can only remove request, if it is not assigned to a drive. Otherwise
    // it should be cleanup by resetting the drive status to RELEASE + FREE.
    // Mark it as UNKNOWN until an unit status clarifies what has happened.
    //
    
    castor::dlf::Param param[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("driveName", tapeDrive->driveName()),
      castor::dlf::Param("oldStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(tapeDrive->status())),
      castor::dlf::Param("newStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(STATUS_UNKNOWN))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
      VDQM_DRIVE_STATE_TRANSITION, 4, param);

    // Set new TapeDriveStatusCode
    tapeDrive->setStatus(STATUS_UNKNOWN);
    
    // Modification time will be updated by a database trigger
      
    // Update the data base. To avoid a deadlock, the tape drive has to be 
    // updated first!
    castor::vdqm::DatabaseHelper::updateRepresentation(tapeDrive.get(),
      cuuid);
    castor::vdqm::DatabaseHelper::updateRepresentation(tapeReq.get(), cuuid);

    castor::exception::Exception ex(EVQREQASS);
    ex.getMessage() << "TapeRequest is assigned to a TapeDrive. "
                    << "Can't delete it at the moment" << std::endl;
    throw ex;

  // Else the TapeRequest is not assigned to a TapeDrive
  } else {

    // Remove the TapeRequest from the db (queue)
    castor::Services *svcs = castor::BaseObject::services();
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    svcs->deleteRep(&ad, tapeReq.get(), false);
  
    // "TapeRequest and its ClientIdentification removed" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID", tapeReq->id())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      VDQM_TAPE_REQUEST_ANDCLIENT_ID_REMOVED, 1, params);
  }
}


//------------------------------------------------------------------------------
// getQueuePosition
//------------------------------------------------------------------------------
int castor::vdqm::handler::TapeRequestHandler::getQueuePosition(
  newVdqmVolReq_t *volumeRequest, Cuuid_t cuuid) 
  throw (castor::exception::Exception) {
    
  //To store the db related informations
  int queuePosition = 
    ptr_IVdqmService->getQueuePosition(volumeRequest->VolReqID);

  // Queue position of TapeRequest
  castor::dlf::Param params[] = {
    castor::dlf::Param("Queue position", queuePosition),
    castor::dlf::Param("tapeRequestID", volumeRequest->VolReqID)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, VDQM_QUEUE_POS_OF_TAPE_REQUEST,
    2, params);

  // Generate a log message if the TapeRequest was not found
  if(queuePosition == -1) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID", volumeRequest->VolReqID)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      VDQM_TAPE_REQUEST_NOT_IN_QUEUE, 1, params);
  }

  return queuePosition;
}


//------------------------------------------------------------------------------
// sendTapeRequestQueue
//------------------------------------------------------------------------------
void castor::vdqm::handler::TapeRequestHandler::sendTapeRequestQueue(
  newVdqmHdr_t *header, 
  newVdqmVolReq_t *volumeRequest,
  newVdqmDrvReq_t *driveRequest, 
  castor::vdqm::OldProtocolInterpreter* oldProtInterpreter,
  Cuuid_t cuuid) 
  throw (castor::exception::Exception) {

  std::string dgn    = "";
  std::string server = "";
    
  if ( *(volumeRequest->dgn)    != '\0' ) dgn    = volumeRequest->dgn;
  if ( *(volumeRequest->server) != '\0' ) server = volumeRequest->server;

  try {
    // This method call retirves the request queue from the database. The
    // result depends on the parameters. If the paramters are not specified,
    // then information about all oft the requests is returned.
    std::auto_ptr< std::vector<newVdqmVolReq_t> > volReqs(
      ptr_IVdqmService->selectTapeRequestQueue(dgn, server));
  
    // If there is a result to send to the client
    if (volReqs.get() != NULL && volReqs->size() > 0 ) {
      for(std::vector<newVdqmVolReq_t>::iterator it = volReqs->begin();
        it != volReqs->end(); it++) {

        //"Send information for showqueues command" message
        castor::dlf::Param param[] = {
          castor::dlf::Param("message", "TapeRequest info"),
          castor::dlf::Param("tapeRequestID", it->VolReqID)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
          VDQM_SEND_SHOWQUEUES_INFO, 2, param);
        
        //Send informations to the client
        oldProtInterpreter->sendToOldClient(header, &(*it), NULL);
      }
    }
  } catch (castor::exception::Exception ex) {

    // To inform the client about the end of the queue, we send again a 
    // volumeRequest with the VolReqID = -1
    volumeRequest->VolReqID = -1;
    
    oldProtInterpreter->sendToOldClient(header, volumeRequest, NULL);

    throw ex;
  }

  // To inform the client about the end of the queue, we send again a 
  // volumeRequest with the VolReqID = -1
  volumeRequest->VolReqID = -1;
  
  oldProtInterpreter->sendToOldClient(header, volumeRequest, NULL);
}
