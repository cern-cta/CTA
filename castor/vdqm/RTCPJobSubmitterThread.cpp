/******************************************************************************
 *                castor/vdqm/RTCPJobSubmitterThread.cpp
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
 * @author castor dev team
 *****************************************************************************/

#include <memory>

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/PortNumbers.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/RemoteCopyConnection.hpp"
#include "castor/vdqm/RTCPJobSubmitterThread.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmTape.hpp"
#include "h/rtcp_constants.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::RTCPJobSubmitterThread::RTCPJobSubmitterThread()
  throw () {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::RTCPJobSubmitterThread::~RTCPJobSubmitterThread()
  throw () {
}


//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::vdqm::RTCPJobSubmitterThread::select()
  throw() {

  Cuuid_t                cuuid    = nullCuuid;
  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;
  castor::IObject        *obj     = NULL;


  Cuuid_create(&cuuid);

  try {
    vdqmSvc = getDbVdqmSvc();
  } catch(castor::exception::Exception& e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::select"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
      3, params);

    return NULL;
  }

  try
  {
    obj = vdqmSvc->requestToSubmit();
    vdqmSvc->commit();
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::select"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_MATCHTAPE2TAPEDRIVE_ERROR, 3, params);

    return NULL;
  }

  return obj;
}


//------------------------------------------------------------------------------
// Inner class TapeRequestAutoPtr constructor
//------------------------------------------------------------------------------
castor::vdqm::RTCPJobSubmitterThread::TapeRequestAutoPtr::TapeRequestAutoPtr(
  castor::vdqm::TapeRequest *const tapeRequest) throw() :
  m_tapeRequest(tapeRequest) {
}


//------------------------------------------------------------------------------
// Inner class TapeRequestAutoPtr get method
//------------------------------------------------------------------------------
castor::vdqm::TapeRequest
  *castor::vdqm::RTCPJobSubmitterThread::TapeRequestAutoPtr::get() throw() {
  return m_tapeRequest;
}


//------------------------------------------------------------------------------
// Inner class TapeDriveAutoPtr destructor
//------------------------------------------------------------------------------
castor::vdqm::RTCPJobSubmitterThread::TapeRequestAutoPtr::~TapeRequestAutoPtr()
throw() {
  // Return if there is no tape request to delete
  if(m_tapeRequest == NULL) {
    return;
  }

  delete m_tapeRequest->tape();
  m_tapeRequest->setTape(NULL);

  delete m_tapeRequest->requestedSrv();
  m_tapeRequest->setRequestedSrv(NULL);

  delete m_tapeRequest->deviceGroupName();
  m_tapeRequest->setDeviceGroupName(NULL);

  delete m_tapeRequest->tapeAccessSpecification();
  m_tapeRequest->setTapeAccessSpecification(NULL);

  if(m_tapeRequest->tapeDrive() != NULL) {

    delete m_tapeRequest->tapeDrive()->deviceGroupName();
    m_tapeRequest->tapeDrive()->setDeviceGroupName(NULL);

    // Remove the request's drive from the request's tape server to break the
    // circular dependency
    m_tapeRequest->tapeDrive()->tapeServer()->removeTapeDrives(
      m_tapeRequest->tapeDrive());

    delete  m_tapeRequest->tapeDrive()->tapeServer();
    m_tapeRequest->tapeDrive()->setTapeServer(NULL);

    delete m_tapeRequest->tapeDrive();
    m_tapeRequest->setTapeDrive(NULL);
  }

  delete m_tapeRequest;
}


//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::vdqm::RTCPJobSubmitterThread::process(castor::IObject *param)
  throw() {

  Cuuid_t                cuuid    = nullCuuid;
  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;


  Cuuid_create(&cuuid);

  // Create an auto pointer to delete the tape request object when it goes out
  // of scope.
  TapeRequestAutoPtr request(dynamic_cast<castor::vdqm::TapeRequest*>(param));

  // If the dynamic cast failed
  if(request.get() == NULL) {
    castor::exception::Internal e;

    e.getMessage()
      << "Failed to cast param to castor::vdqm::TapeRequest*";

    throw e;
  }

  try {
    vdqmSvc = getDbVdqmSvc();
  } catch(castor::exception::Exception& e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC, 3,
      params);

    return;
  }

  TapeDrive *tapeDrive       = request.get()->tapeDrive();
  bool      requestSubmitted = false;

  try {

    // Submit remote copy job
    submitJob(cuuid, request.get());
    requestSubmitted = true;

  } catch(castor::exception::Exception& ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeDriveID", tapeDrive->id()),
      castor::dlf::Param("driveName", tapeDrive->driveName()),
      castor::dlf::Param("tapeRequestID", request.get()->id()),
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code", ex.code())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_FAILED_TO_SUBMIT_REMOTE_COPY_JOB, 5, params);

    try {
      bool       driveExists            = false;
      int        driveStatusBefore      = -1;
      int        driveStatusAfter       = -1;
      u_signed64 runningRequestIdBefore = 0;
      u_signed64 runningRequestIdAfter  = 0;
      bool       requestExists          = false;
      int        requestStatusBefore    = -1;
      int        requestStatusAfter     = -1;
      u_signed64 requestDriveIdBefore   = 0;
      u_signed64 requestDriveIdAfter    = 0;

      // Reset drive and request
      vdqmSvc->resetDriveAndRequest(
        tapeDrive->id(),
        request.get()->id(),
        driveExists,
        driveStatusBefore,
        driveStatusAfter,
        runningRequestIdBefore,
        runningRequestIdAfter,
        requestExists,
        requestStatusBefore,
        requestStatusAfter,
        requestDriveIdBefore,
        requestDriveIdAfter);

      castor::dlf::Param params[] = {
        castor::dlf::Param("tapeDriveID", tapeDrive->id()),
        castor::dlf::Param("driveName", tapeDrive->driveName()),
        castor::dlf::Param("tapeRequestID", request.get()->id()),
        castor::dlf::Param("driveExists", driveExists),
        castor::dlf::Param("driveStatusBef", driveStatusBefore),
        castor::dlf::Param("driveStatusAft", driveStatusAfter),
        castor::dlf::Param("runningRequestIdBef", runningRequestIdBefore),
        castor::dlf::Param("runningRequestIdAft",runningRequestIdAfter),
        castor::dlf::Param("requestExists", requestExists),
        castor::dlf::Param("requestStatusBef", requestStatusBefore),
        castor::dlf::Param("requestStatusAft", requestStatusAfter),
        castor::dlf::Param("requestDriveIdBef", requestDriveIdBefore),
        castor::dlf::Param("requestDriveIdAft", requestDriveIdAfter)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        VDQM_RESET_DRIVE_AND_REQUEST, 13, params);

    } catch(castor::exception::Exception& ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("tapeDriveID", tapeDrive->id()),
        castor::dlf::Param("driveName", tapeDrive->driveName()),
        castor::dlf::Param("tapeRequestID", request.get()->id()),
        castor::dlf::Param("Message", ex.getMessage().str()),
        castor::dlf::Param("Code", ex.code())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        VDQM_RESET_DRIVE_AND_REQUEST_DB_WRITE_FAILED, 5, params);
    }
  }

  if(requestSubmitted) {
    try {
      bool       driveExists            = false;
      int        driveStatusBefore      = -1;
      int        driveStatusAfter       = -1;
      u_signed64 runningRequestIdBefore = 0;
      u_signed64 runningRequestIdAfter  = 0;
      bool       requestExists          = FALSE;
      int        requestStatusBefore    = -1;
      int        requestStatusAfter     = -1;
      u_signed64 requestDriveIdBefore   = 0;
      u_signed64 requestDriveIdAfter    = 0;

      // Write request submitted state change to the database
      if(!vdqmSvc->requestSubmitted(
        tapeDrive->id(),
        request.get()->id(),
        driveExists,
        driveStatusBefore,
        driveStatusAfter,
        runningRequestIdBefore,
        runningRequestIdAfter,
        requestExists,
        requestStatusBefore,
        requestStatusAfter,
        requestDriveIdBefore,
        requestDriveIdAfter)) {

        castor::dlf::Param params[] = {
          castor::dlf::Param("tapeDriveID", tapeDrive->id()),
          castor::dlf::Param("driveName", tapeDrive->driveName()),
          castor::dlf::Param("tapeRequestID", request.get()->id()),
          castor::dlf::Param("driveExists", driveExists),
          castor::dlf::Param("driveStatusBef", driveStatusBefore),
          castor::dlf::Param("driveStatusAft", driveStatusAfter),
          castor::dlf::Param("runningRequestIdBef", runningRequestIdBefore),
          castor::dlf::Param("runningRequestIdAft",runningRequestIdAfter),
          castor::dlf::Param("requestExists", requestExists),
          castor::dlf::Param("requestStatusBef", requestStatusBefore),
          castor::dlf::Param("requestStatusAf", requestStatusAfter),
          castor::dlf::Param("requestDriveIdBef", requestDriveIdBefore),
          castor::dlf::Param("requestDriveIdAft", requestDriveIdAfter)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          VDQM_REQUEST_SUBMITTED_TRANSITION_FAILED, 13, params);
      }
    } catch(castor::exception::Exception& ex) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("tapeDriveID", tapeDrive->id()),
        castor::dlf::Param("driveName", tapeDrive->driveName()),
        castor::dlf::Param("tapeRequestID", request.get()->id()),
        castor::dlf::Param("Message", ex.getMessage().str()),
        castor::dlf::Param("Code", ex.code())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
        VDQM_REQUEST_SUBMITTED_DB_WRITE_FAILED, 5, params);
    }
  } // if(requestSubmitted)

  try
  {
    // Commit the changes to the DB
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    services()->commit(&ad);
  } catch(castor::exception::Exception& ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", "Failed to commit changes" +
        ex.getMessage().str()),
      castor::dlf::Param("Code", ex.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_DRIVE_ALLOCATION_ERROR, 3, params);
  }
}


//-----------------------------------------------------------------------------
// getDbVdqmSvc
//-----------------------------------------------------------------------------
castor::vdqm::IVdqmSvc *castor::vdqm::RTCPJobSubmitterThread::getDbVdqmSvc()
  throw(castor::exception::Exception)
{
  castor::Services *svcs = castor::BaseObject::services();
  castor::IService *svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
  castor::vdqm::IVdqmSvc *vdqmSvc = dynamic_cast<castor::vdqm::IVdqmSvc*>(svc);

  if (0 == vdqmSvc) {
    castor::exception::Internal ex;
    ex.getMessage()
      << "Failed to cast castor::IService* to castor::vdqm::IVdqmSvc*";

    throw ex;
  }

  return vdqmSvc;
}


//-----------------------------------------------------------------------------
// submitJob
//-----------------------------------------------------------------------------
void castor::vdqm::RTCPJobSubmitterThread::submitJob(const Cuuid_t &cuuid,
  castor::vdqm::TapeRequest *request)
  throw(castor::exception::Exception) {
  castor::vdqm::ClientIdentification *client = request->client();
  castor::vdqm::TapeDrive *tapeDrive  = request->tapeDrive();
  castor::vdqm::DeviceGroupName *dgn = tapeDrive->deviceGroupName();
  castor::vdqm::TapeServer *tapeServer = tapeDrive->tapeServer();
  const std::string remoteCopyType = request->remoteCopyType();
  unsigned short port = 0;


  if(remoteCopyType == "RTCPD") {
    port = RTCOPY_PORT;
  } else if(remoteCopyType == "AGGREGATOR" || remoteCopyType == "TAPEBRIDGE") {
    port = TAPEBRIDGE_VDQMPORT;
  } else {
    castor::exception::Internal ie;

    ie.getMessage() << "Invalid remoteCopyType: " << remoteCopyType;

    throw ie;
  }

  RemoteCopyConnection connection(port, tapeServer->serverName());

  try {
    connection.connect();
  } catch (castor::exception::Exception& e) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Failed to connect to " << remoteCopyType << ": "
      << e.getMessage().str();

    throw ie;
  }

  bool acknSucc = true;

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("remoteCopyType", remoteCopyType),
      castor::dlf::Param("tapeRequestID", request->id()),
      castor::dlf::Param("clientUserName", client->userName()),
      castor::dlf::Param("clientMachine", client->machine()),
      castor::dlf::Param("clientPort", client->port()),
      castor::dlf::Param("clientEuid", client->euid()),
      castor::dlf::Param("clientEgid", client->egid()),
      castor::dlf::Param("deviceGroupName", dgn->dgName()),
      castor::dlf::Param("tapeDriveName", tapeDrive->driveName()),
      castor::dlf::Param("tapeServerName", tapeServer->serverName()),
      castor::dlf::Param("tapeServerPort", port)};

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, VDQM_SEND_RTCOPY_JOB, 11,
      params);
  }

  try {
    acknSucc = connection.sendJob(cuuid, remoteCopyType.c_str(), request->id(),
      client->userName(), client->machine(), client->port(), client->euid(),
      client->egid(), dgn->dgName(), tapeDrive->driveName());
  } catch (castor::exception::Exception& e) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Failed to send job to " << remoteCopyType << ": "
      << e.getMessage().str();

    throw ie;
  }

  if(!acknSucc) {
    castor::exception::Internal ie;

    ie.getMessage() <<
      "Did not receive an acknSucc from sending a job to " << remoteCopyType;

    throw ie;
  }
}
