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
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/RTCopyDConnection.hpp"
#include "castor/vdqm/RTCPJobSubmitterThread.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
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

  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;
  castor::IObject        *obj     = NULL;


  try {
    vdqmSvc = getDbVdqmSvc();
  } catch(castor::exception::Exception &e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::select"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
      3, params);

    return NULL;
  }

  try
  {
    obj = vdqmSvc->requestToSubmit();
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::select"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_MATCHTAPE2TAPEDRIVE_ERROR, 3, params);

    return NULL;
  }

  return obj;
}


//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::vdqm::RTCPJobSubmitterThread::process(castor::IObject *param)
  throw() {

  std::auto_ptr<castor::vdqm::TapeRequest>
    request(dynamic_cast<castor::vdqm::TapeRequest*>(param));

  // If the dynamic cast failed
  if(request.get() == NULL) {
    castor::exception::Internal e;

    e.getMessage()
      << "Failed to cast param to castor::vdqm::TapeRequest*";

    throw e;
  }

  // Get a handle to the associated tape drive
  std::auto_ptr<castor::vdqm::TapeDrive> drive(request->tapeDrive());

  try {

    // Submit the remote tape copy job to RTCPD
    submitJobToRTCPD(request.get());

    // Everything is OK, so update the status of the request
    request->setStatus(castor::vdqm::REQUEST_SUBMITTED);

  } catch(castor::exception::Exception &e) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_DRIVE_ALLOCATION_ERROR, 3, params);

    // Un-link the associated tape drive
    request->setTapeDrive(0);
    drive->setRunningTapeReq(0);

    castor::dlf::Param param[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("driveName", drive->driveName()),
      castor::dlf::Param("oldStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(drive->status())),
      castor::dlf::Param("newStatus",
        castor::vdqm::DevTools::tapeDriveStatus2Str(UNIT_UP))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
      VDQM_DRIVE_STATE_TRANSITION, 4, param);

    // The associated tape drive is now free
    drive->setStatus(UNIT_UP);

    // Set the status of the request to pending
    request->setStatus(castor::vdqm::REQUEST_PENDING);

    // The modification times of the drive and the request will be updated by
    // database triggers because status of the drive and request have been
    // changed. In the case of the request, this will move the request to the
    // end of the queue as the oldest requests are serviced first.
  }

  // Needed to commit the changes
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);

  try {

    // Update the attributes of the request and drive in the DB
    services()->updateRep(&ad, request.get(), false);
    services()->updateRep(&ad, drive.get(), false);

    // If the associated drive was un-linked from the request then update
    // the DB
    if(request->tapeDrive() == 0) {
      services()->fillRep(&ad, request.get(), OBJ_TapeDrive, false);
      services()->fillRep(&ad, drive.get(), OBJ_TapeRequest, false);
    }

    // Commit the changes in the DB
    services()->commit(&ad);

  } catch(castor::exception::Exception &e) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
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
// submitJobToRTCPD
//-----------------------------------------------------------------------------
void castor::vdqm::RTCPJobSubmitterThread::submitJobToRTCPD(
  castor::vdqm::TapeRequest* request) throw(castor::exception::Exception) {

  castor::vdqm::ClientIdentification *client     = request->client();
  castor::vdqm::TapeDrive            *tapeDrive  = request->tapeDrive();
  castor::vdqm::DeviceGroupName      *dgn        = tapeDrive->deviceGroupName();
  castor::vdqm::TapeServer           *tapeServer = tapeDrive->tapeServer();

  
  RTCopyDConnection rtcpConnection(RTCOPY_PORT, tapeServer->serverName());

  try {
    rtcpConnection.connect();
  } catch (castor::exception::Exception e) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Failed to connect to RTCopyD: " << e.getMessage().str();

    throw ie;
  }
      
  bool acknSucc = true;

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeRequestID", request->id()),
      castor::dlf::Param("clientUserName", client->userName()),
      castor::dlf::Param("clientMachine", client->machine()),
      castor::dlf::Param("clientPort", client->port()),
      castor::dlf::Param("clientEuid", client->euid()),
      castor::dlf::Param("clientEgid", client->egid()),
      castor::dlf::Param("deviceGroupName", dgn->dgName()),
      castor::dlf::Param("tapeDriveName", tapeDrive->driveName())};

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, VDQM_SEND_RTCPD_JOB, 8,
      params);
  }

  try {
    acknSucc = rtcpConnection.sendJobToRTCPD(request->id(),
      client->userName(), client->machine(), client->port(), client->euid(),
      client->egid(), dgn->dgName(), tapeDrive->driveName()
    );
  } catch (castor::exception::Exception e) {
    castor::exception::Internal ie;

    ie.getMessage()
      << "Failed to send job to RTCPD: " << e.getMessage().str();

    throw ie;
  }

  if(!acknSucc) {
    castor::exception::Internal ie;

    ie.getMessage() <<
      "Did not receive an acknSucc from sending a job to RTCPD";

    throw ie;
  }
}
