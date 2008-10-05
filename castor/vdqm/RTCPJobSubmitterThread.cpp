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
    vdqmSvc->commit();
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

  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;

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
  } catch(castor::exception::Exception &e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function",
        "castor::vdqm::RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
      3, params);

    return;
  }

  TapeDrive *tapeDrive = request.get()->tapeDrive();

  try {

    // Submit the remote tape copy job to RTCPD
    submitJobToRTCPD(request.get());

    try
    {
      // Write successful submission of RTCPD job to the database
      if(!vdqmSvc->writeRTPCDJobSubmission(tapeDrive->id(),
        request.get()->id())) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("tapeDriveID", tapeDrive->id()),
          castor::dlf::Param("driveName", tapeDrive->driveName()),
          castor::dlf::Param("tapeRequestID", request.get()->id())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
          VDQM_IGNORED_RTCPD_JOB_SUBMISSION, 3, params);
      }
    } catch(castor::exception::Exception &e) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("tapeDriveID", tapeDrive->id()),
        castor::dlf::Param("driveName", tapeDrive->driveName()),
        castor::dlf::Param("tapeRequestID", request.get()->id()),
        castor::dlf::Param("Message", "Failed to write successful submission "
          "of RTCPD job to the database: " + e.getMessage().str()),
        castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        VDQM_DRIVE_ALLOCATION_ERROR, 5, params);
    }
  } catch(castor::exception::Exception &e) {
    // Log failure of RTPCD job submission
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeDriveID", tapeDrive->id()),
      castor::dlf::Param("driveName", tapeDrive->driveName()),
      castor::dlf::Param("tapeRequestID", request.get()->id()),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
      VDQM_RTCPD_JOB_SUBMIT_FAILED, 5, params);

    try {
      // Write failed submission of RTCPD job to the database
      if(!vdqmSvc->writeFailedRTPCDJobSubmission(tapeDrive->id(),
        request.get()->id())) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("tapeDriveID", tapeDrive->id()),
          castor::dlf::Param("driveName", tapeDrive->driveName()),
          castor::dlf::Param("tapeRequestID", request.get()->id())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
          VDQM_IGNORED_FAILED_RTCPD_JOB_SUBMISSION, 3, params);
      }
    } catch(castor::exception::Exception &e) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("tapeDriveID", tapeDrive->id()),
        castor::dlf::Param("driveName", tapeDrive->driveName()),
        castor::dlf::Param("tapeRequestID", request.get()->id()),
        castor::dlf::Param("Message", "Failed to write failed submission "
          "of RTCPD job to the database: " + e.getMessage().str()),
        castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        VDQM_DRIVE_ALLOCATION_ERROR, 5, params);
    }
  }

  try
  {
    // Commit the changes to the DB
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    services()->commit(&ad);
  } catch(castor::exception::Exception &e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "RTCPJobSubmitterThread::process"),
      castor::dlf::Param("Message", "Failed to commit changes" +
        e.getMessage().str()),
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
