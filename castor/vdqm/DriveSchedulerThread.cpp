/******************************************************************************
 *                castor/vdqm/DriveSchedulerThread.cpp
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

#include <rtcp_constants.h>

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/ClientIdentification.hpp"
#include "castor/vdqm/DatabaseHelper.hpp"
#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/RTCopyDConnection.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/handler/TapeRequestDedicationHandler.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveSchedulerThread::DriveSchedulerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveSchedulerThread::~DriveSchedulerThread()
  throw () {
}


//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::vdqm::DriveSchedulerThread::select()
  throw() {

/*
  // Temporarily use OLD code which never returns
  try {
    castor::vdqm::handler::TapeRequestDedicationHandler::
      startOLDDriveSchedulerThreads();
  } catch(castor::exception::Exception &e) {
    std::cerr << "Failed to start old drive scheduler code: "
      << e.getMessage().str() << std::endl;
    exit(1);
  }
  return NULL;
*/

  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;
  castor::IObject        *obj     = NULL;


  try {
    vdqmSvc = getDbVdqmSvc();
  } catch(castor::exception::Exception &e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "DriveSchedulerThread::select"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
      3, params);

    return NULL;
  }

  try
  {
    obj = vdqmSvc->NEWmatchTape2TapeDrive();
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "DriveSchedulerThread::select"),
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
void castor::vdqm::DriveSchedulerThread::process(castor::IObject *param)
  throw() {

  castor::vdqm::TapeRequest* request =
    dynamic_cast<castor::vdqm::TapeRequest*>(param);

  // If the dynamic cast failed
  if(request == NULL) {
    castor::exception::Internal e;

    e.getMessage()
      << "Failed to cast param to castor::vdqm::TapeRequest*";

    throw e;
  }

  // Needed for the commit/rollback
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
    
  // Commit to the db
  // TBD: The whole commit state change persistency logic is not correct
  castor::BaseObject::services()->commit(&ad);

  // Allocate the drive
  try
  {

    allocateDrive(request);

    // "Update of representation in DB" message
    castor::dlf::Param params[] = {
       castor::dlf::Param("ID tapeDrive", request->tapeDrive()->id()),
       castor::dlf::Param("ID tapeRequest", request->id()),
       castor::dlf::Param("tapeDrive status", "UNIT_STARTING")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,
      VDQM_UPDATE_REPRESENTATION_IN_DB, 3, params);  

  } catch(castor::exception::Exception &e) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "DriveSchedulerThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_DRIVE_ALLOCATION_ERROR, 3, params);

    try
    {
      // Rollback
      castor::BaseObject::services()->rollback(&ad);
    } catch(castor::exception::Exception &e) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Function", "DriveSchedulerThread::process"),
        castor::dlf::Param("Message",
          "Failed to rollback database transaction"),
        castor::dlf::Param("Code", e.code())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        VDQM_EXCEPTION_IGNORED, 3, params);
      }
  }

  // Clean up
  delete request;
}


//-----------------------------------------------------------------------------
// getDbVdqmSvc
//-----------------------------------------------------------------------------
castor::vdqm::IVdqmSvc *castor::vdqm::DriveSchedulerThread::getDbVdqmSvc()
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
// allocateDrive
//-----------------------------------------------------------------------------
void castor::vdqm::DriveSchedulerThread::allocateDrive(
  castor::vdqm::TapeRequest* request) throw(castor::exception::Exception) {

  // castor::db::ora::OraVdqmSvc::matchTape2TapeDrive() ensures:
  //
  //   The tape request is linked to a set of client indentification data
  //   The tape request is linked to a tape drive
  //
  //   The tape drive of the tape request is linked to a device group name
  //   The tape drive of the tape request is linked to a tape server

  castor::vdqm::ClientIdentification *client    = request->client();
  castor::vdqm::TapeDrive              *tapeDrive = request->tapeDrive();

  castor::vdqm::DeviceGroupName *dgn        = tapeDrive->deviceGroupName();
  castor::vdqm::TapeServer      *tapeServer = tapeDrive->tapeServer();

  
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

  try {
    acknSucc = rtcpConnection.NEWsendJobToRTCPD(tapeDrive->id(),
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

    ie.getMessage() << "Did not receive a acknSucc from sending a job to RTCPD";

    throw ie;
  }
}
