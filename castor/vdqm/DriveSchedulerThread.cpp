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


#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "h/getconfent.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <stdlib.h>

//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::vdqm::DriveSchedulerThread::run(void*) {

  Cuuid_t                cuuid    = nullCuuid;
  castor::vdqm::IVdqmSvc *vdqmSvc = NULL;

  Cuuid_create(&cuuid);

  try {
    vdqmSvc = getDbVdqmSvc();
  } catch(castor::exception::Exception &e) {
    // "Could not get DbVdqmSvc"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "castor::vdqm::DriveSchedulerThread::run"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
      3, params);

    return;
  }


  // 1 = drive allocated, 0 = no possible allocation found, -1 possible
  // allocation found, but invalidated by other threads
  int allocationResult = 0;

  try {
    u_signed64  tapeDriveId;
    std::string tapeDriveName;
    u_signed64  tapeRequestId;
    std::string tapeRequestVid;

    allocationResult = vdqmSvc->allocateDrive(&tapeDriveId, &tapeDriveName,
      &tapeRequestId, &tapeRequestVid);
    vdqmSvc->commit();

    // If a drive was allocated or a possible drive allocation was found, but
    // was invalidated by other threads
    if((allocationResult == 1) || (allocationResult == -1)){
      castor::dlf::Param param[] = {
        castor::dlf::Param("tapeDriveID"  , tapeDriveId),
        castor::dlf::Param("driveName"    , tapeDriveName),
        castor::dlf::Param("tapeRequestID", tapeRequestId),
        castor::dlf::Param("tapeVID"      , tapeRequestVid)};

      if(allocationResult == 1) { // Drive allocated
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
          VDQM_DRIVE_ALLOCATED, 4, param);
      } else { // Invalidated drive allocation
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
          VDQM_INVALIDATED_DRIVE_ALLOCATION, 4, param);
      }
    }
  } catch (castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code", ex.code())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_MATCHTAPE2TAPEDRIVE_ERROR, 3, params);
  }

  // If a drive was allocated
  if(allocationResult == 1) {
    // Notiify the RTCP job submitter threads
    castor::server::NotifierThread::getInstance()->doNotify('J');
  }

  // Delete any old volume priorities
  try {
    // Start with the maximum age of a volume priority being the compile-time
    // default
    unsigned int maxVolPriorityAge = s_maxVolPriorityAge;

    // Overwrite the compile-time default with the value in the configuration
    // file if there is one and it is greater than 0
    char *const maxVolPriorityAgeConfigStr =
      getconfent("VDQM", "MAXVOLPRIORITYAGE", 0);
    if(maxVolPriorityAgeConfigStr != NULL) {
      int const maxVolPriorityAgeConfig = atoi(maxVolPriorityAgeConfigStr);

      if(maxVolPriorityAgeConfig > 0) {
        maxVolPriorityAge = (unsigned int)maxVolPriorityAgeConfig;
      }
    }

    unsigned int prioritiesDeleted =
      vdqmSvc->deleteOldVolPriorities(maxVolPriorityAge);

    if(prioritiesDeleted > 0) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("prioritiesDeleted",prioritiesDeleted)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        VDQM_DEL_OLD_VOL_PRIORITIES, 1, params);;
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code", ex.code())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
      VDQM_DEL_OLD_VOL_PRIORITIES_ERROR, 3, params);
  }
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
