/******************************************************************************
 *                castor/vdqm/DriveDedicationThread.cpp
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

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/vdqm/DriveDedicationThread.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveDedicationThread::DriveDedicationThread()
  throw () {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveDedicationThread::~DriveDedicationThread()
  throw () {
}


//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::vdqm::DriveDedicationThread::select()
  throw() {
  try {
    // Get the DbVdqmSvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
    castor::vdqm::IVdqmSvc *vdqmSvc =
      dynamic_cast<castor::vdqm::IVdqmSvc*>(svc);

    if (0 == vdqmSvc) {
      // "Could not get DbVdqmSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "DriveDedicationThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
        1, params);
      return 0;
    }

    // Actual work
//  castor::IObject* obj = vdqmSvc->matchTape2TapeDrive();
//  vdqmSvc->release();
//  return obj;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "DriveDedicationThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_EXCEPT, 3,
      params);
  }

  return 0;
}


//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::vdqm::DriveDedicationThread::process(castor::IObject *param)
  throw() {
  try {
    // Get the DbVdqmSvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
    castor::vdqm::IVdqmSvc *vdqmSvc =
      dynamic_cast<castor::vdqm::IVdqmSvc*>(svc);

    if (0 == vdqmSvc) {
      // "Could not get DbVdqmSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "DriveDedicationThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_GETSVC,
        1, params);
      return;
    }

  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "DriveDedicationThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, VDQM_DBVDQMSVC_EXCEPT, 3,
      params);
  }

  return;
}
