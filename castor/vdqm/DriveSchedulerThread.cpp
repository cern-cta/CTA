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

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/vdqm/DriveAndRequestPair.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"


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
    obj = vdqmSvc->matchTape2TapeDrive();
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

  try
  {
    allocateDrive(param);
  }
  catch(castor::exception::Exception &e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", "DriveSchedulerThread::process"),
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("Code", e.code())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_DRIVE_ALLOCATION_ERROR, 3, params);
  }

  // Clean up
  if(param != 0)
  {
    // The destructor of castor::vdqm::DriveAndRequestPair deletes the tape
    // drive and tape request objects
    delete param;
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


//-----------------------------------------------------------------------------
// allocateDrive
//-----------------------------------------------------------------------------
void castor::vdqm::DriveSchedulerThread::allocateDrive(castor::IObject *param)
  throw(castor::exception::Exception) {
  std::cout << "castor::vdqm::DriveSchedulerThread::allocateDrive" << std::endl;
}
