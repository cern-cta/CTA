/******************************************************************************
 *                      DatabaseActuatorThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: DatabaseActuatorThread.cpp,v $ $Author: sponcec3 $
 *
 * The DatabaseActuator thread of the RmMaster daemon.
 * It updates the stager database with monitoring data
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/monitoring/rmmaster/IRmMasterSvc.hpp"
#include "castor/monitoring/rmmaster/DatabaseActuatorThread.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::DatabaseActuatorThread::DatabaseActuatorThread
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) :
  m_rmMasterService(0), m_clusterStatus(clusterStatus) {
  // "Collector thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 33);
  castor::IService* orasvc = castor::BaseObject::services()-> service("OraRmMasterSvc", castor::SVC_ORARMMASTERSVC);
  if (0 == orasvc) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraRmMasterSvc";
    throw e;
  }
  m_rmMasterService = dynamic_cast<castor::monitoring::rmmaster::IRmMasterSvc*>(orasvc);
  if (0 == m_rmMasterService) {
    castor::exception::Internal e;
    e.getMessage() << "Could Not cast newly retrieved service into IRmMasterSvc" << std::endl;
    throw e;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::DatabaseActuatorThread::~DatabaseActuatorThread() throw() {
  m_rmMasterService = 0;
}

//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::DatabaseActuatorThread::run
(void* par) throw() {
  // "Thread DatabaseActuator started. Updating shared memory data."
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 32, 0, 0);
  try{
    if (m_rmMasterService != 0) {
      m_rmMasterService->syncClusterStatus(m_clusterStatus);
    }
  }
  catch(castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
  }
  catch (...) {
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 1, params2);
  }
}
