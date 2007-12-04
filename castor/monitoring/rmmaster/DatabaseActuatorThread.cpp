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
 * @(#)$RCSfile: DatabaseActuatorThread.cpp,v $ $Author: waldron $
 *
 * The DatabaseActuator thread of the RmMaster daemon. It updates the stager
 * database with monitoring data
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include files
#include "castor/Services.hpp"
#include "castor/System.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/monitoring/rmmaster/IRmMasterSvc.hpp"
#include "castor/monitoring/rmmaster/DatabaseActuatorThread.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::DatabaseActuatorThread::DatabaseActuatorThread
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) :
  m_rmMasterService(0),
  m_clusterStatus(clusterStatus),
  m_prevMasterName("") {

  // "DatabaseAcutator thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 33, 0, 0);

  // Initialize the oracle rmmaster service.
  castor::IService *orasvc =
    castor::BaseObject::services()->service("OraRmMasterSvc",
					    castor::SVC_ORARMMASTERSVC);
  if (orasvc == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraRmMasterSVC";
    throw e;
  }
  m_rmMasterService =
    dynamic_cast<castor::monitoring::rmmaster::IRmMasterSvc *>(orasvc);
  if (m_rmMasterService == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into "
		   << "IRmMasterSvc" << std::endl;
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::DatabaseActuatorThread::~DatabaseActuatorThread()
  throw() {
  m_rmMasterService = 0;
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::DatabaseActuatorThread::run(void* par)
  throw() {

  // "DatabaseActuator thread running"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 32, 0, 0);

  // Determine whether or not we are operating as the master
  bool master = false;
  try {
    std::string masterName =
      castor::monitoring::rmmaster::LSFSingleton::getInstance()->
      getLSFMasterName();
    std::string hostName = castor::System::getHostName();
    if (masterName == hostName) {
      master = true;
    }

    // Announce if we have changed from master to slave or vice versa
    if ((m_prevMasterName != "") && (m_prevMasterName != masterName)) {
      if (master) {
	// "Assuming role as production RmMaster server. LSF failover detected"
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 47, 0, 0);
      } else if (m_prevMasterName == hostName) {
	// "Assuming role as slave RmMaster server. LSF failover detected"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Master", masterName)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 48, 1, params);
      }
    }
    m_prevMasterName = masterName;

  } catch (castor::exception::Exception e) {

    // "Failed to determine the hostname of the LSF master"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Function", "DatabaseActuatorThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 46, 3, params);
    return;
  }

  // If we are the slave we need to retrieve the cluster status not update it!
  try {
    if (m_rmMasterService != 0) {
      if (master == true) {
	m_rmMasterService->storeClusterStatus(m_clusterStatus);
      } else {
	m_rmMasterService->retrieveClusterStatus(m_clusterStatus, true, false);
      }
    }
  } catch(castor::exception::Exception e) {
    // "Failed to synchronise shared memory with stager database"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 44, 2, params);
  } catch (...) {
    // "General exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "DatabaseActuatorThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 1, params);
  }
}
