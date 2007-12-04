/******************************************************************************
 *                      HeartbeatThread.cpp
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
 * @(#)$RCSfile: HeartbeatThread.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2007/12/04 13:25:55 $ $Author: waldron $
 *
 * The Heartbeat thread of the rmMasterDaemon is responsible for checking all
 * disk servers in shared memory and automatically disabling them if no data
 * has been received from them with X number of seconds.
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/monitoring/rmmaster/HeartbeatThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/System.hpp"
#include "getconfent.h"
#include <time.h>

// Definitions
#define DEFAULT_TIMEOUT 60


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::HeartbeatThread::HeartbeatThread
(castor::monitoring::ClusterStatus* clusterStatus) :
  m_clusterStatus(clusterStatus),
  m_startup(time(NULL)),
  m_timeout(DEFAULT_TIMEOUT),
  m_lastPause(time(NULL)) {

  // "Heartbeat thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 36, 0, 0);

  // Extract heartbeat timeout value
  char *value = getconfent("RmMaster", "HeartbeatTimeout", 0);
  if (value) {
    m_timeout = std::strtol(value, 0, 10);
    if (m_timeout == 0) {
      m_timeout = DEFAULT_TIMEOUT;
      // "Invalid RmMaster/HeartbeatInterval option, using default"
      castor::dlf::Param initParams[] =
        {castor::dlf::Param("Default", value)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 18, 1, initParams);
    }
  }
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::HeartbeatThread::run(void* par) throw() {
  try {

    // "Heartbeat thread running"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 37, 0, 0);

    // Heartbeat check disabled ?
    if (m_timeout == -1) {
      return;
    }

    // If operating in slave mode do nothing!
    try {
      std::string masterName =
	castor::monitoring::rmmaster::LSFSingleton::getInstance()->
	getLSFMasterName();
      std::string hostName = castor::System::getHostName();
      if (masterName != hostName) {
	m_lastPause = time(NULL);
	return;
      }
      // If we just became the resource monitoring master then wait some time
      // for the diskservers or database actuator thread to update the shared
      // memory before disabling diskservers.
      else if ((u_signed64)time(NULL) < (u_signed64)(time(NULL) + (m_timeout * 2))) {
	return;
      }
    } catch (castor::exception::Exception e) {

      // "Failed to determine the hostname of the LSF master"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("Function", "HeartbeatThread::run")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 46, 3, params);
      return;
    }

    // If the rmMasterDaemon has just started we cannot trust the information
    // in the sharedMemory. Wait at least 2 * the timeout value before checking
    // machines
    if (m_startup > (u_signed64)(time(NULL) - (m_timeout * 2))) {
      return;
    }

    // Check all disk servers
    bool changed = false;
    for (castor::monitoring::ClusterStatus::iterator it =
	   m_clusterStatus->begin();
	 it != m_clusterStatus->end();
	 it++, changed = false) {

      // Ignore deleted and already disabled disk servers
      if ((it->second.status() == castor::stager::DISKSERVER_DISABLED) ||
	  (it->second.adminStatus() == castor::monitoring::ADMIN_DELETED)) {
	continue;
      }
      if (it->second.lastMetricsUpdate() < (u_signed64)(time(NULL) - m_timeout)) {
	if (it->second.adminStatus() == castor::monitoring::ADMIN_NONE) {
	  it->second.setStatus(castor::stager::DISKSERVER_DISABLED);
	  changed = true;
	}

	// Remove file systems from production
	for (castor::monitoring::DiskServerStatus::iterator it2 =
	       it->second.begin();
	     it2 != it->second.end();
	     it2++) {
	  if (it2->second.adminStatus() == castor::monitoring::ADMIN_NONE) {
	    if (it2->second.status() != castor::stager::FILESYSTEM_DISABLED) {
	      it2->second.setStatus(castor::stager::FILESYSTEM_DISABLED);
	      changed = true;
	    }
	  }
	}
      }

      if (changed) {
	// "Heartbeat check failed for diskserver, status changed to DISABLED"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Hostname", it->first.c_str())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 38, 1, params);
      }
    }
  } catch (...) {
    // "General exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "HeartbeatThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 1, params);
  }
}
