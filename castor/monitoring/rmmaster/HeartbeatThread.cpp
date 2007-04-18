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
 * @(#)$RCSfile: HeartbeatThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/04/18 14:18:14 $ $Author: waldron $
 *
 * The Heartbeat thread of the rmMasterDaemon is responsible for checking all
 * disk servers in shared memory and automatically disabling them if no data
 * has been received from them with X number of seconds.
 *
 * @author Dennis Waldron
 *****************************************************************************/

#include "castor/monitoring/rmmaster/HeartbeatThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/exception/Exception.hpp"
#include <time.h>

#define DEFAULT_TIMEOUT 60 // in seconds

// -----------------------------------------------------------------------
// External C function used for getting configuration from shift.conf file
// -----------------------------------------------------------------------

extern "C" {
  char* getconfent (const char *, const char *, int);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::HeartbeatThread::HeartbeatThread 
(castor::monitoring::ClusterStatus* clusterStatus) :
  m_clusterStatus(clusterStatus),
  m_startup(time(NULL)) {
  //"Heartbeat thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 36, 0, 0);
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::HeartbeatThread::run(void* par) throw() {
  try { // no exeption should go out
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 37, 0, 0);
    // extract heartbeat timeout value
    int timeout = DEFAULT_TIMEOUT;    
    char *value = getconfent("RmMaster", "HeartbeatTimeout", 0);
    if (value) {
      timeout = std::strtol(value, 0, 10);
      if (0 == timeout) {
	timeout = DEFAULT_TIMEOUT;
	castor::dlf::Param initParams[] =
	  {castor::dlf::Param("Given value", value)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 18, 1, initParams);
      }
    }
    // heartbeat check disabled ?
    if (timeout == -1) {
      return;
    }
    // if the rmMasterDaemon has just started we cannot trust the information
    // in the sharedMemory. Wait at least 2 * the timeout value before checking
    // machines
    if (m_startup > (u_signed64)(time(NULL) - (timeout * 2))) {
      return;
    }
    // check all disk servers
    for (castor::monitoring::ClusterStatus::iterator it =
	   m_clusterStatus->begin();
	 it != m_clusterStatus->end();
	 it++) {
      // ignore deleted and already disabled disk servers
      if ((it->second.status() == castor::stager::DISKSERVER_DISABLED) ||
	  (it->second.adminStatus() == castor::monitoring::ADMIN_DELETED)) {
	continue;
      }
      if (it->second.lastMetricsUpdate() < (u_signed64)(time(NULL) - timeout)) { 
	it->second.setStatus(castor::stager::DISKSERVER_DISABLED);
	// remove disk server from production
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Machine Name", it->first.c_str())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 38, 1, params);
	// remove file systems from production
	for (castor::monitoring::DiskServerStatus::iterator it2 =
	       it->second.begin();
	     it2 != it->second.end();
	     it2++) {
	  it2->second.setStatus(castor::stager::FILESYSTEM_DISABLED);
	}
      }
    }
  } catch (...) {
    // "General exception caught"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 0, 0);
  }
}
