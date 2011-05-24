/******************************************************************************
 *                      LSFStatus.cpp
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
 * @(#)$RCSfile: LSFStatus.cpp,v $ $Author: waldron $
 *
 * Singleton used to access LSF related information
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
// Note that OraRmMasterSvc has to be included BEFORE LSFStatus, as it includes ORACLE
// header files that clash with LSF ones for some macro definitions !
#include "castor/monitoring/rmmaster/ora/OraRmMasterSvc.hpp"
#include "castor/monitoring/rmmaster/LSFStatus.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/System.hpp"
#include "castor/db/newora/OraCnvSvc.hpp"
#include "Cmutex.h"
#include "Cthread_api.h"
#include "getconfent.h"

//-----------------------------------------------------------------------------
// Space declaration for the static LSFStatus instance
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFStatus *
castor::monitoring::rmmaster::LSFStatus::s_instance(0);

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFStatus::LSFStatus()
  throw(castor::exception::Exception) :
  m_rmMasterService(0),
  m_cnvSvc(0),
  m_prevMasterName(""),
  m_prevProduction(false),
  m_lastUpdate(0),
  m_getLSFStatusCalled(false) {

  // Check whether we run with or without LSF
  char *noLSFValue = getconfent("RmMaster", "NoLSFMode", 0);
  m_noLSF = (noLSFValue != NULL && strcasecmp(noLSFValue, "yes") == 0);

  if (m_noLSF) {
    // initialize the OraRmMasterSvc. Note that we do not use here the standard,
    // thread specific service. We create our own instance of it so that we have
    // a private, dedicated connection to ORACLE that will not be commited by
    // any other activity of the thread.
    m_cnvSvc = new castor::db::ora::OraCnvSvc("DbCnvSvc");
    if (0 == m_cnvSvc) {
      castor::exception::Internal e;
      e.getMessage() << "Unable to create an OraCnvSvc";
      throw e;
    }
    m_rmMasterService =
      new castor::monitoring::rmmaster::ora::OraRmMasterSvc("LSFStatusOraSvc", m_cnvSvc);
    if (0 == m_rmMasterService) {
      castor::exception::Internal e;
      e.getMessage() << "Unable to create an OraRmMasterSVC";
      throw e;
    }
    // Initialize checkMasterLock
    int rv = pthread_mutex_init(&m_masterCheckLock, NULL);
    if (rv != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Failed to pthread_mutex_init(m_masterCheckLock)";
      throw e;
    }
  } else {
    // Initialize the LSF library
    if (lsb_init((char*)"RmMasterDaemon") < 0) {
      
      // "Failed to initialize the LSF batch library (LSBLIB)"
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Failed to initialize the LSF batch library (LSBLIB): "
                     << lsberrno ? lsb_sysmsg() : "no message";
      throw e;
    }
  }
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFStatus::~LSFStatus() throw() {
  // release the services that we have created locally.
  // Note that this will in particular disconnect from ORACLE and thus release
  // the lock on the RmMasterLock table that we may have hold if we were the
  // master rmMaster. A new session will then be able to take this lock and
  // become the master
  if (0 != m_rmMasterService) delete m_rmMasterService;
  if (0 != m_cnvSvc) delete m_cnvSvc;
  if (m_noLSF) pthread_mutex_destroy(&m_masterCheckLock);
}


//-----------------------------------------------------------------------------
// GetInstance
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFStatus *
castor::monitoring::rmmaster::LSFStatus::getInstance()
  throw(castor::exception::Exception) {
  // Thread safe instantiation of the singleton
  if (s_instance == 0) {
    Cmutex_lock(&s_instance, -1);
    if (s_instance == 0) {
      s_instance = new castor::monitoring::rmmaster::LSFStatus();
    }
    Cmutex_unlock(&s_instance);
  }
  return s_instance;
}


//-----------------------------------------------------------------------------
// GetLSFStatus
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::LSFStatus::getLSFStatus
(bool &production, bool update)
  throw(castor::exception::Exception) {
  std::string masterName, hostName;
  getLSFStatus(production, masterName, hostName, update);
}


//-----------------------------------------------------------------------------
// GetLSFStatus
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::LSFStatus::getLSFStatus
(bool &production, std::string &masterName, std::string &hostName, bool update)
  throw(castor::exception::Exception) {

  if (m_noLSF) {

    // we are running in no LSF mode. The status here is given by the ability to
    // take a lock in the DB. The one that has this lock is declared the master.
    // If it dies, somebody else will be able to take the lock and will become the
    // new master.

    // For stability reasons we cache the result of the query to the ORACLE DB and only
    // refresh it when updating is enabled and 10 seconds has passed since the
    // previous query.
    if ((!m_getLSFStatusCalled) || (update && ((time(NULL) - m_lastUpdate) > 10))) {
      // serialize the call to isMonitoringMaster that is not thread safe
      pthread_mutex_lock(&m_masterCheckLock);
      // ask the DB whether we are the master
      try {
        production = m_rmMasterService->isMonitoringMaster();
      } catch(castor::exception::Exception e) {
        pthread_mutex_unlock(&m_masterCheckLock);
        throw e;
      }
      // release the serialization lock
      pthread_mutex_unlock(&m_masterCheckLock);
      // remember lass update time, for the caching
      m_lastUpdate = time(NULL);
    } else {
      production = m_prevProduction;
    }

    // fill the hostname and mastername
    hostName = castor::System::getHostName();
    if (production) {
      masterName = hostName;
    } else {
      // we are not the master. We do not know who it is, but we can build a list of candidates
      char** hosts;
      int count;
      int rc = getconfent_multi("DiskManager", "ServerHosts", 1, &hosts, &count);
      if (0 == rc) {
        // loop through candidates and only remove ourselves
        int nbCandidates = 0;
        masterName = "";
        for (int i = 0; i < count; i++) {
          if (0 != strcmp (hosts[i], hostName.c_str())) {
            if (nbCandidates > 0) masterName += ", ";
            masterName += hosts[i];
            nbCandidates++;
          }
        }
        if (nbCandidates > 1) masterName = "one of " + masterName;
        // cleanup memory allocated by getconfent_multi
        for (int i = 0; i < count; i++) free(hosts[i]);
        free(hosts);
      } else {
        // no entry in config file about the possible candidates, giving up
        masterName = "unknown";
      }
    }
    
    // Announce if we have changed status
    if (!m_getLSFStatusCalled) {
      m_getLSFStatusCalled = true;
      // "Initialization information"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Master", masterName)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 49, 1, params);
    } else if (m_prevProduction != production) {
      if (production) {
        // "Assuming role as production RmMaster server, failover detected"
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 47, 0, 0);
      } else {
        // "Assuming role as slave RmMaster server, failover detected"
        castor::dlf::Param params[] =
  	{castor::dlf::Param("Master", masterName)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 48, 1, params);
      }
    }
    // remember current state
    m_prevProduction = production;

  } else {

    // Set the defaults
    production = true;
    masterName = hostName = "";

    // For stability reasons we cache the result of the query to LSF and only
    // refresh it when updating is enabled and 1 minute has passed since the
    // previous query.
    std::string clusterName("");
    if ((m_prevMasterName == "") || (update && ((time(NULL) - m_lastUpdate) > 60))) {

      // Get the name of the LSF master. This is the equivalent to `lsid`
      char **results = NULL;
      clusterInfo *cInfo = ls_clusterinfo(NULL, NULL, results, 0, 0);
      if (cInfo == NULL) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "LSF reported : " << lsb_sysmsg();
        throw e;
      }

      Cthread_mutex_lock(&m_prevMasterName);
      clusterName = cInfo[0].clusterName;
      masterName  = cInfo[0].masterName;
      m_lastUpdate = time(NULL);
    } else {
      Cthread_mutex_lock(&m_prevMasterName);
      masterName = m_prevMasterName;
    }

    // Determine the hostname of the machine we are currently running on. If
    // its different to the LSF master then we are the slave
    try {
      hostName = castor::System::getHostName();
      if (masterName != hostName) {
        production = false;
      }
    } catch (castor::exception::Exception& e) {
      Cthread_mutex_unlock(&m_prevMasterName);
      throw e;
    }

    // Announce if we have changed status
    if (m_prevMasterName == "") {
      // "Initialization information"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Cluster", clusterName),
         castor::dlf::Param("Master", masterName)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 49, 2, params);
    } else if (m_prevMasterName != masterName) {
      if (production) {
        // "Assuming role as production RmMaster server, failover detected"
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 47, 0, 0);
      } else if (m_prevMasterName == hostName) {
        // "Assuming role as slave RmMaster server, failover detected"
        castor::dlf::Param params[] =
  	{castor::dlf::Param("Master", masterName)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 48, 1, params);
      }
    }

    // Return
    m_prevMasterName = masterName;
    Cthread_mutex_unlock(&m_prevMasterName);
  }
}
