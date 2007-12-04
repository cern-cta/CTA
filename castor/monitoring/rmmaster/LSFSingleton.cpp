/******************************************************************************
 *                      LSFHelper.cpp
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
 * @(#)$RCSfile: LSFSingleton.cpp,v $ $Author: waldron $
 *
 * Singleton used to access LSF related information
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/monitoring/rmmaster/LSFSingleton.hpp"
#include "Cmutex.h"
#include "Cthread_api.h"


//-----------------------------------------------------------------------------
// Space declaration for the static LSFSingleton instance
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFSingleton *
  castor::monitoring::rmmaster::LSFSingleton::s_instance(0);

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFSingleton::LSFSingleton() 
  throw(castor::exception::Exception) :
  m_masterName(""),
  m_lastUpdate(0) {
  
  // Initialize the LSF library
  if (lsb_init("RmMasterDaemon") < 0) {
    
    // "Failed to initialize the LSF batch library (LSBLIB)"
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to initialize the LSF batch library (LSBLIB): " 
		   << lsberrno ? lsb_sysmsg() : "no message";
    throw e;
  }
}


//-----------------------------------------------------------------------------
// GetInstance
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::LSFSingleton *
castor::monitoring::rmmaster::LSFSingleton::getInstance() 
  throw(castor::exception::Exception) {
  // Thread safe instantiation of the singleton
  if (s_instance == 0) {
    Cmutex_lock(&s_instance, -1);
    if (s_instance == 0) {
      s_instance = new castor::monitoring::rmmaster::LSFSingleton();
    }
    Cmutex_unlock(&s_instance);
  }
  return s_instance;
}


//-----------------------------------------------------------------------------
// GetLSFMasterName
//-----------------------------------------------------------------------------
std::string castor::monitoring::rmmaster::LSFSingleton::getLSFMasterName()
  throw(castor::exception::Exception) {

  // For stability reasons we cache the result of the query to LSF and only
  // refresh it every 5 minutes.
  Cthread_mutex_lock(&m_masterName);
  char *buf = NULL;
  if ((m_masterName != "") && ((time(NULL) - m_lastUpdate) < 300)) {
    buf = strdup(m_masterName.c_str());
  } else {
  
    // Get the name of the LSF master. This is the equivalent to `lsid`
    char **results = NULL;
    clusterInfo *cInfo = ls_clusterinfo(NULL, NULL, results, 0, 0);
    if (cInfo != NULL) {
      buf = strdup(cInfo[0].masterName);
    } else {
      Cthread_mutex_unlock(&m_masterName);
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << lsberrno ? lsb_sysmsg() : "no message";
      throw e;
    }
    m_lastUpdate = time(NULL);
  }

  // Return the result
  std::string result(buf);
  m_masterName = result;
  Cthread_mutex_unlock(&m_masterName);

  return result;
}
