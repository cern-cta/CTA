/******************************************************************************
 *                      PortsConfig.cpp
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
 * @(#)$RCSfile: PortsConfig.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/11/29 14:39:15 $ $Author: itglp $
 *  
 * A service to provide parameters to access the db layer of a Castor application
 *
 * @author castor dev team
 *****************************************************************************/

#include <iostream>
#include <errno.h>
#include <serrno.h>
#include <getconfent.h>
#include <Cmutex.h>
#include "castor/PortsConfig.hpp"
#include "castor/exception/InvalidArgument.hpp"

castor::PortsConfig* castor::PortsConfig::s_instance(0);

//------------------------------------------------------------------------------
// getInstance
//------------------------------------------------------------------------------
castor::PortsConfig* castor::PortsConfig::getInstance()
  throw (castor::exception::Exception) {
  // make the instantiation of the singleton thread-safe,
  // even though this class is supposed to be instantiated before spawning threads
  if (0 == s_instance) {
    Cmutex_lock(&s_instance, -1);
    if (0 == s_instance) {
      s_instance = new castor::PortsConfig();
    }
    Cmutex_unlock(&s_instance);
  }
  return s_instance;
}


// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::PortsConfig::PortsConfig() throw (castor::exception::Exception) :
  BaseObject() {
    m_hosts.push_back(getConfigHost("STAGER"));
    m_ports.push_back(getConfigPort("STAGER", STAGER_DEFAULT_NOTIFYPORT));
    m_hosts.push_back(getConfigHost("JOBMANAGER"));
    m_ports.push_back(getConfigPort("JOBMANAGER", JOBMANAGER_DEFAULT_NOTIFYPORT));
    //m_hosts.push_back(getConfigHost("RTCPCLIENTD"));
    //m_ports.push_back(getConfigPort("RTCPCLIENTD", RTCPCLIENTD_DEFAULT_NOTIFYPORT));
}

// -----------------------------------------------------------------------
// getConfigPort
// -----------------------------------------------------------------------
int castor::PortsConfig::getConfigPort(const char* configLabel, unsigned defaultValue)
  throw (castor::exception::Exception) {
  const char* value;
  int notifyPort = defaultValue;

  if ((value = getconfent((char*)configLabel, "NOTIFYPORT", 0))) {
    notifyPort = strtol(value, 0, 10);
    if (notifyPort <= 0 || notifyPort > 65535) {
      castor::exception::InvalidArgument e;
      e.getMessage() << "Invalid value configured for " << configLabel <<
        "_NOTIFYPORT: " << notifyPort << " - must be < 65535";
      throw e;
    }
  }
  return notifyPort;
}

// -----------------------------------------------------------------------
// getConfigHost
// -----------------------------------------------------------------------
std::string castor::PortsConfig::getConfigHost(const char* configLabel)
  throw (castor::exception::Exception) {
  const char* value;

  // XXX we need to first try and read the NOTIFYHOST value, for the stager...
  if (!(value = getconfent((char*)configLabel, "NOTIFYHOST", 0)) &&
      !(value = getconfent((char*)configLabel, "HOST", 0))) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid or missing value configured for "
      << configLabel << "_HOST: " << value;
    throw e;
  }
  return value;
}
