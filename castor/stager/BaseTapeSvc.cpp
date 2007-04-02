/******************************************************************************
 *                      BaseTapeSvc.cpp
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
 * @(#)$RCSfile: BaseTapeSvc.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/04/02 15:26:04 $ $Author: sponcec3 $
 *
 * Basic implementation of ITapeSvc.
 * This class only implements the functions that are not DB related.
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files
#include "castor/stager/BaseTapeSvc.hpp"
#include "castor/monitoring/StreamDirection.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "getconfent.h"
#include <string>

#define RMMASTER_PORT 15003

// -----------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------
castor::stager::BaseTapeSvc::BaseTapeSvc()
  throw(castor::exception::Exception) :
  m_rmMasterHost(""), m_rmMasterPort(RMMASTER_PORT) {
  // Try to retrieve the rmmaster machine and port from the
  // config file. No default is provided for the machine.
  char* rmMasterHost = getconfent("RM","HOST", 0);
  if (0 == rmMasterHost) {
    // Raise an exception
    castor::exception::NoEntry e;
    e.getMessage() << "Found not entry RM/HOST in config file";
    throw e;
  }
  m_rmMasterHost = rmMasterHost;
  char* rmMasterPortStr = getconfent("RM","PORT", 0);
  if (rmMasterPortStr){
    m_rmMasterPort = std::strtol(rmMasterPortStr,0,10);
    if (0 == m_rmMasterPort) {
      // Go back to default
      m_rmMasterPort = RMMASTER_PORT;
      // "Bad rmmaster port value in configuration file"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Message", "Bad rmmaster port value in configuration file, default used"),
	 castor::dlf::Param("Given value", rmMasterPortStr)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 0, 2, initParams);
    }
  }    
}

// -----------------------------------------------------------------------
// sendStreamReport
// -----------------------------------------------------------------------
void castor::stager::BaseTapeSvc::sendStreamReport
(const std::string diskServer,
 const std::string fileSystem,
 const castor::monitoring::StreamDirection direction,
 const bool created) throw () {
  try {
    castor::io::UDPSocket s(m_rmMasterPort, m_rmMasterHost);
    castor::monitoring::StreamReport sr;
    sr.setDiskServerName(diskServer);
    sr.setMountPoint(fileSystem);
    sr.setDirection(direction);
    sr.setCreated(created);
    s.sendObject(sr);
  } catch (castor::exception::Exception ex) {
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Message", "Failed to send StreamReport to rmMasterDaemon via UDP. Exception Caught."),
       castor::dlf::Param("Original Error", ex.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 0, 2, initParams);
  }
}
