/******************************************************************************
 *                     RmMasterTapeGatewayHelper.cpp 
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
 * @(#)$RCSfile: RmMasterTapeGatewayHelper.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2009/07/23 12:22:01 $ $Author: waldron $
 *
 * @author castor-dev team
 *****************************************************************************/

// Include Files

#include <string>
#include <cstdlib>

#include "getconfent.h"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/monitoring/StreamDirection.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include "castor/tape/tapegateway/daemon/RmMasterTapeGatewayHelper.hpp"

#define RMMASTER_PORT 15003

// -----------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------

castor::tape::tapegateway::RmMasterTapeGatewayHelper::RmMasterTapeGatewayHelper()
  throw(castor::exception::Exception) :
  m_rmMasterHost(""), m_rmMasterPort(RMMASTER_PORT) {
  // Try to retrieve the rmmaster machine and port from the
  // config file. No default is provided for the machine.
  char* rmMasterHost = getconfent("RM","HOST", 0);
  if (0 == rmMasterHost) {
    // Raise an exception
    castor::exception::NoEntry e;
    e.getMessage() << "Found null entry RM/HOST in config file";
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

      castor::exception::NoEntry e;
      e.getMessage() <<"Bad rmmaster port value in configuration file";
      throw e;
    }
  }
}

// -----------------------------------------------------------------------
// sendStreamReport
// -----------------------------------------------------------------------
void castor::tape::tapegateway::RmMasterTapeGatewayHelper::sendStreamReport
(const std::string diskServer,
 const std::string fileSystem,
 const castor::monitoring::StreamDirection direction,
 const bool created) throw (castor::exception::Exception) {
  try {
    castor::io::UDPSocket s(m_rmMasterPort, m_rmMasterHost);
    castor::monitoring::StreamReport sr;
    sr.setDiskServerName(diskServer);
    sr.setMountPoint(fileSystem);
    sr.setDirection(direction);
    sr.setCreated(created);
    s.sendObject(sr);
  } catch (castor::exception::Exception ex) {
    
    castor::exception::Internal e;
    e.getMessage() <<"Failed to send StreamReport to rmmaster daemon via UDP. Exception Caught"<<ex.getMessage().str();
    throw e;
  }
}
