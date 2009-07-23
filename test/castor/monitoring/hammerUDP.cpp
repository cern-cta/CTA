/******************************************************************************
 *                      hammerUDP.cpp
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
 * @(#)$RCSfile: hammerUDP.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/07/23 12:22:06 $ $Author: waldron $
 *
 * test program hammering the monitoring with UDP messages of
 * psuedo migrators and/or recallers in order to test
 * rmmaster daemon stability
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <castor/exception/Exception.hpp>
#include <castor/exception/NoEntry.hpp>
#include "castor/monitoring/StreamDirection.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include "getconfent.h"

int main(int argc, char** argv) {

  try {
    
    char* rmMasterHost = getconfent("RM","HOST", 0);
    if (0 == rmMasterHost) {
      // Raise an exception
      castor::exception::NoEntry e;
      e.getMessage() << "Found null entry RM/HOST in config file";
      throw e;
    }
    int rmMasterPort = 15003;
    char* rmMasterPortStr = getconfent("RM","PORT", 0);
    if (rmMasterPortStr){
      rmMasterPort = std::strtol(rmMasterPortStr,0,10);
      if (0 == rmMasterPort) {
	// Go back to default
	rmMasterPort = 15003;
	// "Bad rmmaster port value in configuration file"
	castor::dlf::Param initParams[] =
	  {castor::dlf::Param("Message", "Bad rmmaster port value in configuration file, default used"),
	   castor::dlf::Param("Given value", rmMasterPortStr)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 0, 2, initParams);
      }
    }

    // get host name
    char* hostname = (char*) calloc(200, 1);
    if (gethostname(hostname, 200) < 0) {
      std::cerr << "Couldn't get hostname" << std::endl;
      exit(-1);
    }
    
    // endless loop flooding the server with UDP messages
    while(1) {
      try {
	castor::io::UDPSocket s(rmMasterPort, rmMasterHost);
	castor::monitoring::StreamReport sr;
	sr.setDiskServerName(hostname);
	sr.setMountPoint("/srv/castor/01/");
	sr.setDirection(castor::monitoring::STREAMDIRECTION_READ);
	sr.setCreated(true);
	s.sendObject(sr);
      } catch (castor::exception::Exception ex) {
	castor::dlf::Param initParams[] =
	  {castor::dlf::Param("Message", "Failed to send StreamReport to rmmaster daemon via UDP. Exception Caught."),
	   castor::dlf::Param("Original Error", ex.getMessage().str())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 0, 2, initParams);
      }
    }

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception\n" << e.getMessage().str()
	      << "\nExiting" << std::endl;
  }

}
