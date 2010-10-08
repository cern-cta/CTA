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
 * Test program hammering the monitoring with UDP messages of psuedo migrators
 * and/or recallers in order to test rmmaster daemon stability
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <castor/exception/Exception.hpp>
#include <castor/exception/NoEntry.hpp>
#include "castor/monitoring/StreamDirection.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include "castor/System.hpp"
#include <iostream>
#include <unistd.h>
#include <stdlib.h>

int main(int argc, char** argv) {
  try {    
    u_signed64 count = 1;
    // Endless loop flooding the server with UDP messages
    while(1) {
      try {
        castor::io::UDPSocket s(15003, "localhost");
        castor::monitoring::StreamReport sr;
        sr.setDiskServerName(castor::System::getHostName());
        sr.setMountPoint("/srv/castor/01/");
        sr.setDirection(castor::monitoring::STREAMDIRECTION_READ);
        sr.setCreated(true);
        s.sendObject(sr);
        std::cout << count << std::endl;
        count++;
      } catch (castor::exception::Exception& ex) {
        std::cerr << "Caught exception: "
                  << ex.getMessage().str() << std::endl;
      }
    }

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception\n" << e.getMessage().str()
              << "\nExiting" << std::endl;
  }
}
