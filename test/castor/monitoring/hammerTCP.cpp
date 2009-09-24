/******************************************************************************
 *                      hammerTCP.cpp
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
 * @(#)$RCSfile: hammerTCP.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/07/23 12:22:06 $ $Author: waldron $
 *
 * Test program hammering the rmmaster daemon with TCP messages
 *
 * @author Dennis Waldron
 *****************************************************************************/

#include <castor/exception/Exception.hpp>
#include "castor/io/ClientSocket.hpp"
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/MonitorMessageAck.hpp"
#include "castor/System.hpp"
#include <iostream>
#include <unistd.h>
#include <stdlib.h>

int main(int argc, char** argv) {
  try {
    u_signed64 count = 1;
    // Endless loop flooding the server with TCP messages
    while(1) {
      try {
        castor::io::ClientSocket s(15003, "lxcastordev06");
        castor::monitoring::DiskServerMetricsReport dsr;
        dsr.setName(castor::System::getHostName());
        s.connect();
        s.sendObject(dsr);

        // Check the acknowledgement
        castor::IObject *obj = s.readObject();
        castor::MessageAck *ack =
          dynamic_cast<castor::monitoring::MonitorMessageAck *>(obj);
        if (ack != 0) {
          delete ack;
        }
        std::cout << count << std::endl;
        count++;
      } catch (castor::exception::Exception ex) {
        std::cerr << "Caught exception: "
                  << ex.getMessage().str() << std::endl;
      }
    }

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception\n" << e.getMessage().str()
              << "\nExiting" << std::endl;
  }
}
