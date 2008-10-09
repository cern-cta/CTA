/******************************************************************************
 *                 castor/tape/aggregator/AggregatorMain.cpp
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
 *
 *
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/
 


#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/tape/aggregator/AggregatorDaemon.hpp"
#include "castor/tape/aggregator/RequestHandlerThread.hpp"


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    castor::tape::aggregator::AggregatorDaemon daemon("aggregatord");


    //-----------------------
    // Parse the command line
    //-----------------------

    try {
      bool helpOption = false;  // True if help option found on command-line
      daemon.parseCommandLine(argc, argv, helpOption);

      // Display usage message and exit if help option found on command-line
      if(helpOption) {
        std::cout << std::endl;
        castor::tape::aggregator::AggregatorDaemon::usage(std::cout, argv[0]);
        std::cout << std::endl;
        return 0;
      }
    } catch (castor::exception::Exception &e) {
      std::cerr << std::endl << "Failed to parse the command-line: "
        << e.getMessage().str() << std::endl;
      castor::tape::aggregator::AggregatorDaemon::usage(std::cerr, argv[0]);
      std::cerr << std::endl;
      return 1;
    }

    daemon.addThreadPool(
    new castor::server::TCPListenerThreadPool("RequestHandlerThreadPool",
      new castor::tape::aggregator::RequestHandlerThread(),
        daemon.getListenPort()));

    daemon.start();

  } catch (castor::exception::Exception &e) {
    std::cerr << std::endl << "Failed to start daemon: "
      << e.getMessage().str() << std::endl << std::endl;
    castor::tape::aggregator::AggregatorDaemon::usage(std::cerr, argv[0]);
    std::cerr << std::endl;
    return 1;
  }

  return 0;
}
