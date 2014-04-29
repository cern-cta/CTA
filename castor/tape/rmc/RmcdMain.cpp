/******************************************************************************
 *                 castor/tape/rmc/RmcdMain.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/
 


#include "castor/io/PollReactorImpl.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/tape/rmc/RmcDaemon.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {

  try {
    castor::log::SyslogLogger log("rmcd");
    castor::io::PollReactorImpl reactor(log);
    castor::tape::rmc::RmcDaemon daemon(std::cout, std::cerr, log, reactor);

    return daemon.main(argc, argv);
  } catch(castor::exception::Exception &ex) {
    std::cerr << "main() caught a CASTOR exception: " << ex.getMessage().str()
      << std::endl;
    return 1;
  } catch(std::exception &se) {
    std::cerr << "main() caught a std::exception: " << se.what() << std::endl;
    return 1;
  } catch(...) {
    std::cerr << "main() caught an unknown exception" << std::endl;
    return 1;
  }
}
