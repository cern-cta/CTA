/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/log/SyslogLogger.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/acs/Constants.hpp"
#include "castor/acs/AcsDaemon.hpp"
#include "castor/utils/utils.hpp"

#include <iostream>


//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//
// @param argc The number of command-line arguments.
// @param argv The command-line arguments.
// @param log The logging system.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv,
  castor::log::Logger &log);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  // Try to instantiate the logging system API
  castor::log::Logger *logPtr = NULL;
  try {
    logPtr = new castor::log::SyslogLogger("acsd");
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Failed to instantiate object representing CASTOR logging system: " <<
      ex.getMessage().str() << std::endl;
    return 1;
  } 
  castor::log::Logger &log = *logPtr;
  int programRc = 1; // Be pessimistic
  try {
    programRc = exceptionThrowingMain(argc, argv, log);
  } catch(castor::exception::Exception &ex) {
    castor::log::Param params[] = {
      castor::log::Param("message", ex.getMessage().str())};
    log(LOG_ERR, "Caught an unexpected CASTOR exception", params);
  } catch(std::exception &se) {
    castor::log::Param params[] = {castor::log::Param("what", se.what())};
    log(LOG_ERR, "Caught an unexpected standard exception", params);
  } catch(...) {
    log(LOG_ERR, "Caught an unexpected and unknown exception");
  }

  return programRc;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv,
  castor::log::Logger &log) {
  using namespace castor;
  
  tape::reactor::ZMQReactor reactor(log);

  // Create the object providing utilities for working with UNIX capabilities
  castor::server::ProcessCap capUtils;
  
  // Create default configuration
  const acs::AcsDaemon::CastorConf castorConf;
  
  // Create the main acsd object
  acs::AcsDaemon daemon(
    argc,
    argv,
    std::cout,
    std::cerr,
    log,   
    reactor,
    capUtils,
    castorConf);

  // Run the acsd daemon
  return daemon.main();
}
