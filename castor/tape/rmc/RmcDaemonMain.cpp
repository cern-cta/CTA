/******************************************************************************
 *         castor/tape/rmc/RmcDaemonMain.cpp
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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/io/PollReactorImpl.hpp"
#include "castor/legacymsg/CupvProxyTcpIp.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/tape/rmc/RmcDaemon.hpp"
#include "h/Cupv_constants.h"

#include <iostream>

//------------------------------------------------------------------------------
// getConfigParam
//
// Tries to get the value of the specified parameter from parsing
// /etc/castor/castor.conf.
//------------------------------------------------------------------------------
static std::string getConfigParam(const std::string &category, const std::string &name);

//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv, castor::log::Logger &log);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char **const argv) {
  using namespace castor;

  try {
    log::SyslogLogger log("rmcd");

    try {
      return exceptionThrowingMain(argc, argv, log);
    } catch(castor::exception::Exception &ex) {
      log::Param params[] = {castor::log::Param("message", ex.getMessage().str())};
      log(LOG_ERR, "Caught an unexpected CASTOR exception", params);
      return 1;
    } catch(std::exception &se) {
      log::Param params[] = {log::Param("what", se.what())};
      log(LOG_ERR, "Caught an unexpected standard exception", params);
      return 1;
    } catch(...) {
      log(LOG_ERR, "Caught an unexpected and unknown exception");
      return 1;
    }
  } catch(castor::exception::Exception &ex) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": " << ex.getMessage().str() << std::endl;
    return 1;
  } catch(std::exception &se) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": " << se.what() << std::endl;
    return 1;
  } catch(...) {
    std::cerr << "Failed to instantiate object representing CASTOR logging system"
      ": Caught an unknown exception" << std::endl;
    return 1;
  }
  return 0;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char **const argv, castor::log::Logger &log) {
  const std::string cupvHostName = getConfigParam("UPV", "HOST");

  castor::io::PollReactorImpl reactor(log);
  const int netTimeout = 10; // Timeout in seconds
  castor::legacymsg::CupvProxyTcpIp cupv(log, cupvHostName, CUPV_PORT, netTimeout);
  castor::tape::rmc::RmcDaemon daemon(std::cout, std::cerr, log, reactor, cupv);

  return daemon.main(argc, argv);
}

//------------------------------------------------------------------------------
// getConfigParam
//------------------------------------------------------------------------------
static std::string getConfigParam(const std::string &category, const std::string &name)  {
  using namespace castor;

  std::ostringstream task;
  task << "get " << category << ":" << name << " from castor.conf";

  common::CastorConfiguration config;
  std::string value;

  try {
    config = common::CastorConfiguration::getConfig();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration: " << ne.getMessage().str();
    throw ex;
  }

  try {
    value = config.getConfEnt(category.c_str(), name.c_str());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() <<
      ": Failed to get castor configuration entry: " << ne.getMessage().str();
    throw ex;
  }

  return value;
}
