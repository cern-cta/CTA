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
 
#include "castor/acs/Constants.hpp"
#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/RmcProxyTcpIp.hpp"
#include "castor/mediachanger/MmcProxyNotSupported.hpp"
#include "castor/mediachanger/MountCmd.hpp"
#include "castor/mediachanger/MountCmdLine.hpp"
#include "castor/messages/AcsProxyZmq.hpp"
#include "castor/messages/SmartZmqContext.hpp"
#include "castor/utils/utils.hpp"
#include "h/rmc_constants.h"

#include <exception>
#include <iostream>
#include <zmq.h>

/**
 * An exception throwing version of main().
 *
 * @param argc The number of command-line arguments including the program name.
 * @param argv The command-line arguments.
 * @return The exit value of the program.
 */
static int exceptionThrowingMain(const int argc, char *const *const argv);

/**
 * Instantiates a ZMQ context.
 *
 * @param sizeOfIOThreadPoolForZMQ The size of the thread pool used to perform
 * IO.  This is usually 1 thread.
 * @return A pointer to the newly created ZMQ context.
 */
static void *instantiateZmqContext(const int sizeOfIOThreadPoolForZMQ);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  using namespace castor;
  std::string errorMessage;

  try {
    const int rc = exceptionThrowingMain(argc, argv);
    google::protobuf::ShutdownProtobufLibrary();
    return rc;
  } catch(castor::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, ane exception was throw
  // and errorMessage has been set accordingly

  std::cerr << "Aborting: " << errorMessage << std::endl;
  google::protobuf::ShutdownProtobufLibrary();
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace castor;

  const int sizeOfIOThreadPoolForZMQ = 1;
  messages::SmartZmqContext zmqContext(instantiateZmqContext(
    sizeOfIOThreadPoolForZMQ));
  messages::AcsProxyZmq acs(acs::ACS_PORT, zmqContext.get());

  mediachanger::MmcProxyNotSupported mmc;

  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();

  const unsigned short rmcPort = castorConf.getConfEntInt("RMC", "PORT",
    (unsigned short)RMC_PORT);

  const unsigned int rmcMaxRqstAttempts = castorConf.getConfEntInt("RMC",
    "MAXRQSTATTEMPTS", (unsigned int)RMC_MAXRQSTATTEMPTS);

  // The network timeout of rmc communications should be several minutes due
  // to the time it takes to mount and unmount tapes
  const int rmcNetTimeout = 600; // Timeout in seconds

  legacymsg::RmcProxyTcpIp rmc(rmcPort, rmcNetTimeout, rmcMaxRqstAttempts);

  mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
  
  mediachanger::MountCmd cmd(std::cin, std::cout, std::cerr, mc);

  return cmd.exceptionThrowingMain(argc, argv);
}

//------------------------------------------------------------------------------
// instantiateZmqContext
//------------------------------------------------------------------------------
static void *instantiateZmqContext(const int sizeOfIOThreadPoolForZMQ) {
  using namespace castor;
  void *const zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
  if(NULL == zmqContext) {
    const std::string message = utils::errnoToString(errno);
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate ZMQ context: " << message;
    throw ex;
  }
  return zmqContext;
}
