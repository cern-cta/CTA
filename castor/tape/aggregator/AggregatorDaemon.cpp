/******************************************************************************
 *                 castor/tape/aggregator/AggregatorDaemon.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
 
#include "castor/PortNumbers.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/AggregatorDaemon.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/aggregator/VdqmRequestHandler.hpp"
#include "h/Cgetopt.h"
#include "h/common.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::aggregator::AggregatorDaemon::AggregatorDaemon()
  throw(castor::exception::Exception) :
  castor::server::BaseDaemon("aggregatord") {
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::aggregator::AggregatorDaemon::~AggregatorDaemon() throw() {
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::aggregator::AggregatorDaemon::main(const int argc,
  char **argv) {

  try {

    // Initialize the DLF logging
    castor::server::BaseServer::dlfInit(s_dlfMessages);

    // Log the start of the daemon
    logStart(argc, argv);

    // Parse the command line
    try {
      parseCommandLine(argc, argv);

      // Pass the foreground option to the super class BaseDaemon
      m_foreground = m_parsedCommandLine.foregroundOptionSet;

      // Display usage message and exit if help option found on command-line
      if(m_parsedCommandLine.helpOptionSet) {
        std::cout << std::endl;
        castor::tape::aggregator::AggregatorDaemon::usage(std::cout,
          AGGREGATORPROGRAMNAME);
        std::cout << std::endl;
        return 0;
      }
    } catch (castor::exception::Exception &ex) {
      std::cerr << std::endl << "Failed to parse the command-line: "
        << ex.getMessage().str() << std::endl;
      castor::tape::aggregator::AggregatorDaemon::usage(std::cerr,
        AGGREGATORPROGRAMNAME);
      std::cerr << std::endl;
      return 1;
    }

    createVdqmRequestHandlerPool();

    // Start the threads
    start();

  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;
    usage(std::cerr, AGGREGATORPROGRAMNAME);
    std::cerr << std::endl;
    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::usage(std::ostream &os,
  const char *const programName) throw() {
  os << "\nUsage: "<< programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-f, --foreground            Remain in the Foreground\n"
    "\t-c, --config config-file    Configuration file\n"
    "\t-h, --help                  Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// logStart
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::logStart(const int argc,
  const char *const *const argv) throw() {
  std::string concatenatedArgs;

  // Concatenate all of the command-line arguments into one string
  for(int i=0; i < argc; i++) {
    if(i != 0) {
      concatenatedArgs += " ";
    }

    concatenatedArgs += argv[i];
  }

#ifdef EMULATE_GATEWAY
  const char *emulateGatway = "TRUE";
#else
  const char *emulateGatway = "FALSE";
#endif // EMULATE_GATEWAY

  castor::dlf::Param params[] = {
    castor::dlf::Param("argv"         , concatenatedArgs),
    castor::dlf::Param("emulateGatway", emulateGatway)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, AGGREGATOR_STARTED,
    params);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::parseCommandLine(
  const int argc, char **argv) throw(castor::exception::Exception) {

  static struct Coptions longopts[] = {
    {"foreground", NO_ARGUMENT, NULL, 'f'},
    {"help"      , NO_ARGUMENT, NULL, 'h'},
    {NULL        , 0          , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long(argc, argv, "fc:p:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_parsedCommandLine.foregroundOptionSet = true;
      break;
    case 'h':
      m_parsedCommandLine.helpOptionSet = true;
      break;
    case '?':
      {
        std::stringstream oss;
        oss << "Unknown command-line option: " << (char)Coptopt;

        // Log and throw an exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("Function", __FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        TAPE_THROW_EX(castor::exception::InvalidArgument,
          ": " << oss.str());
      }
      break;
    case ':':
      {
        std::stringstream oss;
        oss << "An option is missing a parameter";

        // Log and throw an exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("Function", __FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        TAPE_THROW_EX(castor::exception::InvalidArgument,
          ": " << oss.str());
      }
      break;
    default:
      {
        std::stringstream oss;
        oss << "Cgetopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;

        // Log and throw an exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("Function", __FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        TAPE_THROW_EX(castor::exception::Internal,
          ": " << oss.str());
      }
    }
  }

  if(Coptind > argc) {
    std::stringstream oss;
      oss << "Internal error.  Invalid value for Coptind: " << Coptind;

    // Log and throw an exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __FUNCTION__),
      castor::dlf::Param("Reason", oss.str())};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
    TAPE_THROW_EX(castor::exception::Internal,
      ": " << oss.str());
  }

  // If there is some extra text on the command-line which has not been parsed
  if(Coptind < argc)
  {
    std::stringstream oss;
      oss << "Unexpected command-line argument: " << argv[Coptind]
      << std::endl;

    // Log and throw an exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __FUNCTION__),
      castor::dlf::Param("Reason"  , oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
      AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": " << oss.str());
  }
}


//------------------------------------------------------------------------------
// getVdqmListenPort()
//------------------------------------------------------------------------------
int castor::tape::aggregator::AggregatorDaemon::getVdqmListenPort()
  throw(castor::exception::InvalidConfigEntry) {
  int port = AGGREGATOR_VDQMPORT; // Initialise to default value
  const char *const configEntry = getconfent("TAPEAGGREGATOR", "VDQMPORT", 0);

  if(configEntry != NULL) {
    if(utils::isValidUInt(configEntry)) {
      port = atoi(configEntry);
    } else {
      castor::exception::InvalidConfigEntry ex("TAPEAGGREGATOR", "VDQMPORT",
        configEntry);

      ex.getMessage()
        <<  "File="     << __FILE__
        << " Line="     << __LINE__
        << " Function=" << __FUNCTION__
        << ": Invalid configuration entry: "
        << ex.getEntryCategory() << " " << ex.getEntryName() << " "
        << ex.getEntryValue();

      throw ex;
    }
  }

  return port;
}


//------------------------------------------------------------------------------
// createVdqmRequestHandlerPool
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::
  createVdqmRequestHandlerPool() throw(castor::exception::Exception) {

  const int vdqmListenPort = getVdqmListenPort();

  addThreadPool(
    new castor::server::TCPListenerThreadPool("VdqmRequestHandlerPool",
      new castor::tape::aggregator::VdqmRequestHandler(), vdqmListenPort));

  m_vdqmRequestHandlerThreadPool = getThreadPool('V');

  if(m_vdqmRequestHandlerThreadPool == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
     ": Failed to get VdqmRequestHandlerPool");
  }

  m_vdqmRequestHandlerThreadPool->setNbThreads(MAXDRIVES);
}
