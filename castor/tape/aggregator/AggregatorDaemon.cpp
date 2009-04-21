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
#include "castor/tape/aggregator/Utils.hpp"
#include "castor/tape/aggregator/VdqmRequestHandler.hpp"
#include "h/Cgetopt.h"
#include "h/common.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::aggregator::AggregatorDaemon::AggregatorDaemon()
  throw(castor::exception::Exception) :
  castor::server::BaseDaemon("aggregatord") {
  // Initializes the DLF logging including the definition of the predefined
  // messages.  Please not that castor::server::BaseServer::dlfInit can throw a
  // castor::exception::Exception.
  castor::server::BaseServer::dlfInit(s_dlfMessages);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::aggregator::AggregatorDaemon::~AggregatorDaemon() throw() {
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::usage(std::ostream &os)
  throw() {
  os << "\nUsage: aggregatord [options]\n"
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

  castor::dlf::Param params[] = {
    castor::dlf::Param("argv", concatenatedArgs)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, AGGREGATOR_STARTED, 1,
    params);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorDaemon::parseCommandLine(
  const int argc, char **argv, bool &helpOption)
  throw(castor::exception::Exception) {

  static struct Coptions longopts[] = {
    {"foreground", NO_ARGUMENT      , NULL, 'f'},
    {"config"    , REQUIRED_ARGUMENT, NULL, 'c'},
    {"help"      , NO_ARGUMENT      , NULL, 'h'},
    {NULL        , 0                , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long(argc, argv, "fc:p:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      {
        FILE *fp = fopen(Coptarg, "r");
        if(fp) {
          // The file exists
          fclose(fp);
        } else {
          // The file does not exist
          std::stringstream oss;
          oss << "Configuration file '" << Coptarg << "' does not exist";

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
      setenv("PATH_CONFIG", Coptarg, 1);
      break;
    case 'h':
      helpOption = true;
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

  castor::server::BaseThreadPool *const vdqmRequestHandlerThreadPool =
    getThreadPool('V');

  if(vdqmRequestHandlerThreadPool == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
     ": Failed to get VdqmRequestHandlerPool");
  }

  vdqmRequestHandlerThreadPool->setNbThreads(0);
}
