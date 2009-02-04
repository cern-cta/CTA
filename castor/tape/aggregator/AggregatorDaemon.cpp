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
 
 
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/AggregatorDaemon.hpp"
#include "h/tape_aggregator_constants.h"
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
            castor::dlf::Param("Function", __PRETTY_FUNCTION__),
            castor::dlf::Param("Reason"  , oss.str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
            AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
          castor::exception::InvalidArgument ex;
          ex.getMessage() << __PRETTY_FUNCTION__
            << ": " << oss.str();
          throw ex;
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
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        castor::exception::InvalidArgument ex;
        ex.getMessage() << __PRETTY_FUNCTION__
          << ": " << oss.str();
        throw ex;
      }
    case ':':
      {
        std::stringstream oss;
        oss << "An option is missing a parameter";

        // Log and throw an exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        castor::exception::InvalidArgument ex;
        ex.getMessage() << __PRETTY_FUNCTION__
          << ": " << oss.str();
        throw ex;
      }
    default:
      {
        std::stringstream oss;
        oss << "Cgetopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;

        // Log and throw an exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("Function", __PRETTY_FUNCTION__),
          castor::dlf::Param("Reason"  , oss.str())};
        CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
        castor::exception::Internal ex;
        ex.getMessage() << __PRETTY_FUNCTION__
          << ": " << oss.str();
        throw ex;
      }
    }
  }

  if(Coptind > argc) {
    std::stringstream oss;
      oss << "Internal error.  Invalid value for Coptind: " << Coptind;

    // Log and throw an exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Reason", oss.str())};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
      AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
    castor::exception::Internal ex;
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": " << oss.str();
    throw ex;
  }

  // If there is some extra text on the command-line which has not been parsed
  if(Coptind < argc)
  {
    std::stringstream oss;
      oss << "Unexpected command-line argument: " << argv[Coptind]
      << std::endl;

    // Log and throw an exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Function", __PRETTY_FUNCTION__),
      castor::dlf::Param("Reason"  , oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR,
      AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, params);
    castor::exception::InvalidArgument ex;
    ex.getMessage() << __PRETTY_FUNCTION__
      << ": " << oss.str();
    throw ex;
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
    if(isAValidUInt(configEntry)) {
      port = atoi(configEntry);
    } else {
      castor::exception::InvalidConfigEntry ex("TAPEAGGREGATOR", "VDQMPORT",
        configEntry);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Invalid configuration entry: "
        << ex.getEntryCategory() << " " << ex.getEntryName() << " "
        << ex.getEntryValue();

      throw ex;
    }
  }

  return port;
}


//------------------------------------------------------------------------------
// getRtcpdListenPort()
//------------------------------------------------------------------------------
int castor::tape::aggregator::AggregatorDaemon::getRtcpdListenPort()
  throw(castor::exception::InvalidConfigEntry) {
  int port = AGGREGATOR_RTCPDPORT; // Initialise to default value
  const char *const configEntry = getconfent("TAPEAGGREGATOR", "RTCPDPORT", 0);

  if(configEntry != NULL) {
    if(isAValidUInt(configEntry)) {
      port = atoi(configEntry);
    } else {
      castor::exception::InvalidConfigEntry ex("TAPEAGGREGATOR", "RTCPDPORT",
        configEntry);

      ex.getMessage() << __PRETTY_FUNCTION__
        << ": Invalid configuration entry: "
        << ex.getEntryCategory() << " " << ex.getEntryName() << " "
        << ex.getEntryValue();

      throw ex;
    }
  }

  return port;
}


//------------------------------------------------------------------------------
// isAValidUInt
//------------------------------------------------------------------------------
bool castor::tape::aggregator::AggregatorDaemon::isAValidUInt(const char *str)
  throw() {
  // An empty string is not a valid unsigned integer
  if(*str == '\0') {
    return false;
  }

  // For each character in the string
  for(;*str != '\0'; str++) {
    // If the current character is not a valid numerical digit
    if(*str < '0' || *str > '9') {
      return false;
    }
  }

  return true;
}
