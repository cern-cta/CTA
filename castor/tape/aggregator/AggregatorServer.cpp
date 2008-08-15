/******************************************************************************
 *                 castor/tape/aggregator/AggregatorServer.cpp
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
 
 
#include "castor/Services.hpp"
#include "castor/db/DbParamsSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/AggregatorServer.hpp"
#include "Cgetopt.h"

#include <sstream>


// Hardcoded schema version of the tape database
const std::string TAPESCHEMAVERSION = "2_1_7_12";


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  castor::tape::aggregator::AggregatorServer server(argv[0]);

  server.parseCommandLine(argc, argv);

  server.initDatabaseService();

  try {

    server.start();

  } catch (castor::exception::Exception &e) {
    std::cerr << "Failed to start server : " << e.getMessage().str()
      << std::endl;

    return 1;
  } catch (...) {
    std::cerr << "Failed to start server : Caught an unknown" << std::endl;

    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::aggregator::AggregatorServer::AggregatorServer(
  const char *const serverName) throw():
  castor::server::BaseDaemon(serverName) {
  // Initializes the DLF logging including the definition of the predefined
  // messages.
  dlfInit(s_dlfMessages);
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorServer::usage(const char *const
  programName) throw() {
  std::cerr << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-f, --foreground                  Remain in the Foreground\n"
    "\t-c, --config config-file          Configuration file\n"
    "\t-h, --help                        Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorServer::parseCommandLine(int argc,
  char *argv[]) throw() {

  static struct Coptions longopts[] = {
    {"foreground"             , NO_ARGUMENT      , NULL, 'f'},
    {"config"                 , REQUIRED_ARGUMENT, NULL, 'c'},
    {"help"                   , NO_ARGUMENT      , NULL, 'h'},
    {NULL                     , 0                , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fc:h", longopts, NULL)) != -1) {
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

          // Log
          castor::dlf::Param params[] = {
            castor::dlf::Param("reason", oss.str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
            AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

          // Print error and usage to stderr and then abort
          std::cerr << std::endl << "Error: " << oss.str()
            << std::endl << std::endl;
          usage(argv[0]);
          exit(1);
        }
      }
      setenv("PATH_CONFIG", Coptarg, 1);
      break;
    case 'h':
      help(argv[0]);
      exit(0);
    case '?':
      {
        std::stringstream oss;
        oss << "Unknown command-line option: " << (char)Coptopt;

        // Log
        castor::dlf::Param params[] = {
          castor::dlf::Param("reason", oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

        // Print error and usage to stderr and then abort
        std::cerr << std::endl << "Error: " << oss.str()
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
    case ':':
      {
        std::stringstream oss;
        oss << "An option is missing a parameter";

        // Log
        castor::dlf::Param params[] = {
          castor::dlf::Param("reason", oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

        // Print error and usage to stderr and then abort
        std::cerr << std::endl << "Error: " << oss.str()
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
    default:
      {
        std::stringstream oss;
        oss << "Cgetopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;

        // Log
        castor::dlf::Param params[] = {
          castor::dlf::Param("reason", oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          AGGREGATOR_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

        // Print error and usage to stderr and then abort
        std::cerr << std::endl << "Error: " << oss.str()
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
    }
  }

  if(Coptind > argc) {
    std::cerr
      << std::endl
      << "Internal error.  Invalid value for Coptind: " << Coptind
      << std::endl;
    exit(1);
  }

  // Best to abort if there is some extra text on the command-line which has
  // not been parsed as it could indicate that a valid option never got parsed
  if(Coptind < argc)
  {
    std::cerr
      << std::endl
      << "Error:  Unexpected command-line argument: "
      << argv[Coptind]
      << std::endl << std::endl;
    help(argv[0]);
    exit(1);
  }
}


//------------------------------------------------------------------------------
// initDatabaseService
//------------------------------------------------------------------------------
void castor::tape::aggregator::AggregatorServer::initDatabaseService() {

  // Check the database connection details file exists
  FILE *fp = fopen(ORATAPECONFIGFILE, "r");
  if(fp) {
    // The file exists
    fclose(fp);
  } else {
    // The file does not exist
    std::cerr
      << std::endl
      << "Error: Database connection details file \"" << ORATAPECONFIGFILE
      << "\" does not exist"
      << std::endl << std::endl;
    exit(1);
  }

  // Pass to the DB service the schema version and DB connection details file
  castor::IService* s =
    castor::BaseObject::sharedServices()->service("DbParamsSvc",
    castor::SVC_DBPARAMSSVC);
  castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(s);
  if(params == 0) {
    castor::exception::Internal e;
    e.getMessage() << "Could not instantiate the parameters service";
    throw e;
  }
  params->setSchemaVersion(TAPESCHEMAVERSION);
  params->setDbAccessConfFile(ORATAPECONFIGFILE);
}
