/******************************************************************************
 *                      VdqmServer.cpp
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
 * @author castor dev team
 *****************************************************************************/
 
#define VDQMSERV

#include "castor/Services.hpp"
#include "castor/db/DbParamsSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/vdqm/Constants.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/RequestHandlerThread.hpp"
#include "castor/vdqm/RTCPJobSubmitterThread.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmServer.hpp"
#include "h/Cgetopt.h"
#include "h/Cinit.h"
#include "h/Cuuid.h"
#include "h/Cpool_api.h"
#include "h/net.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <stdio.h>
#include <sstream>
#include <string>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServer::VdqmServer()
  throw():
  castor::server::BaseDaemon("Vdqm"),
  m_requestHandlerThreadNumber(REQUESTHANDLERDEFAULTTHREADNUMBER),
  m_RTCPJobSubmitterThreadNumber(RTCPJOBSUBMITTERDEFAULTTHREADNUMBER),
  m_schedulerThreadNumber(SCHEDULERDEFAULTTHREADNUMBER) {
  // Initializes the DLF logging including the definition of the predefined
  // messages.
  dlfInit(s_dlfMessages);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::parseCommandLine(int argc, char *argv[])
  throw() {

  static struct Coptions longopts[] = {
    {"foreground"             , NO_ARGUMENT      , NULL, 'f'},
    {"config"                 , REQUIRED_ARGUMENT, NULL, 'c'},
    {"help"                   , NO_ARGUMENT      , NULL, 'h'},
    {"requestHandlerThreads"  , REQUIRED_ARGUMENT, NULL, 'r'},
    {"rtcpJobSubmitterThreads", REQUIRED_ARGUMENT, NULL, 'j'},
    {"schedulerThreads"       , REQUIRED_ARGUMENT, NULL, 's'},
    {NULL                     , 0                , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fc:hr:j:s:", longopts, NULL)) != -1) {
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
            VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

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
      usage(argv[0]);
      exit(0);
    case 'r':
      m_requestHandlerThreadNumber = atoi(Coptarg);
      break;
    case 'j':
      m_RTCPJobSubmitterThreadNumber = atoi(Coptarg);
      break;
    case 's':
      m_schedulerThreadNumber = atoi(Coptarg);
      break;
    case '?':
      {
        std::stringstream oss;
        oss << "Unknown command-line option: " << (char)Coptopt;

        // Log
        castor::dlf::Param params[] = {
          castor::dlf::Param("reason", oss.str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

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
          VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

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
          VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

        // Print error and usage to stderr and then abort
        std::cerr << std::endl << "Error: " << oss.str()
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
    }
  }

  if(Coptind > argc) {
    std::stringstream oss;
    oss << "Internal error.  Invalid value for Coptind: " << Coptind;

    // Log
    castor::dlf::Param params[] = {
      castor::dlf::Param("reason", oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

    // Print error and usage to stderr and then abort
    std::cerr << std::endl << "Error: " << oss.str()
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }

  // Best to abort if there is some extra text on the command-line which has
  // not been parsed as it could indicate that a valid option never got parsed
  if(Coptind < argc)
  {
    std::stringstream oss;
    oss << "Unexpected command-line argument: " << argv[Coptind];

    // Log
    castor::dlf::Param params[] = {
      castor::dlf::Param("reason", oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_FAILED_TO_PARSE_COMMAND_LINE, 1, params);

    // Print error and usage to stderr and then abort
    std::cerr << std::endl << "Error: " << oss.str()
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::usage(std::string programName)
  throw() {
  std::cerr << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-f, --foreground                  Remain in the Foreground\n"
    "\t-c, --config config-file          Configuration file\n"
    "\t-h, --help                        Print this help and exit\n"
    "\t-r, --requestHandlerThreads num   Default "
    << REQUESTHANDLERDEFAULTTHREADNUMBER << "\n"
    "\t-j, --RTCPJobSubmitterThreads num Default "
    << RTCPJOBSUBMITTERDEFAULTTHREADNUMBER << "\n"
    "\t-s, --schedulerThreads num Default "
    << SCHEDULERDEFAULTTHREADNUMBER << "\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// initDatabaseService
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::initDatabaseService() {

  // Check the database connection details file exists
  FILE *fp = fopen(ORAVDQMCONFIGFILE, "r");
  if(fp) {
    // The file exists
    fclose(fp);
  } else {
    // The file does not exist
    std::stringstream oss;
    oss << "Database connection details file '" << ORAVDQMCONFIGFILE
      << "' does not exist";

    // Log
    castor::dlf::Param params[] = {
      castor::dlf::Param("reason", oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_FAILED_TO_INIT_DB_SERVICE, 1, params);

    // Print error to stderr and then abort
    std::cerr << std::endl << "Error: " << oss.str()
      << std::endl << std::endl;
    exit(1);
  }

  // Pass to the DB service the schema version and DB connection details file
  castor::IService* s =
    castor::BaseObject::sharedServices()->service("DbParamsSvc",
    castor::SVC_DBPARAMSSVC);
  castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(s);
  if(params == 0) {
    std::stringstream oss;
    oss << "Could not instantiate the parameters service";

    // Log
    castor::dlf::Param params[] = {
      castor::dlf::Param("reason", oss.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      VDQM_FAILED_TO_INIT_DB_SERVICE, 1, params);

    // Print error to stderr and then abort
    std::cerr << std::endl << "Error: " << oss.str()
      << std::endl << std::endl;
    exit(1);
  }

  params->setSchemaVersion(castor::vdqm::VDQMSCHEMAVERSION);
  params->setDbAccessConfFile(ORAVDQMCONFIGFILE);
}


//------------------------------------------------------------------------------
// getListenPort
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getListenPort()
{
  return VDQM_PORT;
}


//------------------------------------------------------------------------------
// getRequestHandlerThreadNumber
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getRequestHandlerThreadNumber()
{
  return m_requestHandlerThreadNumber;
}


//------------------------------------------------------------------------------
// getRTCPJobSubmitterThreadNumber
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getRTCPJobSubmitterThreadNumber()
{
  return m_RTCPJobSubmitterThreadNumber;
}


//------------------------------------------------------------------------------
// getSchedulerThreadNumber
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getSchedulerThreadNumber()
{
  return m_schedulerThreadNumber;
}
