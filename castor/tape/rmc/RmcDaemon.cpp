/******************************************************************************
 *                 castor/tape/rmc/RmcDaemon.cpp
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
 
 
#include "castor/PortNumbers.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/tape/rmc/DlfMessageConstants.hpp"
#include "castor/tape/rmc/RmcDaemon.hpp"
/*
//#include "castor/tape/tapebridge/ConfigParamLogger.hpp"
//#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/common.h"
*/
#include "h/Cuuid.h"

#include <getopt.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::RmcDaemon::RmcDaemon(log::Logger &logger)
  throw(castor::exception::Exception):
  castor::server::BaseDaemon(logger),
  m_vdqmRequestHandlerThreadPool(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::RmcDaemon::~RmcDaemon() throw() {
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::rmc::RmcDaemon::main(const int argc,
  char **argv) {

  // Try to initialize the DLF logging system, quitting with an error message
  // to stderr if the initialization fails
  try {

    castor::server::BaseServer::dlfInit(s_dlfMessages);

  } catch(castor::exception::Exception &ex) {
    std::cerr << std::endl <<
      "Failed to start daemon"
      ": Failed to initialize DLF"
      ": " << ex.getMessage().str() << std::endl << std::endl;

    return 1;
  }

  // Try to start the daemon, quitting with an error message to stderr and DLF
  // if the start fails
  try {

    exceptionThrowingMain(argc, argv);

  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;
    usage(std::cerr, "rmcd");
    std::cerr << std::endl;

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
      RMCD_FAILED_TO_START, params);

    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::tape::rmc::RmcDaemon::exceptionThrowingMain(
  const int argc, char **argv) throw(castor::exception::Exception) {
  logStartOfDaemon(argc, argv);
  m_cmdLine = parseCmdLine(argc, argv);

  // Display usage message and exit if help option found on command-line
  if(m_cmdLine.help) {
    std::cout << std::endl;
    castor::tape::rmc::RmcDaemon::usage(std::cout, "rmcd");
    std::cout << std::endl;
    return 0;
  }

  // Pass the foreground option to the super class BaseDaemon
  m_foreground = m_cmdLine.foreground;

/*

  // Determine and log the configuration parameters used by the tapebridged
  // daemon to know how many files should be bulk requested per request for
  // migration to or recall from tape.
  BulkRequestConfigParams bulkRequestConfigParams;
  bulkRequestConfigParams.determineConfigParams();
  ConfigParamLogger::writeToDlf(nullCuuid,
    bulkRequestConfigParams.getBulkRequestMigrationMaxBytes());
  ConfigParamLogger::writeToDlf(nullCuuid,
    bulkRequestConfigParams.getBulkRequestMigrationMaxFiles());
  ConfigParamLogger::writeToDlf(nullCuuid,
    bulkRequestConfigParams.getBulkRequestRecallMaxBytes());
  ConfigParamLogger::writeToDlf(nullCuuid,
    bulkRequestConfigParams.getBulkRequestRecallMaxFiles());

  // Determine and log the values of the tape-bridge configuration-parameters.
  // Only log the maximum number of bytes and files before a flush to tape if
  // the tape flush mode requires them.
  TapeFlushConfigParams tapeFlushConfigParams;
  tapeFlushConfigParams.determineConfigParams();
  {
    const char *const tapeFlushModeStr = tapebridge_tapeFlushModeToStr(
      tapeFlushConfigParams.getTapeFlushMode().getValue());
    ConfigParamLogger::writeToDlf(
      nullCuuid,
      tapeFlushConfigParams.getTapeFlushMode().getCategory(),
      tapeFlushConfigParams.getTapeFlushMode().getName(),
      tapeFlushModeStr,
      tapeFlushConfigParams.getTapeFlushMode().getSource());
  }
  if(TAPEBRIDGE_ONE_FLUSH_PER_N_FILES ==
    tapeFlushConfigParams.getTapeFlushMode().getValue()) {
    ConfigParamLogger::writeToDlf(nullCuuid,
      tapeFlushConfigParams.getMaxBytesBeforeFlush());
    ConfigParamLogger::writeToDlf(nullCuuid,
      tapeFlushConfigParams.getMaxFilesBeforeFlush());
  }

  // Extract the tape-drive names from the TPCONFIG file
  utils::TpconfigLines tpconfigLines;
  utils::parseTpconfigFile(TPCONFIGPATH, tpconfigLines);
  std::list<std::string> driveNames;
  utils::extractTpconfigDriveNames(tpconfigLines, driveNames);

  // Put the tape-drive names into a string stream ready to make a log message
  std::stringstream driveNamesStream;
  for(std::list<std::string>::const_iterator itor = driveNames.begin();
    itor != driveNames.end(); itor++) {

    if(itor != driveNames.begin()) {
      driveNamesStream << ",";
    }

    driveNamesStream << *itor;
  }

  // Log the result of parsing the TPCONFIG file to extract the drive unit
  // names 
  castor::dlf::Param params[] = {
    castor::dlf::Param("filename" , TPCONFIGPATH          ),
    castor::dlf::Param("nbDrives" , driveNames.size()     ),
    castor::dlf::Param("unitNames", driveNamesStream.str())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_PARSED_TPCONFIG, params);

  createVdqmRequestHandlerPool(
    bulkRequestConfigParams,
    tapeFlushConfigParams,
    driveNames.size());
*/

  // Start the threads
  start();

  return 0;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::usage(std::ostream &os,
  const std::string &programName) throw() {
  os << "\nUsage: "<< programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-f, --foreground Remain in the Foreground\n"
    "\t-h, --help       Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  const std::string concatenatedArgs = argvToString(argc, argv);

  // The DLF way
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("argv", concatenatedArgs)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, RMCD_STARTED,
      params);
  }

  // The Logger way
  {
    log::Param params[] = {
      log::Param("argv", concatenatedArgs)};
    logMsg(LOG_INFO, "rmcd daemon started", params);
  }
}

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------
std::string castor::tape::rmc::RmcDaemon::argvToString(const int argc,
  const char *const *const argv) throw() {
  std::string str;

  for(int i=0; i < argc; i++) {
    if(i != 0) {
      str += " ";
    }

    str += argv[i];
  }
  return str;
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
castor::tape::rmc::RmcdCmdLine castor::tape::rmc::RmcDaemon::parseCmdLine(
  const int argc, char **argv) throw(castor::exception::Internal,
  castor::exception::InvalidArgument, castor::exception::InvalidNbArguments,
  castor::exception::MissingOperand) {
  const std::string task = "parse command-line arguments";

  static struct option longopts[] = {
    {"foreground", 0, NULL, 'f'},
    {"help"      , 0, NULL, 'h'},
    {NULL        , 0, NULL,   0}
  };
  RmcdCmdLine cmdLine;
  char c;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;
  while ((c = getopt_long(argc, argv, "fh", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_cmdLine.foreground = true;
      break;
    case 'h':
      m_cmdLine.help = true;
      break;
    case ':':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Failed to " << task <<
          ": The -" << (char)optopt << " option requires a parameter";
        throw ex;
      }
      break;
    case '?':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Failed to " << task;

        if(optopt == 0) {
          ex.getMessage() << ": Unknown command-line option";
        } else {
          ex.getMessage() << ": Unknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() << "Failed to " << task <<
          ": getopt_long returned the following unknown value: 0x" <<
          std::hex << (int)c;
        throw ex;
      }
    } // switch (c)
  } // while ((c = getopt_long( ... )) != -1)

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  if(0 != nbArgs) {
    castor::exception::InvalidNbArguments ex;
    ex.getMessage() << "Failed to " << task <<
      ": Invalid number of command-line arguments: expected=0 actual=" <<
      nbArgs;
    throw ex;
  }

  return cmdLine;
}

//------------------------------------------------------------------------------
// createVdqmRequestHandlerPool
//------------------------------------------------------------------------------
/*
void castor::tape::rmc::RmcDaemon::
  createVdqmRequestHandlerPool(
  const BulkRequestConfigParams &bulkRequestConfigParams,
  const TapeFlushConfigParams   &tapeFlushConfigParams,
  const uint32_t                nbDrives)
  throw(castor::exception::Exception) {

  const int vdqmListenPort = utils::getPortFromConfig("TAPEBRIDGE", "VDQMPORT",
    TAPEBRIDGE_VDQMPORT);

  std::auto_ptr<server::IThread>
    thread(new castor::tape::rmc::VdqmRequestHandler(
      bulkRequestConfigParams,
      tapeFlushConfigParams,
      nbDrives));

  std::auto_ptr<server::BaseThreadPool>
    threadPool(new castor::server::TCPListenerThreadPool(
      "VdqmRequestHandlerPool", thread.release(), vdqmListenPort));

  addThreadPool(threadPool.release());

  m_vdqmRequestHandlerThreadPool = getThreadPool('V');

  if(m_vdqmRequestHandlerThreadPool == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
     ": Failed to get VdqmRequestHandlerPool");
  }

  m_vdqmRequestHandlerThreadPool->setNbThreads(nbDrives);
}
*/
