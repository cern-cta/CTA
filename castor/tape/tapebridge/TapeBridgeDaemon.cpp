/******************************************************************************
 *                 castor/tape/tapebridge/TapeBridgeDaemon.cpp
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
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/TapeBridgeDaemon.hpp"
#include "castor/tape/tapebridge/ConfigParamLogger.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/tapebridge/VdqmRequestHandler.hpp"
#include "h/Cgetopt.h"
#include "h/common.h"
#include "h/Cuuid.h"
#include "h/tapebridge_tapeFlushModeToStr.h"

#include <algorithm>
#include <memory>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapebridge::TapeBridgeDaemon::TapeBridgeDaemon()
  throw(castor::exception::Exception) :
  castor::server::BaseDaemon("tapebridged") {
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapebridge::TapeBridgeDaemon::~TapeBridgeDaemon() throw() {
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tapebridge::TapeBridgeDaemon::main(const int argc,
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
    usage(std::cerr, TAPEBRIDGEPROGRAMNAME);
    std::cerr << std::endl;

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_FAILED_TO_START, params);

    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::tape::tapebridge::TapeBridgeDaemon::exceptionThrowingMain(
  const int argc, char **argv) throw(castor::exception::Exception) {

  logStartOfDaemon(argc, argv);
  parseCommandLine(argc, argv);

  // Display usage message and exit if help option found on command-line
  if(m_parsedCommandLine.helpOptionSet) {
    std::cout << std::endl;
    castor::tape::tapebridge::TapeBridgeDaemon::usage(std::cout,
      TAPEBRIDGEPROGRAMNAME);
    std::cout << std::endl;
    return 0;
  }

  // Pass the foreground option to the super class BaseDaemon
  m_foreground = m_parsedCommandLine.foregroundOptionSet;

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
  ConfigParamLogger::writeToDlf(nullCuuid,
    tapeFlushConfigParams.getTapeFlushMode());
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

  // Start the threads
  start();

  return 0;
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeBridgeDaemon::usage(std::ostream &os,
  const char *const programName) throw() {
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
void castor::tape::tapebridge::TapeBridgeDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  std::string concatenatedArgs;

  concatenatedArgs = appendArgvToString(argc, argv, concatenatedArgs);

  castor::dlf::Param params[] = {
    castor::dlf::Param("argv", concatenatedArgs)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, TAPEBRIDGE_STARTED,
    params);
}


//------------------------------------------------------------------------------
// appendArgvToString
//------------------------------------------------------------------------------
std::string &castor::tape::tapebridge::TapeBridgeDaemon::appendArgvToString(
  const int argc, const char *const *const argv, std::string &str) throw() {

  for(int i=0; i < argc; i++) {
    if(i != 0) {
      str += " ";
    }

    str += argv[i];
  }

  return str;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeBridgeDaemon::parseCommandLine(
  const int argc, char **argv) throw(castor::exception::Exception) {

  static struct Coptions longopts[] = {
    {"foreground", NO_ARGUMENT, NULL, 'f'},
    {"help"      , NO_ARGUMENT, NULL, 'h'},
    {NULL        , 0          , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long(argc, argv, "fh", longopts, NULL)) != -1) {
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
        oss <<
          "Failed to parse the command-line"
          ": Unknown command-line option"
          ": option='" << (char)Coptopt << "'";

        // Throw an exception
        castor::exception::InvalidArgument ex;
        ex.getMessage() << oss.str();
        throw(ex);
      }
      break;
    case ':':
      {
        std::stringstream oss;
        oss <<
          "Failed to parse the command-line"
          ": An option is missing a parameter";

        // Throw an exception
        castor::exception::InvalidArgument ex;
        ex.getMessage() << oss.str();
        throw(ex);
      }
      break;
    default:
      {
        std::stringstream oss;
        oss <<
          "Failed to parse the command-line"
          ": Cgetopt_long returned an unknown value"
          ": value=0x" << std::hex << (int)c;

        // Throw an exception
        TAPE_THROW_EX(castor::exception::Internal,
          ": " << oss.str());
      }
    }
  }

  if(Coptind > argc) {
    std::stringstream oss;
    oss <<
      "Failed to parse the command-line"
      ": Invalid Coptind value"
      ": Coptind=" << Coptind;

    // Throw an exception
    TAPE_THROW_EX(castor::exception::Internal,
      ": " << oss.str());
  }

  // If there is some extra text on the command-line which has not been parsed
  if(Coptind < argc)
  {
    std::stringstream oss;
    oss <<
      "Failed to parse the command-line"
      ": Unexpected command-line argument"
      ": argument='" << argv[Coptind] << "'";

    // Throw an exception
    castor::exception::InvalidArgument ex;
    ex.getMessage() << oss.str();
    throw(ex);
  }
}


//------------------------------------------------------------------------------
// createVdqmRequestHandlerPool
//------------------------------------------------------------------------------
void castor::tape::tapebridge::TapeBridgeDaemon::
  createVdqmRequestHandlerPool(
  const BulkRequestConfigParams &bulkRequestConfigParams,
  const TapeFlushConfigParams   &tapeFlushConfigParams,
  const uint32_t                nbDrives)
  throw(castor::exception::Exception) {

  const int vdqmListenPort = utils::getPortFromConfig("TAPEBRIDGE", "VDQMPORT",
    TAPEBRIDGE_VDQMPORT);

  std::auto_ptr<server::IThread>
    thread(new castor::tape::tapebridge::VdqmRequestHandler(
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
