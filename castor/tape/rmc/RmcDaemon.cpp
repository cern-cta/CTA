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
#include "castor/tape/rmc/RmcDaemon.hpp"
#include "h/Cuuid.h"

#include <getopt.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::RmcDaemon::RmcDaemon(std::ostream &stdOut,
  std::ostream &stdErr, log::Logger &log)
  throw(castor::exception::Exception):
  castor::server::Daemon(stdOut, stdErr, log) {
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

  // Enter the exception throwing main of the daemon
  try {

    exceptionThrowingMain(argc, argv);

  } catch (castor::exception::Exception &ex) {
    // Write the exception to both standard error and the logging system
    m_stdErr << std::endl << ex.getMessage().str() << std::endl << std::endl;

    castor::log::Param params[] = {
      log::Param("Message", ex.getMessage().str()),
      log::Param("Code"   , ex.code()            )};
    m_log(LOG_ERR, "Exiting due to an exception", params);

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
  parseCommandLine(argc, argv);

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

  return 0;
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void castor::tape::rmc::RmcDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  const std::string concatenatedArgs = argvToString(argc, argv);

  log::Param params[] = {
    log::Param("argv", concatenatedArgs)};
  m_log(LOG_INFO, "rmcd daemon started", params);
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
