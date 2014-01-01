/******************************************************************************
 *                 castor/tape/rmc/RmcDaemon.hpp
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

#ifndef CASTOR_TAPE_RMC_RMCDAEMON_HPP
#define CASTOR_TAPE_RMC_RMCDAEMON_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/exception/InvalidNbArguments.hpp"
#include "castor/exception/MissingOperand.hpp"
#include "castor/log/Logger.hpp"
#include "castor/server/BaseDaemon.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/tape/rmc/RmcdCmdLine.hpp"

#include <stdint.h>
#include <iostream>
#include <list>
#include <map>
#include <string>

namespace castor {
namespace tape   {
namespace rmc    {

/**
 * The remote media-changer daemon.
 */
class RmcDaemon : public castor::server::BaseDaemon {

public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   */
  RmcDaemon(log::Logger &logger) throw(castor::exception::Exception);

  /**
   * Destructor.
   */
  ~RmcDaemon() throw();

  /**
   * The main entry function of the daemon.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   * @return     The return code of the process.
   */
  int main(const int argc, char **argv);

protected:

  /**
   * Exception throwing main() function which basically implements the
   * non-exception throwing main() function except for the initialisation of
   * DLF and the "exception catch and log" logic.
   */
  int exceptionThrowingMain(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Logs the start of the daemon.
   */
  void logStartOfDaemon(const int argc, const char *const *const argv) throw();

  /**
   * Creates a string that contains the specified command-line arguments
   * separated by single spaces.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  std::string argvToString(const int argc, const char *const *const argv)
    throw();

  /**
   * The results of parsing the command-line.
   */
  RmcdCmdLine m_cmdLine;

  /**
   * The VDQM request handler thread pool which should contain as many threads
   * as there are drives per tape server.
   */
  server::BaseThreadPool *m_vdqmRequestHandlerThreadPool;

  /**
   * Parses the specified command-line arguments and returns the result.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   * @return The result of parsing the command-line arguments.
   */
  RmcdCmdLine parseCmdLine(const int argc, char **argv)
    throw(castor::exception::Internal, castor::exception::InvalidArgument,
      castor::exception::InvalidNbArguments, castor::exception::MissingOperand);

  /**
   * Writes the command-line usage message of this daemon onto the
   * specified output stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name to be used in the message.
   */
  void usage(std::ostream &os, const std::string &programName) throw();

  /**
   * Creates the VDQM request handler thread pool.
   *
   * @param bulkRequestConfigParams The values of the bul-request
   *                                configuration-parameters to be used by the
   *                                tapebridged daemon.
   * @param tapeFlushConfigParams   The values of the tape-flush
   *                                configuration-parameters to be used by the
   *                                tapebridged daemon.
   * @param nbDrives                The number of tape drives attached to the
   *                                tape server that the tapebridged daemon is
   *                                running on.
   */
//void createVdqmRequestHandlerPool(
//  const BulkRequestConfigParams &bulkRequestConfigParams,
//  const TapeFlushConfigParams   &tapeFlushConfigParams,
//  const uint32_t                nbDrives)
//  throw (castor::exception::Exception);

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

}; // class RmcDaemon

} // namespace rmc
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_RMC_RMCDAEMON_HPP
