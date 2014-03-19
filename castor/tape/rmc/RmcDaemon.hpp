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
#include "castor/server/Daemon.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/tape/rmc/RmcdCmdLine.hpp"

#include <stdint.h>
#include <list>
#include <map>
#include <ostream>
#include <string>

namespace castor {
namespace tape   {
namespace rmc    {

/**
 * The remote media-changer daemon.
 */
class RmcDaemon : public castor::server::Daemon {

public:

  /**
   * Constructor.
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   */
  RmcDaemon(std::ostream &stdOut, std::ostream &stdErr, log::Logger &log)
    throw(castor::exception::Exception);

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

}; // class RmcDaemon

} // namespace rmc
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_RMC_RMCDAEMON_HPP
