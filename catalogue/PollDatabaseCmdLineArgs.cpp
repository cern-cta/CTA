/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/PollDatabaseCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

#include <ostream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PollDatabaseCmdLineArgs::PollDatabaseCmdLineArgs(const int argc, const char *const *const argv) {
  if(argc != 3) {
    exception::Exception ex;
    ex.getMessage() << "Wrong number of command-line arguments: excepted=2 actual=" << (argc - 1) << std::endl <<
      std::endl;
    printUsage(ex.getMessage());
    throw ex;
  }

  numberOfSecondsToKeepPolling = utils::toUint64(argv[1]);
  dbConfigPath = argv[2];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void PollDatabaseCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "    cta-database-poll numberOfSecondsToKeepPolling databaseConnectionFile" << std::endl <<
    "Where:" << std::endl <<
    "    numberOfSecondsToKeepPolling" << std::endl <<
    "        The total number of seconds cta-database-poll should run before" <<  std::endl <<
    "        exiting." << std::endl <<
    "    databaseConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        catalogue database." << std::endl;
}

} // namespace catalogue
} // namespace cta
