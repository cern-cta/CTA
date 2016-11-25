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

#include "catalogue/DeleteAllCatalogueDataCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"

#include <ostream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeleteAllCatalogueDataCmdLineArgs::DeleteAllCatalogueDataCmdLineArgs(const int argc, const char *const *const argv) {
  if(argc != 2) {
    exception::Exception ex;
    ex.getMessage() << "Wrong number of command-line arguments: excepted=1 actual=" << (argc - 1) << std::endl <<
      std::endl;
    printUsage(ex.getMessage());
    throw ex;
  }
  dbConfigPath = argv[1];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "    cta-catalogue-delete-all-data databaseConnectionFile" << std::endl <<
    "Where:" << std::endl <<
    "    databaseConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        catalogue database" << std::endl;
}

} // namespace catalogue
} // namespace cta
