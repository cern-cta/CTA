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

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/UnlockSchemaCmd.hpp"
#include "catalogue/UnlockSchemaCmdLineArgs.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
UnlockSchemaCmd::UnlockSchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
UnlockSchemaCmd::~UnlockSchemaCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int UnlockSchemaCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const UnlockSchemaCmdLineArgs cmdLineArgs(argc, argv);
  const auto dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  auto catalogue = CatalogueFactory::create(dbLogin, nbDbConns);
  catalogue->unlockSchema();

  return 0;
}

} // namespace catalogue
} // namespace cta
