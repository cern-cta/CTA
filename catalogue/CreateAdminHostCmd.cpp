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
#include "catalogue/CreateAdminHostCmd.hpp"
#include "catalogue/CreateAdminHostCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/ConnFactoryFactory.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateAdminHostCmd::CreateAdminHostCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateAdminHostCmd::~CreateAdminHostCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int CreateAdminHostCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const CreateAdminHostCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    CreateAdminHostCmdLineArgs::printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  auto catalogue = CatalogueFactory::create(dbLogin, nbDbConns);
  const common::dataStructures::SecurityIdentity adminRunningCommand(getUsername(), getHostname());

  catalogue->createAdminHost(adminRunningCommand, cmdLineArgs.adminHostname, cmdLineArgs.comment);
  return 0;
}

} // namespace catalogue
} // namespace cta
