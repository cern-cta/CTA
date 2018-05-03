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
#include "catalogue/CreateAdminUserCmd.hpp"
#include "catalogue/CreateAdminUserCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateAdminUserCmd::CreateAdminUserCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateAdminUserCmd::~CreateAdminUserCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int CreateAdminUserCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const CreateAdminUserCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  const uint64_t nbArchiveFileListingDbConns = 1;
  log::DummyLogger dummyLog("dummy", "dummy");
  auto catalogue = CatalogueFactory::create(dummyLog, dbLogin, nbDbConns, nbArchiveFileListingDbConns);
  const common::dataStructures::SecurityIdentity adminRunningCommand(getUsername(), getHostname());

  catalogue->createAdminUser(adminRunningCommand, cmdLineArgs.adminUsername, cmdLineArgs.comment);
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateAdminUserCmd::printUsage(std::ostream &os) {
  CreateAdminUserCmdLineArgs::printUsage(os);
}

} // namespace catalogue
} // namespace cta
