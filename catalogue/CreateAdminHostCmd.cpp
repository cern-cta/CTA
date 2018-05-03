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
#include "common/log/DummyLogger.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "common/utils/utils.hpp"

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
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  const uint64_t nbArchiveFileListingDbConns = 0;
  log::DummyLogger dummyLog("dummy", "dummy");
  auto catalogue = CatalogueFactory::create(dummyLog, dbLogin, nbDbConns, nbArchiveFileListingDbConns);
  const common::dataStructures::SecurityIdentity adminRunningCommand(getUsername(), getHostname());

  // Validate that the hostname is a valid IPv4 or IPv6 address, or a Fully-Qualified Domain Name
  if(!utils::isValidIPAddress(cmdLineArgs.adminHostname)) {
    utils::assertIsFQDN(cmdLineArgs.adminHostname);
  }

  catalogue->createAdminHost(adminRunningCommand, cmdLineArgs.adminHostname, cmdLineArgs.comment);
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateAdminHostCmd::printUsage(std::ostream &os) {
  CreateAdminHostCmdLineArgs::printUsage(os);
}

} // namespace catalogue
} // namespace cta
