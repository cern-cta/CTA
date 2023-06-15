/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/CreateAdminUserCmd.hpp"
#include "catalogue/CreateAdminUserCmdLineArgs.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateAdminUserCmd::CreateAdminUserCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) :
CmdLineTool(inStream, outStream, errStream) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CreateAdminUserCmd::~CreateAdminUserCmd() noexcept {}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int CreateAdminUserCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const CreateAdminUserCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  const uint64_t nbArchiveFileListingDbConns = 1;
  log::DummyLogger dummyLog("dummy", "dummy");
  auto catalogueFactory = CatalogueFactoryFactory::create(dummyLog, dbLogin, nbDbConns, nbArchiveFileListingDbConns);
  auto catalogue = catalogueFactory->create();
  const common::dataStructures::SecurityIdentity adminRunningCommand(getUsername(), getHostname());

  catalogue->AdminUser()->createAdminUser(adminRunningCommand, cmdLineArgs.adminUsername, cmdLineArgs.comment);
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateAdminUserCmd::printUsage(std::ostream& os) {
  CreateAdminUserCmdLineArgs::printUsage(os);
}

}  // namespace catalogue
}  // namespace cta
