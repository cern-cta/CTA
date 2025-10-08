/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/CreateAdminUserCmd.hpp"
#include "catalogue/CreateAdminUserCmdLineArgs.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

namespace cta::catalogue {

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
  auto catalogueFactory = CatalogueFactoryFactory::create(dummyLog, dbLogin, nbDbConns, nbArchiveFileListingDbConns);
  auto catalogue = catalogueFactory->create();
  const common::dataStructures::SecurityIdentity adminRunningCommand(getUsername(), getHostname());

  catalogue->AdminUser()->createAdminUser(adminRunningCommand, cmdLineArgs.adminUsername, cmdLineArgs.comment);
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateAdminUserCmd::printUsage(std::ostream &os) {
  CreateAdminUserCmdLineArgs::printUsage(os);
}

} // namespace cta::catalogue
