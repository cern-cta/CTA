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
#include "catalogue/DeleteAllCatalogueDataCmd.hpp"
#include "catalogue/DeleteAllCatalogueDataCmdLineArgs.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DeleteAllCatalogueDataCmd::DeleteAllCatalogueDataCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DeleteAllCatalogueDataCmd::~DeleteAllCatalogueDataCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int DeleteAllCatalogueDataCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const DeleteAllCatalogueDataCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbDbConns = 1;
  auto catalogue = CatalogueFactory::create(dbLogin, nbDbConns);

  if(catalogue->schemaIsLocked()) {
    m_err <<
      "Cannot delete the data in the catalogue because the schema is locked.\n"
      "\n"
      "Please see the following command-line tools:\n"
      "    cta-catalogue-schema-lock\n"
      "    cta-catalogue-schema-status\n"
      "    cta-catalogue-schema-unlock" << std::endl;
    return 1;
  }
  deleteAllRowsExceptForCTA_CATALOGUE(*catalogue);

  return 0;
}

//------------------------------------------------------------------------------
// deleteAllRowsExceptForCTA_CATALOGUE
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteAllRowsExceptForCTA_CATALOGUE(Catalogue &catalogue) {
  deleteAdminUsers(catalogue);
  deleteAdminHosts(catalogue);
  deleteArchiveRoutes(catalogue);
  deleteRequesterMountRules(catalogue);
  deleteRequesterGroupMountRules(catalogue);
  deleteArchiveAndTapeFiles(catalogue);
  deleteTapes(catalogue);
  deleteStorageClasses(catalogue);
  deleteTapePools(catalogue);
  deleteLogicalLibrares(catalogue);
  deleteMountPolicies(catalogue);
}

//------------------------------------------------------------------------------
// deleteAdminUsers
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteAdminUsers(Catalogue &catalogue) {
  const std::list<common::dataStructures::AdminUser> adminUsers = catalogue.getAdminUsers();
  for(auto &adminUser: adminUsers) {
    catalogue.deleteAdminUser(adminUser.name);
  }
  m_out << "Deleted " << adminUsers.size() << " admin users" << std::endl;
}

//------------------------------------------------------------------------------
// deleteAdminHosts
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteAdminHosts(Catalogue &catalogue) {
  const std::list<common::dataStructures::AdminHost> adminHosts = catalogue.getAdminHosts();
  for(auto &adminHost: adminHosts) {
    catalogue.deleteAdminHost(adminHost.name);
  }
  m_out << "Deleted " << adminHosts.size() << " admin hosts" << std::endl;
}

//------------------------------------------------------------------------------
// deleteArchiveRoutes
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteArchiveRoutes(Catalogue &catalogue) {
  const std::list<common::dataStructures::ArchiveRoute> archiveRoutes = catalogue.getArchiveRoutes();
  for(auto &archiveRoute: archiveRoutes) {
    catalogue.deleteArchiveRoute(archiveRoute.diskInstanceName, archiveRoute.storageClassName,
      archiveRoute.copyNb);
  }
  m_out << "Deleted " << archiveRoutes.size() << " archive routes" << std::endl;
}

//------------------------------------------------------------------------------
// delelteAllRequesterMountRules
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteRequesterMountRules(Catalogue &catalogue) {
  const std::list<common::dataStructures::RequesterMountRule> rules = catalogue.getRequesterMountRules();
  for(auto &rule: rules) {
    catalogue.deleteRequesterMountRule(rule.diskInstance, rule.name);
  }
  m_out << "Deleted " << rules.size() << " requester mount-rules" << std::endl;
}

//------------------------------------------------------------------------------
// deleteRequesterGroupMountRules
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteRequesterGroupMountRules(Catalogue &catalogue) {
  const std::list<common::dataStructures::RequesterGroupMountRule> rules =
    catalogue.getRequesterGroupMountRules();
  for(auto &rule: rules) {
    catalogue.deleteRequesterGroupMountRule(rule.diskInstance, rule.name);
  }
  m_out << "Deleted " << rules.size() << " requester-group mount-rules" << std::endl;
}

//------------------------------------------------------------------------------
// deleteArchiveAndTapeFiles
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteArchiveAndTapeFiles(Catalogue &catalogue) {
  std::unique_ptr<ArchiveFileItor> itor = catalogue.getArchiveFileItor();
  uint64_t nbArchiveFiles = 0;
  while(itor->hasMore()) {
    const auto archiveFile = itor->next();
    catalogue.deleteArchiveFile(archiveFile.diskInstance, archiveFile.archiveFileID);
    nbArchiveFiles++;
  }
  m_out << "Deleted " << nbArchiveFiles << " archive files and their associated tape copies" << std::endl;
}

//------------------------------------------------------------------------------
// deleteTapes
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteTapes(Catalogue &catalogue) {
  const std::list<common::dataStructures::Tape> tapes = catalogue.getTapes();
  for(auto &tape: tapes) {
    catalogue.deleteTape(tape.vid);
  }
  m_out << "Deleted " << tapes.size() << " tapes" << std::endl;
}

//------------------------------------------------------------------------------
// deleteStorageClasses
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteStorageClasses(Catalogue &catalogue) {
  const std::list<common::dataStructures::StorageClass> storageClasses = catalogue.getStorageClasses();
  for(auto &storageClass: storageClasses) {
    catalogue.deleteStorageClass(storageClass.diskInstance, storageClass.name);
  }
  m_out << "Deleted " << storageClasses.size() << " storage classes" << std::endl;
}

//------------------------------------------------------------------------------
// deleteTapePools
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteTapePools(Catalogue &catalogue) {
  const std::list<common::dataStructures::TapePool> tapePools = catalogue.getTapePools();
  for(auto &tapePool: tapePools) {
    catalogue.deleteTapePool(tapePool.name);
  }
  m_out << "Deleted " << tapePools.size() << " tape pools" << std::endl;
}

//------------------------------------------------------------------------------
// deleteLogicalLibraries
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteLogicalLibrares(Catalogue &catalogue) {
  const std::list<common::dataStructures::LogicalLibrary> logicalLibraries = catalogue.getLogicalLibraries();
  for(auto &logicalLibrary: logicalLibraries) {
    catalogue.deleteLogicalLibrary(logicalLibrary.name);
  }
  m_out << "Deleted " << logicalLibraries.size() << " logical libraries" << std::endl;
}

//------------------------------------------------------------------------------
// deleteMountPolicies
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::deleteMountPolicies(Catalogue &catalogue) {
  const std::list<common::dataStructures::MountPolicy> mountPolicies = catalogue.getMountPolicies();
  for(auto &mountPolicy: mountPolicies) {
    catalogue.deleteMountPolicy(mountPolicy.name);
  }
  m_out << "Deleted " << mountPolicies.size() << " mount policies" << std::endl;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void DeleteAllCatalogueDataCmd::printUsage(std::ostream &os) {
  DeleteAllCatalogueDataCmdLineArgs::printUsage(os);
}

} // namespace catalogue
} // namespace cta
