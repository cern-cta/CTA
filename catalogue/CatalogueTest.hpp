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

#pragma once

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/DbLoginFactory.hpp"

#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <set>

namespace unitTests {

class cta_catalogue_CatalogueTest : public ::testing::TestWithParam<cta::rdbms::DbLoginFactory*> {
public:

  /**
   * Constructor.
   */
  cta_catalogue_CatalogueTest():
    m_bootstrapComment("bootstrap") {

    m_cliSI.username = "cli_user_name";
    m_cliSI.host = "cli_host";

    m_bootstrapAdminSI.username = "bootstrap_admin_user_name";
    m_bootstrapAdminSI.host = "bootstrap_host";

    m_adminSI.username = "admin_user_name";
    m_adminSI.host = "admin_host";
  }

protected:

  virtual void SetUp() {
    using namespace cta;
    using namespace cta::catalogue;

    m_catalogue.reset(CatalogueFactory::create(GetParam()->create()));

    {
      const std::list<common::dataStructures::AdminUser> adminUsers = m_catalogue->getAdminUsers();
      for(auto &adminUser: adminUsers) {
        m_catalogue->deleteAdminUser(adminUser.name);
      }
    }
    {
      const std::list<common::dataStructures::AdminHost> adminHosts = m_catalogue->getAdminHosts();
      for(auto &adminHost: adminHosts) {
        m_catalogue->deleteAdminHost(adminHost.name);
      }
    }
    {
      const std::list<common::dataStructures::ArchiveRoute> archiveRoutes = m_catalogue->getArchiveRoutes();
      for(auto &archiveRoute: archiveRoutes) {
        m_catalogue->deleteArchiveRoute(archiveRoute.diskInstanceName, archiveRoute.storageClassName,
          archiveRoute.copyNb);
      }
    }
    {
      const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
      for(auto &rule: rules) {
        m_catalogue->deleteRequesterMountRule(rule.diskInstance, rule.name);
      }
    }
    {
      const std::list<common::dataStructures::RequesterGroupMountRule> rules =
        m_catalogue->getRequesterGroupMountRules();
      for(auto &rule: rules) {
        m_catalogue->deleteRequesterGroupMountRule(rule.diskInstance, rule.name);
      }
    }
    {
      std::unique_ptr<ArchiveFileItor> itor = m_catalogue->getArchiveFileItor();
      while(itor->hasMore()) {
        m_catalogue->deleteArchiveFile(itor->next().archiveFileID);
      }
    }
    {
      const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
      for(auto &tape: tapes) {
        m_catalogue->deleteTape(tape.vid);
      }
    }
    {
      const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();
      for(auto &storageClass: storageClasses) {
        m_catalogue->deleteStorageClass(storageClass.diskInstance, storageClass.name);
      }
    }
    {
      const std::list<common::dataStructures::TapePool> tapePools = m_catalogue->getTapePools();
      for(auto &tapePool: tapePools) {
        m_catalogue->deleteTapePool(tapePool.name);
      }
    }
    {
      const std::list<common::dataStructures::LogicalLibrary> logicalLibraries = m_catalogue->getLogicalLibraries();
      for(auto &logicalLibrary: logicalLibraries) {
        m_catalogue->deleteLogicalLibrary(logicalLibrary.name);
      }
    }
    {
      const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
      for(auto &mountPolicy: mountPolicies) {
        m_catalogue->deleteMountPolicy(mountPolicy.name);
      }
    }
  }

  virtual void TearDown() {
    m_catalogue.reset();
  }

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  const std::string m_bootstrapComment;
  cta::common::dataStructures::SecurityIdentity m_cliSI;
  cta::common::dataStructures::SecurityIdentity m_bootstrapAdminSI;
  cta::common::dataStructures::SecurityIdentity m_adminSI;

  /**
   * Creates a map from VID to tape given the specified list of tapes.
   *
   * @param listOfTapes The list of tapes from which the map shall be created.
   * @return The map from VID to tape.
   */
  std::map<std::string, cta::common::dataStructures::Tape> tapeListToMap(
    const std::list<cta::common::dataStructures::Tape> &listOfTapes) {
    using namespace cta;

    try {
      std::map<std::string, cta::common::dataStructures::Tape> vidToTape;

      for (auto &&tape: listOfTapes) {
        if(vidToTape.end() != vidToTape.find(tape.vid)) {
          throw exception::Exception(std::string("Duplicate VID: value=") + tape.vid);
        }
        vidToTape[tape.vid] = tape;
      }

      return vidToTape;
    } catch(exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    }
  }

  /**
   * Creates a map from archive file ID to archive file from the specified iterator.
   *
   * @param itor Iterator over archive files.
   * @return Map from archive file ID to archive file.
   */
  std::map<uint64_t, cta::common::dataStructures::ArchiveFile> archiveFileItorToMap(cta::catalogue::ArchiveFileItor &itor) {
    using namespace cta;

    try {
      std::map<uint64_t, common::dataStructures::ArchiveFile> m;
      while(itor.hasMore()) {
        const common::dataStructures::ArchiveFile archiveFile = itor.next();
        if(m.end() != m.find(archiveFile.archiveFileID)) {
          exception::Exception ex;
          ex.getMessage() << "Archive file with ID " << archiveFile.archiveFileID << " is a duplicate";
          throw ex;
        }
        m[archiveFile.archiveFileID] = archiveFile;
      }
      return m;
    } catch(exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    }
  }
}; // cta_catalogue_CatalogueTest

} // namespace unitTests
