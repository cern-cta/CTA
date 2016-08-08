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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueTest.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"

#include <algorithm>
#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <memory>
#include <set>

namespace unitTests {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta_catalogue_CatalogueTest::cta_catalogue_CatalogueTest():
  m_bootstrapComment("bootstrap") {

  m_cliSI.username = "cli_user_name";
  m_cliSI.host = "cli_host";

  m_bootstrapAdminSI.username = "bootstrap_admin_user_name";
  m_bootstrapAdminSI.host = "bootstrap_host";

  m_adminSI.username = "admin_user_name";
  m_adminSI.host = "admin_host";
}

//------------------------------------------------------------------------------
// Setup
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::SetUp() {
  using namespace cta;
  using namespace cta::catalogue;

  try {
    const uint64_t nbConns = 2;
    m_catalogue = CatalogueFactory::create(GetParam()->create(), nbConns);

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
        const auto archiveFile = itor->next();
        m_catalogue->deleteArchiveFile(archiveFile.diskInstance, archiveFile.archiveFileID);
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
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// TearDown
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::TearDown() {
  m_catalogue.reset();
}

//------------------------------------------------------------------------------
// tapeListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::Tape> cta_catalogue_CatalogueTest::tapeListToMap(
  const std::list<cta::common::dataStructures::Tape> &listOfTapes) {
  using namespace cta;

  try {
    std::map<std::string, cta::common::dataStructures::Tape> vidToTape;

    for (auto &tape: listOfTapes) {
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

//------------------------------------------------------------------------------
// archiveFileItorToMap
//------------------------------------------------------------------------------
std::map<uint64_t, cta::common::dataStructures::ArchiveFile> cta_catalogue_CatalogueTest::archiveFileItorToMap(
  cta::catalogue::ArchiveFileItor &itor) {
  using namespace cta;

  try {
    std::map<uint64_t, common::dataStructures::ArchiveFile> m;
    while(itor.hasMore()) {
      const auto archiveFile = itor.next();
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

//------------------------------------------------------------------------------
// adminUserListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::AdminUser> cta_catalogue_CatalogueTest::adminUserListToMap(
  const std::list<cta::common::dataStructures::AdminUser> &listOfAdminUsers) {
  using namespace cta;

  try {
    std::map<std::string, common::dataStructures::AdminUser> m;

    for(auto &adminUser: listOfAdminUsers) {
      if(m.end() != m.find(adminUser.name)) {
        exception::Exception ex;
        ex.getMessage() << "Admin user " << adminUser.name << " is a duplicate";
        throw ex;
      }
      m[adminUser.name] = adminUser;
    }
    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// adminHostListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::AdminHost> cta_catalogue_CatalogueTest::adminHostListToMap(
  const std::list<cta::common::dataStructures::AdminHost> &listOfAdminHosts) {
  using namespace cta;

  try {
    std::map<std::string, common::dataStructures::AdminHost> m;

    for(auto &adminHost: listOfAdminHosts) {
      if(m.end() != m.find(adminHost.name)) {
        exception::Exception ex;
        ex.getMessage() << "Admin host " << adminHost.name << " is a duplicate";
        throw ex;
      }
      m[adminHost.name] = adminHost;
    }
    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

TEST_P(cta_catalogue_CatalogueTest, createBootstrapAdminAndHostNoAuth) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    std::list<common::dataStructures::AdminHost> hosts;
    hosts = m_catalogue->getAdminHosts();
    ASSERT_EQ(1, hosts.size());

    const common::dataStructures::AdminHost host = hosts.front();
    ASSERT_EQ(m_bootstrapComment, host.comment);

    const common::dataStructures::EntryLog creationLog = host.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      host.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(2, admins.size());

    const common::dataStructures::AdminUser a1 = admins.front();
    admins.pop_front();
    const common::dataStructures::AdminUser a2 = admins.front();

    ASSERT_NE(a1, a2);
    ASSERT_TRUE((a1.name == m_bootstrapAdminSI.username && a2.name == m_adminSI.username) ||
      (a2.name == m_bootstrapAdminSI.username && a1.name == m_adminSI.username));

    if(a1.name == m_bootstrapAdminSI.username) {
      ASSERT_EQ(m_bootstrapAdminSI.username, a1.name);
      ASSERT_EQ(m_bootstrapComment, a1.comment);
      ASSERT_EQ(m_cliSI.username, a1.creationLog.username);
      ASSERT_EQ(m_cliSI.host, a1.creationLog.host);
      ASSERT_EQ(m_cliSI.username, a1.lastModificationLog.username);
      ASSERT_EQ(m_cliSI.host, a1.lastModificationLog.host);

      ASSERT_EQ(m_adminSI.username, a2.name);
      ASSERT_EQ(createAdminUserComment, a2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.username, a2.creationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.username, a2.lastModificationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminSI.username, a2.name);
      ASSERT_EQ(m_bootstrapComment, a2.comment);
      ASSERT_EQ(m_cliSI.username, a2.creationLog.username);
      ASSERT_EQ(m_cliSI.host, a2.creationLog.host);
      ASSERT_EQ(m_cliSI.username, a2.lastModificationLog.username);
      ASSERT_EQ(m_cliSI.host, a2.lastModificationLog.host);

      ASSERT_EQ(m_adminSI.username, a1.name);
      ASSERT_EQ(createAdminUserComment, a1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.username, a1.creationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.username, a1.lastModificationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.lastModificationLog.host);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username,
    "comment 2"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->deleteAdminUser(m_bootstrapAdminSI.username);

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
  ASSERT_THROW(m_catalogue->deleteAdminUser("non_existant_sdmin_user"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username, createAdminUserComment);

  {
    std::map<std::string, common::dataStructures::AdminUser> admins = adminUserListToMap(m_catalogue->getAdminUsers());
    ASSERT_EQ(2, admins.size());

    const auto itor = admins.find(m_adminSI.username);
    ASSERT_FALSE(admins.end() == itor);

    ASSERT_EQ(createAdminUserComment, itor->second.comment);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyAdminUserComment(m_bootstrapAdminSI, m_adminSI.username, modifiedComment);

  {
    std::map<std::string, common::dataStructures::AdminUser> admins = adminUserListToMap(m_catalogue->getAdminUsers());
    ASSERT_EQ(2, admins.size());

    const auto itor = admins.find(m_adminSI.username);
    ASSERT_FALSE(admins.end() == itor);

    ASSERT_EQ(modifiedComment, itor->second.comment);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment_nonExtistentAdminUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyAdminUserComment(m_bootstrapAdminSI, m_adminSI.username, modifiedComment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createAdminHost) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminHostComment = "Create host user";
  const std::string anotherAdminHost = "another_admin_host";
  m_catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, createAdminHostComment);

  {
    std::list<common::dataStructures::AdminHost> hosts;
    hosts = m_catalogue->getAdminHosts();
    ASSERT_EQ(2, hosts.size());

    const common::dataStructures::AdminHost h1 = hosts.front();
    hosts.pop_front();
    const common::dataStructures::AdminHost h2 = hosts.front();

    ASSERT_NE(h1, h2);
    ASSERT_TRUE(
      (h1.name == m_bootstrapAdminSI.host && h2.name == anotherAdminHost)
      ||
      (h2.name == anotherAdminHost && h1.name == m_bootstrapAdminSI.host)
    );

    if(h1.name == m_bootstrapAdminSI.host) {
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.name);
      ASSERT_EQ(m_bootstrapComment, h1.comment);
      ASSERT_EQ(m_cliSI.username, h1.creationLog.username);
      ASSERT_EQ(m_cliSI.host, h1.creationLog.host);
      ASSERT_EQ(m_cliSI.username, h1.lastModificationLog.username);
      ASSERT_EQ(m_cliSI.host, h1.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h2.name);
      ASSERT_EQ(createAdminHostComment, h2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.username, h2.creationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.username, h2.lastModificationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.name);
      ASSERT_EQ(m_bootstrapComment, h2.comment);
      ASSERT_EQ(m_cliSI.username, h2.creationLog.username);
      ASSERT_EQ(m_cliSI.host, h2.creationLog.host);
      ASSERT_EQ(m_cliSI.username, h2.lastModificationLog.username);
      ASSERT_EQ(m_cliSI.host, h2.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h1.name);
      ASSERT_EQ(createAdminHostComment, h1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.username, h1.creationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.username, h1.lastModificationLog.username);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.lastModificationLog.host);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, createAdminHost_same_twice) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string anotherAdminHost = "another_admin_host";

  m_catalogue->createAdminHost(m_bootstrapAdminSI, anotherAdminHost, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, "comment 2"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminHost) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->deleteAdminHost(m_bootstrapAdminSI.host);

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminHost_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());
  ASSERT_THROW(m_catalogue->deleteAdminHost("non_exstant_admin_host"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminHostComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    const std::list<common::dataStructures::AdminHost> hosts = m_catalogue->getAdminHosts();
    ASSERT_EQ(1, hosts.size());

    const common::dataStructures::AdminHost host = hosts.front();
    ASSERT_EQ(m_bootstrapComment, host.comment);

    const common::dataStructures::EntryLog creationLog = host.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = host.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminHostComment = "Create host user";
  const std::string anotherAdminHost = "another_admin_host";
  m_catalogue->createAdminHost(m_bootstrapAdminSI, anotherAdminHost, createAdminHostComment);

  {
    std::map<std::string, common::dataStructures::AdminHost> hosts = adminHostListToMap(m_catalogue->getAdminHosts());
    ASSERT_EQ(2, hosts.size());

    const auto itor = hosts.find(anotherAdminHost);
    ASSERT_FALSE(hosts.end() == itor);

    ASSERT_EQ(createAdminHostComment, itor->second.comment);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyAdminHostComment(m_bootstrapAdminSI, anotherAdminHost, modifiedComment);

  {
    std::map<std::string, common::dataStructures::AdminHost> hosts = adminHostListToMap(m_catalogue->getAdminHosts());
    ASSERT_EQ(2, hosts.size());

    const auto itor = hosts.find(anotherAdminHost);
    ASSERT_FALSE(hosts.end() == itor);

    ASSERT_EQ(modifiedComment, itor->second.comment);
  }
}

TEST_P(cta_catalogue_CatalogueTest, isAdmin_false) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->isAdmin(m_cliSI));
}

TEST_P(cta_catalogue_CatalogueTest, isAdmin_true) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  ASSERT_TRUE(m_catalogue->isAdmin(m_bootstrapAdminSI));
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::list<common::dataStructures::StorageClass> storageClasses =
    m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
  ASSERT_EQ(storageClass.name, storageClasses.front().name);
  ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_same_twice) {
  using namespace cta;

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);
  ASSERT_THROW(m_catalogue->createStorageClass(m_cliSI, storageClass), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_same_name_different_disk_instance) {
  using namespace cta;

  common::dataStructures::StorageClass storageClass1;
  storageClass1.diskInstance = "disk_instance_1";
  storageClass1.name = "storage_class";
  storageClass1.nbCopies = 2;
  storageClass1.comment = "Create storage class";

  common::dataStructures::StorageClass storageClass2 = storageClass1;
  storageClass2.diskInstance = "disk_instance_2";

  m_catalogue->createStorageClass(m_cliSI, storageClass1);
  m_catalogue->createStorageClass(m_cliSI, storageClass2);

  const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

  ASSERT_EQ(2, storageClasses.size());

  {
    auto eqStorageClass1 = [&storageClass1](const common::dataStructures::StorageClass &obj) {
      return obj.diskInstance == storageClass1.diskInstance && obj.name == storageClass1.name;
    };
    auto itor = std::find_if(storageClasses.begin(), storageClasses.end(), eqStorageClass1);
    ASSERT_FALSE(itor == storageClasses.end());
  }

  {
    auto eqStorageClass2 = [&storageClass2](const common::dataStructures::StorageClass &obj) {
      return obj.diskInstance == storageClass2.diskInstance && obj.name == storageClass2.name;
    };
    auto itor = std::find_if(storageClasses.begin(), storageClasses.end(), eqStorageClass2);
    ASSERT_FALSE(itor == storageClasses.end());
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::list<common::dataStructures::StorageClass> storageClasses =
    m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
  ASSERT_EQ(storageClass.name, storageClasses.front().name);
  ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteStorageClass(storageClass.diskInstance, storageClass.name);
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_THROW(m_catalogue->deleteStorageClass("non_existant_disk_instance", "non_existant_storage_class"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbCopies = 5;
  m_catalogue->modifyStorageClassNbCopies(m_cliSI, storageClass.diskInstance, storageClass.name, modifiedNbCopies);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(modifiedNbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies_nonExistentStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string diskInstance = "disk_instance";
  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 5;
  ASSERT_THROW(m_catalogue->modifyStorageClassNbCopies(m_cliSI, diskInstance, storageClassName, nbCopies),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyStorageClassComment(m_cliSI, storageClass.diskInstance, storageClass.name, modifiedComment);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(modifiedComment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment_nonExistentStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string diskInstance = "disk_instance";
  const std::string storageClassName = "storage_class";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyStorageClassComment(m_cliSI, diskInstance, storageClassName, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted,
    comment);
      
  const std::list<common::dataStructures::TapePool> pools =
    m_catalogue->getTapePools();
      
  ASSERT_EQ(1, pools.size());
      
  const common::dataStructures::TapePool pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
  ASSERT_EQ(isEncrypted, pool.encryption);
  ASSERT_EQ(comment, pool.comment);

  const common::dataStructures::EntryLog creationLog = pool.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_P(cta_catalogue_CatalogueTest, createTapePool_same_twice) {
  using namespace cta;
  
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted,
    comment);
  ASSERT_THROW(m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted,
    comment);

  const std::list<common::dataStructures::TapePool> pools =
    m_catalogue->getTapePools();

  ASSERT_EQ(1, pools.size());

  const common::dataStructures::TapePool pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
  ASSERT_EQ(isEncrypted, pool.encryption);
  ASSERT_EQ(comment, pool.comment);

  const common::dataStructures::EntryLog creationLog = pool.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteTapePool(pool.name);
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_THROW(m_catalogue->deleteTapePool("non_existant_tape_pool"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, comment);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());
      
    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbPartialTapes = 5;
  m_catalogue->modifyTapePoolNbPartialTapes(m_cliSI, tapePoolName, modifiedNbPartialTapes);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());
      
    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(modifiedNbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes_nonExistentTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->modifyTapePoolNbPartialTapes(m_cliSI, tapePoolName, nbPartialTapes),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, comment);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyTapePoolComment(m_cliSI, tapePoolName, modifiedComment);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());
      
    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(modifiedComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment_nonExistentTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_cliSI, tapePoolName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapePoolEncryption) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, comment);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedIsEncrypted = !isEncrypted;
  m_catalogue->setTapePoolEncryption(m_cliSI, tapePoolName, modifiedIsEncrypted);

  {
    const std::list<common::dataStructures::TapePool> pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());
      
    const common::dataStructures::TapePool pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(modifiedIsEncrypted, pool.encryption);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapePoolEncryption_nonExistentTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const bool isEncrypted = false;
  ASSERT_THROW(m_catalogue->setTapePoolEncryption(m_cliSI, tapePoolName, isEncrypted), exception::UserError);
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);
      
  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
  ASSERT_EQ(1, routes.size());
      
  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_same_name_different_disk_instance) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass1DiskInstance1;
  storageClass1DiskInstance1.diskInstance = "disk_instance_1";
  storageClass1DiskInstance1.name = "storage_class_1";
  storageClass1DiskInstance1.nbCopies = 2;
  storageClass1DiskInstance1.comment = "Create storage class";

  common::dataStructures::StorageClass storageClass1DiskInstance2;
  storageClass1DiskInstance2.diskInstance = "disk_instance_2";
  storageClass1DiskInstance2.name = "storage_class_1";
  storageClass1DiskInstance2.nbCopies = 2;
  storageClass1DiskInstance2.comment = "Create storage class";

  m_catalogue->createStorageClass(m_cliSI, storageClass1DiskInstance1);
  m_catalogue->createStorageClass(m_cliSI, storageClass1DiskInstance2);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass1DiskInstance1.diskInstance, storageClass1DiskInstance1.name,
    copyNb, tapePoolName, comment);
  m_catalogue->createArchiveRoute(m_cliSI, storageClass1DiskInstance2.diskInstance, storageClass1DiskInstance2.name,
    copyNb, tapePoolName, comment);
      
  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
  ASSERT_EQ(2, routes.size());

  {
    auto routeForStorageClass1DiskInstance1 =
      [&storageClass1DiskInstance1](const common::dataStructures::ArchiveRoute &ar) {
      return ar.diskInstanceName == storageClass1DiskInstance1.diskInstance &&
        ar.storageClassName == storageClass1DiskInstance1.name;
    };
    auto itor = std::find_if(routes.begin(), routes.end(), routeForStorageClass1DiskInstance1);
    ASSERT_FALSE(itor == routes.end());
  }

  {
    auto routeForStorageClass1DiskInstance2 =
      [&storageClass1DiskInstance2](const common::dataStructures::ArchiveRoute &ar) {
      return ar.diskInstanceName == storageClass1DiskInstance2.diskInstance &&
        ar.storageClassName == storageClass1DiskInstance2.name;
    };
    auto itor = std::find_if(routes.begin(), routes.end(), routeForStorageClass1DiskInstance2);
    ASSERT_FALSE(itor == routes.end());
  }
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRouteTapePool_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted,
    "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb,
    tapePoolName, comment), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteArchiveRoute(storageClass.diskInstance, storageClass.name, copyNb);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());
  ASSERT_THROW(m_catalogue->deleteArchiveRoute("non_existant_disk_instance", "non_existant_storage_class", 1234),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_deleteStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClass.diskInstance, storageClass.name), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->createTapePool(m_cliSI, anotherTapePoolName, nbPartialTapes, isEncrypted, "Create another tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyArchiveRouteTapePoolName(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb,
    anotherTapePoolName);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(anotherTapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName_nonExistentArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  ASSERT_THROW(m_catalogue->modifyArchiveRouteTapePoolName(m_cliSI, storageClass.diskInstance, storageClass.name,
    copyNb, tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyArchiveRouteComment(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb,
    modifiedComment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.diskInstance, route.diskInstanceName);
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(modifiedComment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment_nonExistentArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyArchiveRouteComment(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
      
  const std::list<common::dataStructures::LogicalLibrary> libs =
    m_catalogue->getLogicalLibraries();
      
  ASSERT_EQ(1, libs.size());
      
  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_same_twice) {
  using namespace cta;
  
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
  ASSERT_THROW(m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
      
  const std::list<common::dataStructures::LogicalLibrary> libs =
    m_catalogue->getLogicalLibraries();
      
  ASSERT_EQ(1, libs.size());
      
  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteLogicalLibrary(logicalLibraryName);
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary_non_existant) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
  ASSERT_THROW(m_catalogue->deleteLogicalLibrary("non_existant_logical_library"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();
      
    ASSERT_EQ(1, libs.size());
      
    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyLogicalLibraryComment(m_cliSI, logicalLibraryName, modifiedComment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();
      
    ASSERT_EQ(1, libs.size());
      
    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedComment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryComment_nonExisentLogicalLibrary) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  ASSERT_THROW(m_catalogue->modifyLogicalLibraryComment(m_cliSI, logicalLibraryName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_no_encryption_key) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const optional<std::string> encryptionKey = nullopt;
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_16_exabytes_capacity) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = std::numeric_limits<uint64_t>::max();
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_FALSE(tape.lbp);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_same_twice) {
  using namespace cta;

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue, comment);
  ASSERT_THROW(m_catalogue->createTape(m_cliSI, vid, logicalLibraryName,
    tapePoolName, encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_many_tapes) {
  using namespace cta;

  const std::string logicalLibrary = "logical_library_name";
  const std::string tapePool = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t) 10 * 1000 * 1000 * 1000 * 1000;
  const bool disabled = true;
  const bool full = false;
  const std::string comment = "Create tape";

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibrary, "Create logical library");

  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  m_catalogue->createTapePool(m_cliSI, tapePool, 2, true, "Create tape pool");

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  const uint64_t nbTapes = 10;

  for(uint64_t i = 1; i <= nbTapes; i++) {
    std::ostringstream vid;
    vid << "vid" << i;

    m_catalogue->createTape(m_cliSI, vid.str(), logicalLibrary, tapePool, encryptionKey, capacityInBytes, disabled,
      full, comment);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_FALSE(vidToTape.end() == vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(logicalLibrary, tape.logicalLibraryName);
      ASSERT_EQ(tapePool, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabled == tape.disabled);
      ASSERT_TRUE(full == tape.full);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "vid1";
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = logicalLibrary;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(logicalLibrary, vidToTape.begin()->second.logicalLibraryName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePool;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(tapePool, vidToTape.begin()->second.tapePoolName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.capacityInBytes = capacityInBytes;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(capacityInBytes, vidToTape.begin()->second.capacityInBytes);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.disabled = true;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_TRUE(vidToTape.begin()->second.disabled);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.full = false;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_FALSE(vidToTape.begin()->second.full);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "non_existant_vid";
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_TRUE(tapes.empty());
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "vid1";
    searchCriteria.logicalLibrary = logicalLibrary;
    searchCriteria.tapePool = tapePool;
    searchCriteria.capacityInBytes = capacityInBytes;
    searchCriteria.disabled = true;
    searchCriteria.full = false;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
    ASSERT_EQ(logicalLibrary, vidToTape.begin()->second.logicalLibraryName);
    ASSERT_EQ(tapePool, vidToTape.begin()->second.tapePoolName);
    ASSERT_EQ(capacityInBytes, vidToTape.begin()->second.capacityInBytes);
    ASSERT_TRUE(vidToTape.begin()->second.disabled);
    ASSERT_FALSE(vidToTape.begin()->second.full);
  }

  {
    std::set<std::string> vids;
    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;
      vids.insert(vid.str());
    }

    const common::dataStructures::VidToTapeMap vidToTape = m_catalogue->getTapesByVid(vids);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_FALSE(vidToTape.end() == vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(logicalLibrary, tape.logicalLibraryName);
      ASSERT_EQ(tapePool, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabled == tape.disabled);
      ASSERT_TRUE(full == tape.full);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_FALSE(tape.lbp);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteTape(tape.vid);
  ASSERT_TRUE(m_catalogue->getTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  ASSERT_THROW(m_catalogue->deleteTape("non_exsitant_tape"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string anotherLogicalLibraryName = "another_logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");
  m_catalogue->createLogicalLibrary(m_cliSI, anotherLogicalLibraryName, "Create another logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeLogicalLibraryName(m_cliSI, vid, anotherLogicalLibraryName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(anotherLogicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeLogicalLibraryName_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  ASSERT_THROW(m_catalogue->modifyTapeLogicalLibraryName(m_cliSI, vid, logicalLibraryName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeTapePoolName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string anotherTapePoolName = "another_tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTapePool(m_cliSI, anotherTapePoolName, 2, true, "Create another tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeTapePoolName(m_cliSI, vid, anotherTapePoolName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(anotherTapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeTapePoolName_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  ASSERT_THROW(m_catalogue->modifyTapeTapePoolName(m_cliSI, vid, tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeCapacityInBytes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedCapacityInBytes = 2 * capacityInBytes;
  m_catalogue->modifyTapeCapacityInBytes(m_cliSI, vid, modifiedCapacityInBytes);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(modifiedCapacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeCapacityInBytes_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;

  ASSERT_THROW(m_catalogue->modifyTapeCapacityInBytes(m_cliSI, vid, capacityInBytes), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKey) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedEncryptionKey = "modified_encryption_key";
  m_catalogue->modifyTapeEncryptionKey(m_cliSI, vid, modifiedEncryptionKey);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(modifiedEncryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKey_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string encryptionKey = "encryption_key";

  ASSERT_THROW(m_catalogue->modifyTapeEncryptionKey(m_cliSI, vid, encryptionKey), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeLabelled) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string labelDrive = "labelling_drive";
  const bool lbpIsOn = true;
  m_catalogue->tapeLabelled(vid, labelDrive, lbpIsOn);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE((bool)tape.lbp);
    ASSERT_TRUE(tape.lbp.value());
    ASSERT_EQ(comment, tape.comment);
    ASSERT_TRUE((bool)tape.labelLog);
    ASSERT_EQ(labelDrive, tape.labelLog.value().drive);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeLabelled_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string labelDrive = "drive";
  const bool lbpIsOn = true;

  ASSERT_THROW(m_catalogue->tapeLabelled(vid, labelDrive, lbpIsOn), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForArchive) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->tapeMountedForArchive(vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForArchive_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForArchive(vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForRetrieve) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->tapeMountedForRetrieve(vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForRetrieve_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForRetrieve(vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeFull) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_cliSI, vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(tape.full);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeFull_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeFull(m_cliSI, vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, noSpaceLeftOnTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->noSpaceLeftOnTape(vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(tape.full);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, noSpaceLeftOnTape_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->noSpaceLeftOnTape(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDisabled) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");

  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");

  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeDisabled(m_cliSI, vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.disabled);
    ASSERT_FALSE(tape.full);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDisabled_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeDisabled(m_cliSI, vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
   fullValue, comment);
  const bool lbpIsOn = true;
  m_catalogue->tapeLabelled(vid, "tape_drive", lbpIsOn);

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(tapePoolName, tape.tapePool);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);
}

TEST_P(cta_catalogue_CatalogueTest, createMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  const std::list<common::dataStructures::MountPolicy> mountPolicies =
    m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(name, mountPolicy.name);

  ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

  ASSERT_EQ(comment, mountPolicy.comment);

  const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    mountPolicy.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createMountPolicy_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  ASSERT_THROW(m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(name, mountPolicy.name);

  ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

  ASSERT_EQ(comment, mountPolicy.comment);

  const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteMountPolicy(name);

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteMountPolicy_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());
  ASSERT_THROW(m_catalogue->deleteMountPolicy("non_existant_mount_policy"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchivePriority) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedArchivePriority = archivePriority + 10;
  m_catalogue->modifyMountPolicyArchivePriority(m_cliSI, name, modifiedArchivePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(modifiedArchivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchivePriority_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;

  ASSERT_THROW(m_catalogue->modifyMountPolicyArchivePriority(m_cliSI, name, archivePriority), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchiveMinRequestAge) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinArchiveRequestAge = minArchiveRequestAge + 10;
  m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_cliSI, name, modifiedMinArchiveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(modifiedMinArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchiveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minArchiveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_cliSI, name, minArchiveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetreivePriority) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedRetrievePriority = retrievePriority + 10;
  m_catalogue->modifyMountPolicyRetrievePriority(m_cliSI, name, modifiedRetrievePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(modifiedRetrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrievePriority_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t retrievePriority = 1;

  ASSERT_THROW(m_catalogue->modifyMountPolicyRetrievePriority(m_cliSI, name, retrievePriority), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrieveMinRequestAge) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinRetrieveRequestAge = minRetrieveRequestAge + 10;
  m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_cliSI, name, modifiedMinRetrieveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(modifiedMinRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrieveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minRetrieveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_cliSI, name, minRetrieveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyMaxDrivesAllowed) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMaxDrivesAllowed = maxDrivesAllowed + 10;
  m_catalogue->modifyMountPolicyMaxDrivesAllowed(m_cliSI, name, modifiedMaxDrivesAllowed);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(modifiedMaxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyMaxDrivesAllowed_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t maxDrivesAllowed = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyMaxDrivesAllowed(m_cliSI, name, maxDrivesAllowed), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyMountPolicyComment(m_cliSI, name, modifiedComment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(modifiedComment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->modifyMountPolicyComment(m_cliSI, name, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);
  ASSERT_THROW(m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule_non_existant_mount_policy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string comment = "Create mount rule for requester";
  const std::string mountPolicyName = "non_existant_mount_policy";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  ASSERT_THROW(m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->deleteRequesterMountRule(diskInstanceName, requesterName);
  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterMountRule_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());
  ASSERT_THROW(m_catalogue->deleteRequesterMountRule("non_existant_disk_instance", "non_existant_requester"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterMountRulePolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string anotherMountPolicyName = "another_mount_policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    anotherMountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create another mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  m_catalogue->modifyRequesterMountRulePolicy(m_cliSI, diskInstanceName, requesterName, anotherMountPolicyName);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterMountRulePolicy_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";

  ASSERT_THROW(m_catalogue->modifyRequesterMountRulePolicy(m_cliSI, diskInstanceName, requesterName, mountPolicyName),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesteMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyRequesteMountRuleComment(m_cliSI, diskInstanceName, requesterName, modifiedComment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesteMountRuleComment_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->modifyRequesteMountRuleComment(m_cliSI, diskInstanceName, requesterName, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRulePolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string anotherMountPolicyName = "another_mount_policy";

  m_catalogue->createMountPolicy(
    m_cliSI,
    anotherMountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create another mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  m_catalogue->modifyRequesterGroupMountRulePolicy(m_cliSI, diskInstanceName, requesterGroupName, anotherMountPolicyName);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRulePolicy_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";

  ASSERT_THROW(m_catalogue->modifyRequesterGroupMountRulePolicy(m_cliSI, diskInstanceName, requesterGroupName,
    mountPolicyName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  const std::string modifiedComment = "ModifiedComment";
  m_catalogue->modifyRequesterGroupMountRuleComment(m_cliSI, diskInstanceName, requesterGroupName, modifiedComment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
    ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRuleComment_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  const std::string comment  = "Comment";

  ASSERT_THROW(m_catalogue->modifyRequesterGroupMountRuleComment(m_cliSI, diskInstanceName, requesterGroupName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules =
    m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);
  ASSERT_THROW(m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName,
    requesterGroupName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule_non_existant_mount_policy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string comment = "Create mount rule for requester group";
  const std::string mountPolicyName = "non_existant_mount_policy";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  ASSERT_THROW(m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName,
    requesterGroupName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterGroupMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);
  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterGroupMountRule_non_existant) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());
  ASSERT_THROW(m_catalogue->deleteRequesterGroupMountRule("non_existant_disk_isntance", "non_existant_requester_group"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, prepareForNewFile_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::UserIdentity userIdentity;
  userIdentity.name = requesterName;
  userIdentity.group = "group";
  uint64_t expectedArchiveFileId = 0;
  for(uint64_t i = 0; i<10; i++) {
    const common::dataStructures::ArchiveFileQueueCriteria queueCriteria =
      m_catalogue->prepareForNewFile(storageClass.diskInstance, storageClass.name, userIdentity);

    if(0 == i) {
      expectedArchiveFileId = queueCriteria.fileId;
    } else {
      expectedArchiveFileId++;
    }
    ASSERT_EQ(expectedArchiveFileId, queueCriteria.fileId);

    ASSERT_EQ(1, queueCriteria.copyToPoolMap.size());
    ASSERT_EQ(copyNb, queueCriteria.copyToPoolMap.begin()->first);
    ASSERT_EQ(tapePoolName, queueCriteria.copyToPoolMap.begin()->second);
    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);
  }
}

TEST_P(cta_catalogue_CatalogueTest, prepareForNewFile_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::UserIdentity userIdentity;
  userIdentity.name = "username";
  userIdentity.group = requesterGroupName;
  uint64_t expectedArchiveFileId = 0;
  for(uint64_t i = 0; i<10; i++) {
    const common::dataStructures::ArchiveFileQueueCriteria queueCriteria =
      m_catalogue->prepareForNewFile(storageClass.diskInstance, storageClass.name, userIdentity);

    if(0 == i) {
      expectedArchiveFileId = queueCriteria.fileId;
    } else {
      expectedArchiveFileId++;
    }
    ASSERT_EQ(expectedArchiveFileId, queueCriteria.fileId);

    ASSERT_EQ(1, queueCriteria.copyToPoolMap.size());
    ASSERT_EQ(copyNb, queueCriteria.copyToPoolMap.begin()->first);
    ASSERT_EQ(tapePoolName, queueCriteria.copyToPoolMap.begin()->second);
    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);
  }
}

TEST_P(cta_catalogue_CatalogueTest, prepareForNewFile_requester_mount_rule_overide) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string requesterRuleComment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName,
    requesterRuleComment);

  const std::list<common::dataStructures::RequesterMountRule> requesterRules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, requesterRules.size());

  const common::dataStructures::RequesterMountRule requesterRule = requesterRules.front();

  ASSERT_EQ(requesterName, requesterRule.name);
  ASSERT_EQ(mountPolicyName, requesterRule.mountPolicy);
  ASSERT_EQ(requesterRuleComment, requesterRule.comment);
  ASSERT_EQ(m_cliSI.username, requesterRule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, requesterRule.creationLog.host);
  ASSERT_EQ(requesterRule.creationLog, requesterRule.lastModificationLog);

  const std::string requesterGroupRuleComment = "Create mount rule for requester group";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_cliSI, mountPolicyName, diskInstanceName, requesterName,
    requesterGroupRuleComment);

  const std::list<common::dataStructures::RequesterGroupMountRule> requesterGroupRules =
    m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, requesterGroupRules.size());

  const common::dataStructures::RequesterGroupMountRule requesterGroupRule = requesterGroupRules.front();

  ASSERT_EQ(requesterName, requesterGroupRule.name);
  ASSERT_EQ(mountPolicyName, requesterGroupRule.mountPolicy);
  ASSERT_EQ(requesterGroupRuleComment, requesterGroupRule.comment);
  ASSERT_EQ(m_cliSI.username, requesterGroupRule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, requesterGroupRule.creationLog.host);
  ASSERT_EQ(requesterGroupRule.creationLog, requesterGroupRule.lastModificationLog);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, isEncrypted, "Create tape pool");

  const uint64_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClass.diskInstance, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::UserIdentity userIdentity;
  userIdentity.name = requesterName;
  userIdentity.group = "group";
  uint64_t expectedArchiveFileId = 0;
  for(uint64_t i = 0; i<10; i++) {
    const common::dataStructures::ArchiveFileQueueCriteria queueCriteria =
      m_catalogue->prepareForNewFile(storageClass.diskInstance, storageClass.name, userIdentity);

    if(0 == i) {
      expectedArchiveFileId = queueCriteria.fileId;
    } else {
      expectedArchiveFileId++;
    }
    ASSERT_EQ(expectedArchiveFileId, queueCriteria.fileId);

    ASSERT_EQ(1, queueCriteria.copyToPoolMap.size());
    ASSERT_EQ(copyNb, queueCriteria.copyToPoolMap.begin()->first);
    ASSERT_EQ(tapePoolName, queueCriteria.copyToPoolMap.begin()->second);
    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);
  }
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFile) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";
  const std::string diskInstanceName2 = "disk_instance_2";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid1, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes,
    disabledValue, fullValue, createTapeComment);
  m_catalogue->createTape(m_cliSI, vid2, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes,
    disabledValue, fullValue, createTapeComment);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(createTapeComment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    auto it = vidToTape.find(vid2);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid2, tape.vid);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(encryptionKey, tape.encryptionKey);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
    ASSERT_EQ(createTapeComment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";
  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.checksumType         = checksumType;
  file1Written.checksumValue        = checksumValue;
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumType         = checksumType;
  file2Written.checksumValue        = checksumValue;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file2Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
    ASSERT_EQ(file2Written.checksumType, tapeFile2.checksumType);
    ASSERT_EQ(file2Written.checksumValue, tapeFile2.checksumValue);
  }

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_cliSI,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_cliSI, mountPolicyName, diskInstanceName1, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_cliSI.username, rule.creationLog.username);
  ASSERT_EQ(m_cliSI.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  common::dataStructures::UserIdentity userIdentity;
  userIdentity.name = requesterName;
  userIdentity.group = "group";
  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, userIdentity);

  ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
  ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);

  // Check that the diskInstanceName mismatch detection works
  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName2, archiveFileId, userIdentity),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileItor_zero_prefetch) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->getArchiveFileItor(catalogue::ArchiveFileSearchCriteria(), 0), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, fileWrittenToTape_many_archive_files) {
  using namespace cta;

  const std::string vid = "VID123";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes, disabledValue,
    fullValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  const uint64_t nbArchiveFiles = 10;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    const uint64_t archiveFileSize = 1;
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);
    std::ostringstream diskFilePath;
    diskFilePath << "/public_dir/public_file_" << i;

    catalogue::TapeFileWritten fileWritten;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = storageClass.diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileUser = "public_disk_user";
    fileWritten.diskFileGroup = "public_disk_group";
    fileWritten.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
    fileWritten.size = archiveFileSize;
    fileWritten.checksumType = checksumType;
    fileWritten.checksumValue = checksumValue;
    fileWritten.storageClassName = storageClass.name;
    fileWritten.vid = vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.compressedSize = 1;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    m_catalogue->fileWrittenToTape(fileWritten);

    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(i, tape.lastFSeq);
  }

  std::list<uint64_t> prefetches = {1, 2, 3, nbArchiveFiles - 1, nbArchiveFiles, nbArchiveFiles+1, nbArchiveFiles*2};
  for(auto prefetch: prefetches) {
    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.archiveFileId = 1;
      searchCriteria.diskInstance = storageClass.diskInstance;
      searchCriteria.diskFileId = std::to_string(12345678);
      searchCriteria.diskFilePath = "/public_dir/public_file_1";
      searchCriteria.diskFileUser = "public_disk_user";
      searchCriteria.diskFileGroup = "public_disk_group";
      searchCriteria.storageClass = storageClass.name;
      searchCriteria.vid = vid;
      searchCriteria.tapeFileCopyNb = 1;
      searchCriteria.tapePool = tapePoolName;

      std::unique_ptr<catalogue::ArchiveFileItor> archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria,
        prefetch);
      std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(1, m.size());

      const auto idAndFile = m.find(1);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(searchCriteria.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(searchCriteria.diskFilePath, archiveFile.diskFileInfo.path);
      ASSERT_EQ(searchCriteria.diskFileUser, archiveFile.diskFileInfo.owner);
      ASSERT_EQ(searchCriteria.diskFileGroup, archiveFile.diskFileInfo.group);
      ASSERT_EQ(searchCriteria.storageClass, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());
      ASSERT_EQ(searchCriteria.vid, archiveFile.tapeFiles.begin()->second.vid);
    }
    std::unique_ptr<catalogue::ArchiveFileItor> archiveFileItor =
      m_catalogue->getArchiveFileItor(catalogue::ArchiveFileSearchCriteria(), prefetch);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(*archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      const uint64_t archiveFileSize = 1;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
      fileWritten.diskFileUser = "public_disk_user";
      fileWritten.diskFileGroup = "public_disk_group";
      fileWritten.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
      fileWritten.size = archiveFileSize;
      fileWritten.checksumType = checksumType;
      fileWritten.checksumValue = checksumValue;
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.compressedSize = 1;
      fileWritten.copyNb = 1;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
      ASSERT_EQ(fileWritten.diskFileUser, archiveFile.diskFileInfo.owner);
      ASSERT_EQ(fileWritten.diskFileGroup, archiveFile.diskFileInfo.group);
      ASSERT_EQ(fileWritten.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumType, archiveFile.checksumType);
      ASSERT_EQ(fileWritten.checksumValue, archiveFile.checksumValue);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());
      ASSERT_EQ(fileWritten.vid, archiveFile.tapeFiles.begin()->second.vid);
      ASSERT_EQ(fileWritten.fSeq, archiveFile.tapeFiles.begin()->second.fSeq);
      ASSERT_EQ(fileWritten.blockId, archiveFile.tapeFiles.begin()->second.blockId);
      ASSERT_EQ(fileWritten.compressedSize, archiveFile.tapeFiles.begin()->second.compressedSize);
      ASSERT_EQ(fileWritten.checksumType, archiveFile.tapeFiles.begin()->second.checksumType);
      ASSERT_EQ(fileWritten.checksumValue, archiveFile.tapeFiles.begin()->second.checksumValue);
      ASSERT_EQ(fileWritten.copyNb, archiveFile.tapeFiles.begin()->first);
      ASSERT_EQ(fileWritten.copyNb, archiveFile.tapeFiles.begin()->second.copyNb);
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.archiveFileId = 10;
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(1, m.size());
      ASSERT_EQ(10, m.begin()->first);
      ASSERT_EQ(10, m.begin()->second.archiveFileID);
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.diskInstance = storageClass.diskInstance;
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.diskFileId = "12345687";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(1, m.size());
      ASSERT_EQ("12345687", m.begin()->second.diskFileId);
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.diskFilePath = "/public_dir/public_file_10";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(1, m.size());
      ASSERT_EQ("/public_dir/public_file_10", m.begin()->second.diskFileInfo.path);
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.diskFileUser = "public_disk_user";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.diskFileGroup = "public_disk_group";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.storageClass = "storage_class";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.vid = vid;
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.tapeFileCopyNb = 1;
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.tapePool = "tape_pool_name";
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(nbArchiveFiles, m.size());
    }

    {
      catalogue::ArchiveFileSearchCriteria searchCriteria;
      searchCriteria.archiveFileId = nbArchiveFiles + 1234;
      archiveFileItor = m_catalogue->getArchiveFileItor(searchCriteria, prefetch);
      m = archiveFileItorToMap(*archiveFileItor);
      ASSERT_EQ(0, m.size());
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, fileWrittenToTape_2_tape_files_different_tapes) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
                                    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid1, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);
  m_catalogue->createTape(m_cliSI, vid2, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.checksumType         = checksumType;
  file1Written.checksumValue        = checksumValue;
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumType         = checksumType;
  file2Written.checksumValue        = checksumValue;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file2Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
    ASSERT_EQ(file2Written.checksumType, tapeFile2.checksumType);
    ASSERT_EQ(file2Written.checksumValue, tapeFile2.checksumValue);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }
}

TEST_P(cta_catalogue_CatalogueTest, fileWrittenToTape_2_tape_files_same_archive_file_same_tape) {
  using namespace cta;

  const std::string vid = "VID123";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
                                    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;
  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const uint64_t archiveFileSize = 1;

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.checksumType         = checksumType;
  file1Written.checksumValue        = checksumValue;
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumType         = checksumType;
  file2Written.checksumValue        = checksumValue;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid;
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->fileWrittenToTape(file2Written), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, fileWrittenToTape_2_tape_files_corrupted_diskFilePath) {
  using namespace cta;

  const std::string vid = "VID123";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
                                    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_FALSE(tape.lbp);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.checksumType         = checksumType;
  file1Written.checksumValue        = checksumValue;
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = "CORRUPTED disk file path";
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumType         = checksumType;
  file2Written.checksumValue        = checksumValue;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = "VID123";
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;

  ASSERT_THROW(m_catalogue->fileWrittenToTape(file2Written), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "Create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "Create tape pool");
  m_catalogue->createTape(m_cliSI, vid1, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);
  m_catalogue->createTape(m_cliSI, vid2, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(encryptionKey, tape.encryptionKey);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_FALSE(tape.lbp);
      ASSERT_EQ(comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_cliSI.username, creationLog.username);
      ASSERT_EQ(m_cliSI.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.checksumType         = checksumType;
  file1Written.checksumValue        = checksumValue;
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    std::unique_ptr<catalogue::ArchiveFileItor> archiveFileItor = m_catalogue->getArchiveFileItor();
    const std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(*archiveFileItor);
    ASSERT_EQ(1, m.size());
    auto mItor = m.find(file1Written.archiveFileId);
    ASSERT_FALSE(m.end() == mItor);

    const common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file1Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumType         = checksumType;
  file2Written.checksumValue        = checksumValue;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    std::unique_ptr<catalogue::ArchiveFileItor> archiveFileItor = m_catalogue->getArchiveFileItor();
    const std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(*archiveFileItor);
    ASSERT_EQ(1, m.size());

    {
      auto mItor = m.find(file1Written.archiveFileId);
      ASSERT_FALSE(m.end() == mItor);

      const common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumType, archiveFile.checksumType);
      ASSERT_EQ(file2Written.checksumValue, archiveFile.checksumValue);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
      ASSERT_EQ(file2Written.diskFileUser, archiveFile.diskFileInfo.owner);
      ASSERT_EQ(file2Written.diskFileGroup, archiveFile.diskFileInfo.group);
      ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
      ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
      ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
      ASSERT_EQ(file2Written.vid, tapeFile2.vid);
      ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
      ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
      ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
      ASSERT_EQ(file2Written.checksumType, tapeFile2.checksumType);
      ASSERT_EQ(file2Written.checksumValue, tapeFile2.checksumValue);
      ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
    }
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file2Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
    ASSERT_EQ(file2Written.checksumType, tapeFile2.checksumType);
    ASSERT_EQ(file2Written.checksumValue, tapeFile2.checksumValue);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->deleteArchiveFile("disk_instance", archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumType, archiveFile.checksumType);
    ASSERT_EQ(file2Written.checksumValue, archiveFile.checksumValue);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.diskFileInfo.owner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.diskFileInfo.group);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.diskFileInfo.recoveryBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.checksumType, tapeFile1.checksumType);
    ASSERT_EQ(file1Written.checksumValue, tapeFile1.checksumValue);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
    ASSERT_EQ(file2Written.checksumType, tapeFile2.checksumType);
    ASSERT_EQ(file2Written.checksumValue, tapeFile2.checksumValue);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile_non_existant) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFileItor()->hasMore());
  ASSERT_THROW(m_catalogue->deleteArchiveFile("disk_instance", 12345678), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_non_existant_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  std::set<std::string> vids = {{"non_existent_tape"}};
  ASSERT_THROW(m_catalogue->getTapesByVid(vids), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_no_vids) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->getTapesByVid(vids).empty());
}

} // namespace unitTests
