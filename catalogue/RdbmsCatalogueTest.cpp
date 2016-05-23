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
#include "catalogue/Sqlite.hpp"
#include "catalogue/SqliteConn.hpp"
#include "catalogue/TestingRdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <set>

namespace unitTests {

class cta_catalogue_RdbmsCatalogueTest : public ::testing::Test {
public:
  cta_catalogue_RdbmsCatalogueTest():
    m_bootstrapComment("bootstrap") {

    m_cliUI.group = "cli_group_name";
    m_cliUI.name = "cli_user_name";
    m_cliSI.user = m_cliUI;
    m_cliSI.host = "cli_host";

    m_bootstrapAdminUI.group = "bootstrap_admin_group_name";
    m_bootstrapAdminUI.name = "bootstrap_admin_user_name";
    m_bootstrapAdminSI.user = m_bootstrapAdminUI;
    m_bootstrapAdminSI.host = "bootstrap_host";

    m_adminUI.group = "admin_group_name";
    m_adminUI.name = "admin_user_name";
    m_adminSI.user = m_adminUI;
    m_adminSI.host = "admin_host";
  }

protected:

  virtual void SetUp() {
    m_catalogueConn.reset(new cta::catalogue::SqliteConn(":memory:"));
    m_catalogueConn->enableForeignKeys();
    m_catalogueConn->createCatalogueDatabaseSchema();
    m_catalogue.reset(new cta::catalogue::TestingRdbmsCatalogue(*m_catalogueConn));
  }

  virtual void TearDown() {
  }

  std::unique_ptr<cta::catalogue::SqliteConn> m_catalogueConn;
  std::unique_ptr<cta::catalogue::TestingRdbmsCatalogue> m_catalogue;
  const std::string m_bootstrapComment;
  cta::common::dataStructures::UserIdentity     m_cliUI;
  cta::common::dataStructures::SecurityIdentity m_cliSI;
  cta::common::dataStructures::UserIdentity     m_bootstrapAdminUI;
  cta::common::dataStructures::SecurityIdentity m_bootstrapAdminSI;
  cta::common::dataStructures::UserIdentity     m_adminUI;
  cta::common::dataStructures::SecurityIdentity m_adminSI;
};

TEST_F(cta_catalogue_RdbmsCatalogueTest, createBootstrapAdminAndHostNoAuth) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
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
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      host.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createAdminUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "create admin user";
  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI,
    createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(2, admins.size());

    const common::dataStructures::AdminUser a1 = admins.front();
    admins.pop_front();
    const common::dataStructures::AdminUser a2 = admins.front();

    ASSERT_NE(a1, a2);
    ASSERT_TRUE((a1.name == m_bootstrapAdminUI.name && a2.name == m_adminUI.name) ||
      (a2.name == m_bootstrapAdminUI.name && a1.name == m_adminUI.name));

    if(a1.name == m_bootstrapAdminUI.name) {
      ASSERT_EQ(m_bootstrapAdminUI.name, a1.name);
      ASSERT_EQ(m_bootstrapComment, a1.comment);
      ASSERT_EQ(m_cliSI.user, a1.creationLog.user);
      ASSERT_EQ(m_cliSI.host, a1.creationLog.host);
      ASSERT_EQ(m_cliSI.user, a1.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, a1.lastModificationLog.host);

      ASSERT_EQ(m_adminUI.name, a2.name);
      ASSERT_EQ(createAdminUserComment, a2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, a2.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, a2.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminUI.name, a2.name);
      ASSERT_EQ(m_bootstrapComment, a2.comment);
      ASSERT_EQ(m_cliSI.user, a2.creationLog.user);
      ASSERT_EQ(m_cliSI.host, a2.creationLog.host);
      ASSERT_EQ(m_cliSI.user, a2.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, a2.lastModificationLog.host);

      ASSERT_EQ(m_adminUI.name, a1.name);
      ASSERT_EQ(createAdminUserComment, a1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, a1.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, a1.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.lastModificationLog.host);
    }
  }
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createAdminUser_same_twice) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI,
    "comment 2"), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createAdminHost) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminHosts().empty());

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminHostComment = "create host user";
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
      ASSERT_EQ(m_cliSI.user, h1.creationLog.user);
      ASSERT_EQ(m_cliSI.host, h1.creationLog.host);
      ASSERT_EQ(m_cliSI.user, h1.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, h1.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h2.name);
      ASSERT_EQ(createAdminHostComment, h2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, h2.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, h2.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.name);
      ASSERT_EQ(m_bootstrapComment, h2.comment);
      ASSERT_EQ(m_cliSI.user, h2.creationLog.user);
      ASSERT_EQ(m_cliSI.host, h2.creationLog.host);
      ASSERT_EQ(m_cliSI.user, h2.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, h2.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h1.name);
      ASSERT_EQ(createAdminHostComment, h1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, h1.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, h1.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.lastModificationLog.host);
    }
  }
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createAdminHost_same_twice) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string anotherAdminHost = "another_admin_host";

  m_catalogue->createAdminHost(m_bootstrapAdminSI, anotherAdminHost, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, "comment 2"), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, isAdmin_false) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->isAdmin(m_cliSI));
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, isAdmin_true) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  ASSERT_TRUE(m_catalogue->isAdmin(m_bootstrapAdminSI));
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  const std::string comment = "create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, comment);

  const std::list<common::dataStructures::StorageClass> storageClasses =
    m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  const common::dataStructures::StorageClass storageClass =
    storageClasses.front();
  ASSERT_EQ(storageClassName, storageClass.name);
  ASSERT_EQ(nbCopies, storageClass.nbCopies);
  ASSERT_EQ(comment, storageClass.comment);

  const common::dataStructures::EntryLog creationLog = storageClass.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    storageClass.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createStorageClass_same_twice) {
  using namespace cta;

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  const std::string comment = "create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, comment);
  ASSERT_THROW(m_catalogue->createStorageClass(m_cliSI,
    storageClassName, nbCopies, comment), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  const std::string comment = "create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted,
    comment);
      
  const std::list<common::dataStructures::TapePool> pools =
    m_catalogue->getTapePools();
      
  ASSERT_EQ(1, pools.size());
      
  const common::dataStructures::TapePool pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
  ASSERT_EQ(is_encrypted, pool.encryption);
  ASSERT_EQ(comment, pool.comment);

  const common::dataStructures::EntryLog creationLog = pool.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_F(cta_catalogue_RdbmsCatalogueTest, createTapePool_same_twice) {
  using namespace cta;
  
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  const std::string comment = "create tape pool";
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted,
    comment);
  ASSERT_THROW(m_catalogue->createTapePool(m_cliSI,
    tapePoolName, nbPartialTapes, is_encrypted, comment),
    exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted, "create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClassName, copyNb, tapePoolName,
    comment);
      
  const std::list<common::dataStructures::ArchiveRoute> routes =
    m_catalogue->getArchiveRoutes();
      
  ASSERT_EQ(1, routes.size());
      
  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClassName, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::TapeCopyToPoolMap copyToPoolMap =
    m_catalogue->getTapeCopyToPoolMap(storageClassName);
  ASSERT_EQ(1, copyToPoolMap.size());
  std::pair<uint64_t, std::string> maplet = *(copyToPoolMap.begin());
  ASSERT_EQ(copyNb, maplet.first);
  ASSERT_EQ(tapePoolName, maplet.second);
}
  
TEST_F(cta_catalogue_RdbmsCatalogueTest, createArchiveRouteTapePool_same_twice) {
  using namespace cta;
  
  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted,
    "create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClassName, copyNb, tapePoolName,
    comment);
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_cliSI,
    storageClassName, copyNb, tapePoolName, comment),
    exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createArchiveRoute_deleteStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted, "create tape pool");

  const uint64_t copyNb = 1;
  const std::string comment = "create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClassName, copyNb, tapePoolName,
                               comment);

  const std::list<common::dataStructures::ArchiveRoute> routes =
    m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClassName, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::TapeCopyToPoolMap copyToPoolMap =
    m_catalogue->getTapeCopyToPoolMap(storageClassName);
  ASSERT_EQ(1, copyToPoolMap.size());
  std::pair<uint64_t, std::string> maplet = *(copyToPoolMap.begin());
  ASSERT_EQ(copyNb, maplet.first);
  ASSERT_EQ(tapePoolName, maplet.second);

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClassName), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createLogicalLibrary) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
      
  const std::list<common::dataStructures::LogicalLibrary> libs =
    m_catalogue->getLogicalLibraries();
      
  ASSERT_EQ(1, libs.size());
      
  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_F(cta_catalogue_RdbmsCatalogueTest, createLogicalLibrary_same_twice) {
  using namespace cta;
  
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
  ASSERT_THROW(m_catalogue->createLogicalLibrary(m_cliSI,
    logicalLibraryName, comment),
    exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes("", "", "", "", "", "", "", "").empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes("", "", "", "", "", "", "", "");

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_EQ(comment, tape.comment);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createTape_same_twice) {
  using namespace cta;

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue, comment);
  ASSERT_THROW(m_catalogue->createTape(m_cliSI, vid, logicalLibraryName,
    tapePoolName, encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, getTapesForWriting) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes("", "", "", "", "", "", "", "").empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const std::string comment = "create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);
  ASSERT_FALSE(tape.lbp);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_group";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "create mount group";

  m_catalogue->createMountPolicy(
    m_cliSI,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  const std::list<common::dataStructures::MountPolicy> groups =
    m_catalogue->getMountPolicies();

  ASSERT_EQ(1, groups.size());

  const common::dataStructures::MountPolicy group = groups.front();

  ASSERT_EQ(name, group.name);

  ASSERT_EQ(archivePriority, group.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);

  ASSERT_EQ(retrievePriority, group.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);

  ASSERT_EQ(maxDrivesAllowed, group.maxDrivesAllowed);

  ASSERT_EQ(comment, group.comment);

  const common::dataStructures::EntryLog creationLog = group.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    group.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createMountPolicy_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_group";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;
  const std::string &comment = "create mount group";

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
    comment), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesters().empty());

  const std::string mountPolicyName = "mount_group";
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
    "create mount group");

  const std::string comment = "create user";
  const std::string userName = "user_name";
  const std::string group = "group";
  cta::common::dataStructures::UserIdentity userIdentity;
  userIdentity.name=userName;
  userIdentity.group=group;
  m_catalogue->createRequester(m_cliSI, userIdentity, mountPolicyName, comment);

  std::list<common::dataStructures::Requester> users;
  users = m_catalogue->getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(comment, user.comment);
  ASSERT_EQ(m_cliSI.user, user.creationLog.user);
  ASSERT_EQ(m_cliSI.host, user.creationLog.host);
  ASSERT_EQ(user.creationLog, user.lastModificationLog);

  const common::dataStructures::MountPolicy policy =
    m_catalogue->getMountPolicyForAUser(userIdentity);

  ASSERT_EQ(archivePriority, policy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, policy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, policy.maxDrivesAllowed);
  ASSERT_EQ(retrievePriority, policy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, policy.retrieveMinRequestAge);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createUser_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesters().empty());

  const std::string mountPolicyName = "mount_group";
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
    "create mount group");
  
  const std::string comment = "create user";
  const std::string name = "name";
  const std::string mountPolicy = "mount_group";
  const std::string group = "group";
  cta::common::dataStructures::UserIdentity userIdentity;
  userIdentity.name=name;
  userIdentity.group=group;
  m_catalogue->createRequester(m_cliSI, userIdentity, mountPolicyName, comment);
  ASSERT_THROW(m_catalogue->createRequester(m_cliSI, userIdentity, mountPolicy, comment),
    exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, insertArchiveFile) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");

  catalogue::ArchiveFileRow row;
  row.archiveFileId = 1234;
  row.diskFileId = "EOS_file_ID";
  row.size = 1;
  row.storageClassName = storageClassName;

  row.diskInstance = "disk_file_instance";
  row.diskFilePath = "disk_file_path";
  row.diskFileUser = "disk_file_user";
  row.diskFileGroup = "disk_file_group";
  row.diskFileRecoveryBlob = "disk_file_recovery_blob";

  m_catalogue->insertArchiveFile(row);

  std::list<common::dataStructures::ArchiveFile> files;
  files = m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "");
  ASSERT_EQ(1, files.size());

  const common::dataStructures::ArchiveFile archiveFile = files.front();

  ASSERT_EQ(row.archiveFileId, archiveFile.archiveFileID);
  ASSERT_EQ(row.diskFileId, archiveFile.diskFileID);
  ASSERT_EQ(row.size, archiveFile.fileSize);
  ASSERT_EQ(row.storageClassName, archiveFile.storageClass);

  ASSERT_EQ(row.diskInstance, archiveFile.diskInstance);
  ASSERT_EQ(row.diskFilePath, archiveFile.drData.drPath);
  ASSERT_EQ(row.diskFileUser, archiveFile.drData.drOwner);
  ASSERT_EQ(row.diskFileGroup, archiveFile.drData.drGroup);
  ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile.drData.drBlob);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, insertArchiveFile_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");
  catalogue::ArchiveFileRow row;
  row.archiveFileId = 1234; // Should be ignored
  row.diskFileId = "EOS_file_ID";
  row.size = 1;
  row.storageClassName = storageClassName;

  row.diskInstance = "recovery_instance";
  row.diskFilePath = "disk_file_path";
  row.diskFileUser = "disk_file_user";
  row.diskFileGroup = "disk_file_group";
  row.diskFileRecoveryBlob = "disk_file_recovery_blob";

  m_catalogue->insertArchiveFile(row);
  ASSERT_THROW(m_catalogue->insertArchiveFile(row), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, prepareForNewFile) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesters().empty());

  const std::string mountPolicyName = "mount_group";
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
    "create mount group");

  const std::string userComment = "create user";
  const std::string userName = "user_name";
  const std::string group = "group";
  cta::common::dataStructures::UserIdentity userIdentity;
  userIdentity.name=userName;
  userIdentity.group=group;
  m_catalogue->createRequester(m_cliSI, userIdentity, mountPolicyName, userComment);

  std::list<common::dataStructures::Requester> users;
  users = m_catalogue->getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(userComment, user.comment);
  ASSERT_EQ(m_cliSI.user, user.creationLog.user);
  ASSERT_EQ(m_cliSI.host, user.creationLog.host);
  ASSERT_EQ(user.creationLog, user.lastModificationLog);  

  const common::dataStructures::MountPolicy policy =
    m_catalogue->getMountPolicyForAUser(userIdentity);

  ASSERT_EQ(archivePriority, policy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, policy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, policy.maxDrivesAllowed);
  ASSERT_EQ(retrievePriority, policy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, policy.retrieveMinRequestAge);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");
      
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool is_encrypted = true;
  m_catalogue->createTapePool(m_cliSI, tapePoolName, nbPartialTapes, is_encrypted,
    "create tape pool");

  const uint64_t copyNb = 1;
  const std::string archiveRouteComment = "create archive route";
  m_catalogue->createArchiveRoute(m_cliSI, storageClassName, copyNb, tapePoolName,
    archiveRouteComment);
      
  const std::list<common::dataStructures::ArchiveRoute> routes =
    m_catalogue->getArchiveRoutes();
      
  ASSERT_EQ(1, routes.size());
      
  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClassName, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::TapeCopyToPoolMap copyToPoolMap =
    m_catalogue->getTapeCopyToPoolMap(storageClassName);
  ASSERT_EQ(1, copyToPoolMap.size());
  std::pair<uint64_t, std::string> maplet = *(copyToPoolMap.begin());
  ASSERT_EQ(copyNb, maplet.first);
  ASSERT_EQ(tapePoolName, maplet.second);

  const common::dataStructures::ArchiveFileQueueCriteria queueCriteria =
    m_catalogue->prepareForNewFile(storageClassName, userIdentity);

  ASSERT_EQ(1, queueCriteria.fileId);
  ASSERT_EQ(1, queueCriteria.copyToPoolMap.size());
  ASSERT_EQ(copyNb, queueCriteria.copyToPoolMap.begin()->first);
  ASSERT_EQ(tapePoolName, queueCriteria.copyToPoolMap.begin()->second);
  ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, createTapeFile_2_files) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");

  const uint64_t archiveFileId = 1234;

  catalogue::ArchiveFileRow row;
  row.archiveFileId = archiveFileId;
  row.diskFileId = "EOS_file_ID";
  row.size = 1;
  row.storageClassName = storageClassName;

  row.diskInstance = "disk_file_instance";
  row.diskFilePath = "disk_file_path";
  row.diskFileUser = "disk_file_user";
  row.diskFileGroup = "disk_file_group";
  row.diskFileRecoveryBlob = "disk_file_recovery_blob";

  m_catalogue->insertArchiveFile(row);

  {
    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    archiveFiles = m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "");
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_TRUE(archiveFiles.front().tapeFiles.empty());

    const common::dataStructures::ArchiveFile archiveFile = archiveFiles.front();

    ASSERT_EQ(row.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(row.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(row.size, archiveFile.fileSize);
    ASSERT_EQ(row.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(row.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(row.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(row.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(row.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_TRUE(archiveFile.tapeFiles.empty());
  }

  ASSERT_TRUE(m_catalogue->getTapeFiles().empty());

  common::dataStructures::TapeFile tapeFile1;
  tapeFile1.vid = "VID1";
  tapeFile1.fSeq = 5678;
  tapeFile1.blockId = 9012;
  tapeFile1.compressedSize = 5;
  tapeFile1.copyNb = 1;

  m_catalogue->createTapeFile(tapeFile1, archiveFileId);

  {
    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    archiveFiles = m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "");
    ASSERT_EQ(1, archiveFiles.size());

    const common::dataStructures::ArchiveFile archiveFile = archiveFiles.front();

    ASSERT_EQ(row.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(row.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(row.size, archiveFile.fileSize);
    ASSERT_EQ(row.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(row.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(row.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(row.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(row.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());

    {
      auto copyNbToTapeFileItor = archiveFile.tapeFiles.find(1);
      ASSERT_EQ(1, copyNbToTapeFileItor->first);
      ASSERT_EQ(tapeFile1.vid, copyNbToTapeFileItor->second.vid);
      ASSERT_EQ(tapeFile1.fSeq, copyNbToTapeFileItor->second.fSeq);
      ASSERT_EQ(tapeFile1.blockId, copyNbToTapeFileItor->second.blockId);
      ASSERT_EQ(tapeFile1.compressedSize, copyNbToTapeFileItor->second.compressedSize);
      ASSERT_EQ(tapeFile1.copyNb, copyNbToTapeFileItor->second.copyNb);
    }
  }

  {
    const std::list<common::dataStructures::TapeFile> tapeFiles = m_catalogue->getTapeFiles();

    ASSERT_EQ(1, tapeFiles.size());
    ASSERT_EQ(tapeFile1.vid, tapeFiles.front().vid);
    ASSERT_EQ(tapeFile1.fSeq, tapeFiles.front().fSeq);
    ASSERT_EQ(tapeFile1.blockId, tapeFiles.front().blockId);
    ASSERT_EQ(tapeFile1.compressedSize, tapeFiles.front().compressedSize);
    ASSERT_EQ(tapeFile1.copyNb, tapeFiles.front().copyNb);
  }

  common::dataStructures::TapeFile tapeFile2;
  tapeFile2.vid = "VID2";
  tapeFile2.fSeq = 3456;
  tapeFile2.blockId = 7890;
  tapeFile2.compressedSize = 6;
  tapeFile2.copyNb = 2;

  m_catalogue->createTapeFile(tapeFile2, archiveFileId);

  {
    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    archiveFiles = m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "");
    ASSERT_EQ(1, archiveFiles.size());

    const common::dataStructures::ArchiveFile archiveFile = archiveFiles.front();

    ASSERT_EQ(row.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(row.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(row.size, archiveFile.fileSize);
    ASSERT_EQ(row.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(row.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(row.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(row.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(row.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    {
      auto copyNbToTapeFileItor = archiveFile.tapeFiles.find(1);
      ASSERT_EQ(1, copyNbToTapeFileItor->first);
      ASSERT_EQ(tapeFile1.vid, copyNbToTapeFileItor->second.vid);
      ASSERT_EQ(tapeFile1.fSeq, copyNbToTapeFileItor->second.fSeq);
      ASSERT_EQ(tapeFile1.blockId, copyNbToTapeFileItor->second.blockId);
      ASSERT_EQ(tapeFile1.compressedSize, copyNbToTapeFileItor->second.compressedSize);
      ASSERT_EQ(tapeFile1.copyNb, copyNbToTapeFileItor->second.copyNb);
    }

    {
      auto copyNbToTapeFileItor = archiveFile.tapeFiles.find(2);
      ASSERT_EQ(2, copyNbToTapeFileItor->first);
      ASSERT_EQ(tapeFile2.vid, copyNbToTapeFileItor->second.vid);
      ASSERT_EQ(tapeFile2.fSeq, copyNbToTapeFileItor->second.fSeq);
      ASSERT_EQ(tapeFile2.blockId, copyNbToTapeFileItor->second.blockId);
      ASSERT_EQ(tapeFile2.compressedSize, copyNbToTapeFileItor->second.compressedSize);
      ASSERT_EQ(tapeFile2.copyNb, copyNbToTapeFileItor->second.copyNb);
    }
  }

  {
    const std::list<common::dataStructures::TapeFile> tapeFiles = m_catalogue->getTapeFiles();

    ASSERT_EQ(2, tapeFiles.size());
  }
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, getTapeLastFseq_no_such_tape) {
  using namespace cta;

  const std::string vid = "V12345";
  ASSERT_THROW(m_catalogue->getTapeLastFSeq(vid), exception::Exception);
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, getTapeLastFseq) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes("", "", "", "", "", "", "", "").empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKey = "encryption_key";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const std::string comment = "create tape";

  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName,
    "create logical library");
  m_catalogue->createTapePool(m_cliSI, tapePoolName, 2, true, "create tape pool");
  m_catalogue->createTape(m_cliSI, vid, logicalLibraryName, tapePoolName,
    encryptionKey, capacityInBytes, disabledValue, fullValue,
    comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes("", "", "", "", "", "", "", "");

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(encryptionKey, tape.encryptionKey);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_EQ(comment, tape.comment);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
  ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_EQ(0, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 1);
  ASSERT_EQ(1, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 2);
  ASSERT_EQ(2, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 3);
  ASSERT_EQ(3, m_catalogue->getTapeLastFSeq(vid));

  // An increment greater than 1 should not be allowed
  ASSERT_THROW(m_catalogue->setTapeLastFSeq(vid, 5), exception::Exception);

  m_catalogue->setTapeLastFSeq(vid, 4);
  ASSERT_EQ(4, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 5);
  ASSERT_EQ(5, m_catalogue->getTapeLastFSeq(vid));

  // A decrement should not be allowed
  ASSERT_THROW(m_catalogue->setTapeLastFSeq(vid, 4), exception::Exception);

  m_catalogue->setTapeLastFSeq(vid, 6);
  ASSERT_EQ(6, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 7);
  ASSERT_EQ(7, m_catalogue->getTapeLastFSeq(vid));

  // Keeping the same lats FSeq should not be allowed
  ASSERT_THROW(m_catalogue->setTapeLastFSeq(vid, 7), exception::Exception);

  m_catalogue->setTapeLastFSeq(vid, 8);
  ASSERT_EQ(8, m_catalogue->getTapeLastFSeq(vid));
  m_catalogue->setTapeLastFSeq(vid, 9);
  ASSERT_EQ(9, m_catalogue->getTapeLastFSeq(vid));
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, getArchiveFile) {
  using namespace cta;

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_TRUE(NULL == m_catalogue->getArchiveFile(archiveFileId).get());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");

  catalogue::ArchiveFileRow row;
  row.archiveFileId = archiveFileId;
  row.diskFileId = "EOS_file_ID";
  row.size = 1;
  row.storageClassName = storageClassName;

  row.diskInstance = "disk_file_instance";
  row.diskFilePath = "disk_file_path";
  row.diskFileUser = "disk_file_user";
  row.diskFileGroup = "disk_file_group";
  row.diskFileRecoveryBlob = "disk_file_recovery_blob";

  m_catalogue->insertArchiveFile(row);

  {
    std::list<common::dataStructures::ArchiveFile> files;
    files = m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "");
    ASSERT_EQ(1, files.size());

    const common::dataStructures::ArchiveFile archiveFile = files.front();

    ASSERT_EQ(row.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(row.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(row.size, archiveFile.fileSize);
    ASSERT_EQ(row.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(row.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(row.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(row.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(row.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_TRUE(archiveFile.tapeFiles.empty());
  }

  ASSERT_TRUE(m_catalogue->getTapeFiles().empty());

  {
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile = m_catalogue->getArchiveFile(archiveFileId);
    ASSERT_TRUE(NULL != archiveFile.get());

    ASSERT_EQ(row.archiveFileId, archiveFile->archiveFileID);
    ASSERT_EQ(row.diskFileId, archiveFile->diskFileID);
    ASSERT_EQ(row.size, archiveFile->fileSize);
    ASSERT_EQ(row.storageClassName, archiveFile->storageClass);

    ASSERT_EQ(row.diskInstance, archiveFile->diskInstance);
    ASSERT_EQ(row.diskFilePath, archiveFile->drData.drPath);
    ASSERT_EQ(row.diskFileUser, archiveFile->drData.drOwner);
    ASSERT_EQ(row.diskFileGroup, archiveFile->drData.drGroup);
    ASSERT_EQ(row.diskFileRecoveryBlob, archiveFile->drData.drBlob);

    ASSERT_TRUE(archiveFile->tapeFiles.empty());
  }

  ASSERT_TRUE(m_catalogue->getTapeFiles().empty());
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, fileWrittenToTape_2_tape_files) {
  using namespace cta;

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_TRUE(NULL == m_catalogue->getArchiveFile(archiveFileId).get());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");

  const uint64_t archiveFileSize = 1;

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = "PUBLIC";
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.storageClassName     = storageClassName;
  file1Written.vid                  = "VID123";
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    std::unique_ptr<common::dataStructures::ArchiveFile> retrievedFile = m_catalogue->getArchiveFile(archiveFileId);
    ASSERT_TRUE(NULL != retrievedFile.get());

    ASSERT_EQ(file1Written.archiveFileId, retrievedFile->archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, retrievedFile->diskFileID);
    ASSERT_EQ(file1Written.size, retrievedFile->fileSize);
    ASSERT_EQ(file1Written.storageClassName, retrievedFile->storageClass);

    ASSERT_EQ(file1Written.diskInstance, retrievedFile->diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, retrievedFile->drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, retrievedFile->drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, retrievedFile->drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, retrievedFile->drData.drBlob);

    ASSERT_EQ(1, retrievedFile->tapeFiles.size());
  }

  ASSERT_EQ(1, m_catalogue->getTapeFiles().size());

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = "VID123";
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    std::unique_ptr<common::dataStructures::ArchiveFile> retrievedFile = m_catalogue->getArchiveFile(archiveFileId);
    ASSERT_TRUE(NULL != retrievedFile.get());

    ASSERT_EQ(file2Written.archiveFileId, retrievedFile->archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, retrievedFile->diskFileID);
    ASSERT_EQ(file2Written.size, retrievedFile->fileSize);
    ASSERT_EQ(file2Written.storageClassName, retrievedFile->storageClass);

    ASSERT_EQ(file2Written.diskInstance, retrievedFile->diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, retrievedFile->drData.drPath);
    ASSERT_EQ(file2Written.diskFileUser, retrievedFile->drData.drOwner);
    ASSERT_EQ(file2Written.diskFileGroup, retrievedFile->drData.drGroup);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, retrievedFile->drData.drBlob);

    ASSERT_EQ(2, retrievedFile->tapeFiles.size());
  }

  ASSERT_EQ(2, m_catalogue->getTapeFiles().size());
}

TEST_F(cta_catalogue_RdbmsCatalogueTest, fileWrittenToTape_2_tape_files_corrupted_diskFilePath) {
  using namespace cta;

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_TRUE(NULL == m_catalogue->getArchiveFile(archiveFileId).get());

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies,
    "create storage class");

  const uint64_t archiveFileSize = 1;

  catalogue::TapeFileWritten file1Written;
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = "PUBLIC";
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileUser         = "public_disk_user";
  file1Written.diskFileGroup        = "public_disk_group";
  file1Written.diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  file1Written.size                 = archiveFileSize;
  file1Written.storageClassName     = storageClassName;
  file1Written.vid                  = "VID123";
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    std::unique_ptr<common::dataStructures::ArchiveFile> retrievedFile = m_catalogue->getArchiveFile(archiveFileId);
    ASSERT_TRUE(NULL != retrievedFile.get());

    ASSERT_EQ(file1Written.archiveFileId, retrievedFile->archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, retrievedFile->diskFileID);
    ASSERT_EQ(file1Written.size, retrievedFile->fileSize);
    ASSERT_EQ(file1Written.storageClassName, retrievedFile->storageClass);

    ASSERT_EQ(file1Written.diskInstance, retrievedFile->diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, retrievedFile->drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, retrievedFile->drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, retrievedFile->drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, retrievedFile->drData.drBlob);

    ASSERT_EQ(1, retrievedFile->tapeFiles.size());
  }

  ASSERT_EQ(1, m_catalogue->getTapeFiles().size());

  catalogue::TapeFileWritten file2Written;
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFilePath         = "CORRUPTED disk file path";
  file2Written.diskFileUser         = file1Written.diskFileUser;
  file2Written.diskFileGroup        = file1Written.diskFileGroup;
  file2Written.diskFileRecoveryBlob = file1Written.diskFileRecoveryBlob;
  file2Written.size                 = archiveFileSize;
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = "VID123";
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;

  ASSERT_THROW(m_catalogue->fileWrittenToTape(file2Written), exception::Exception);
}

} // namespace unitTests
