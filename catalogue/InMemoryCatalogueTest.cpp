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
#include "catalogue/RdbmsCatalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <set>

namespace unitTests {

namespace {
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
}

class cta_catalogue_InMemoryCatalogueTest : public ::testing::Test {
public:
  cta_catalogue_InMemoryCatalogueTest():
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
    using namespace cta::catalogue;

    const DbLogin catalogueLogin(DbLogin::DBTYPE_IN_MEMORY, "", "", "");
    m_catalogue.reset(dynamic_cast<RdbmsCatalogue*>(CatalogueFactory::create(catalogueLogin)));
  }

  virtual void TearDown() {
    m_catalogue.reset();
  }

  std::unique_ptr<cta::catalogue::RdbmsCatalogue> m_catalogue;
  const std::string m_bootstrapComment;
  cta::common::dataStructures::SecurityIdentity m_cliSI;
  cta::common::dataStructures::SecurityIdentity m_bootstrapAdminSI;
  cta::common::dataStructures::SecurityIdentity m_adminSI;
};

TEST_F(cta_catalogue_InMemoryCatalogueTest, createBootstrapAdminAndHostNoAuth) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createAdminUser) {
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

  const std::string createAdminUserComment = "create admin user";
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createAdminUser_same_twice) {
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

  m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminUser(m_bootstrapAdminSI, m_adminSI.username,
    "comment 2"), exception::Exception);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createAdminHost) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createAdminHost_same_twice) {
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
    anotherAdminHost, "comment 2"), exception::Exception);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, isAdmin_false) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->isAdmin(m_cliSI));
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, isAdmin_true) {
  using namespace cta;

  m_catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  ASSERT_TRUE(m_catalogue->isAdmin(m_bootstrapAdminSI));
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createStorageClass) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    storageClass.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createStorageClass_same_twice) {
  using namespace cta;

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  const std::string comment = "create storage class";
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, comment);
  ASSERT_THROW(m_catalogue->createStorageClass(m_cliSI,
    storageClassName, nbCopies, comment), exception::Exception);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createTapePool) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_F(cta_catalogue_InMemoryCatalogueTest, createTapePool_same_twice) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createArchiveRoute) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
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
  
TEST_F(cta_catalogue_InMemoryCatalogueTest, createArchiveRouteTapePool_same_twice) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createArchiveRoute_deleteStorageClass) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createLogicalLibrary) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}
  
TEST_F(cta_catalogue_InMemoryCatalogueTest, createLogicalLibrary_same_twice) {
  using namespace cta;
  
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "create logical library";
  m_catalogue->createLogicalLibrary(m_cliSI, logicalLibraryName, comment);
  ASSERT_THROW(m_catalogue->createLogicalLibrary(m_cliSI,
    logicalLibraryName, comment),
    exception::Exception);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createTape) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createTape_same_twice) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, getTapesForWriting) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createMountPolicy) {
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    group.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, createMountPolicy_same_twice) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createUser) {
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

  const std::list<common::dataStructures::Requester> users = m_catalogue->getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(comment, user.comment);
  ASSERT_EQ(m_cliSI.username, user.creationLog.username);
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, createUser_same_twice) {
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, prepareForNewFile) {
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

  const std::list<common::dataStructures::Requester> users = m_catalogue->getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(userComment, user.comment);
  ASSERT_EQ(m_cliSI.username, user.creationLog.username);
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
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

TEST_F(cta_catalogue_InMemoryCatalogueTest, prepareToRetrieveFile) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes("", "", "", "", "", "", "", "").empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
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
  m_catalogue->createTape(m_cliSI, vid1, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes,
    disabledValue, fullValue, comment);
  m_catalogue->createTape(m_cliSI, vid2, logicalLibraryName, tapePoolName, encryptionKey, capacityInBytes,
    disabledValue, fullValue, comment);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes("", "", "", "", "", "", "", "");
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
    ASSERT_EQ(comment, tape.comment);

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
    ASSERT_EQ(comment, tape.comment);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");

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
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
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
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
  }

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

  const std::list<common::dataStructures::Requester> users = m_catalogue->getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(userComment, user.comment);
  ASSERT_EQ(m_cliSI.username, user.creationLog.username);
  ASSERT_EQ(m_cliSI.host, user.creationLog.host);
  ASSERT_EQ(user.creationLog, user.lastModificationLog);  

  const common::dataStructures::MountPolicy policy = m_catalogue->getMountPolicyForAUser(userIdentity);

  ASSERT_EQ(archivePriority, policy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, policy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, policy.maxDrivesAllowed);
  ASSERT_EQ(retrievePriority, policy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, policy.retrieveMinRequestAge);

  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->prepareToRetrieveFile(archiveFileId, userIdentity);

  ASSERT_EQ(2, queueCriteria.tapeFiles.size());
  ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, fileWrittenToTape_2_tape_files_different_tapes) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
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
  m_catalogue->createTape(m_cliSI, vid1, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);
  m_catalogue->createTape(m_cliSI, vid2, logicalLibraryName, tapePoolName,
                          encryptionKey, capacityInBytes, disabledValue, fullValue,
                          comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes("", "", "", "", "", "", "", "");

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
    ASSERT_EQ(comment, tape.comment);

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
    ASSERT_EQ(comment, tape.comment);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");

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
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(file1Written.vid, "", "", "", "", "", "", "");
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
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
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  m_catalogue->fileWrittenToTape(file2Written);

  {
    ASSERT_EQ(2, m_catalogue->getTapes("", "", "", "", "", "", "", "").size());
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(file2Written.vid, "", "", "", "", "", "", "");
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file2Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file2Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file2Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = copyNbToTapeFile2Itor->second;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.compressedSize, tapeFile2.compressedSize);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, fileWrittenToTape_2_tape_files_same_tape) {
  using namespace cta;

  const std::string vid = "VID123";
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
    ASSERT_EQ(comment, tape.comment);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 2;
  m_catalogue->createStorageClass(m_cliSI, storageClassName, nbCopies, "create storage class");

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
  file1Written.vid                  = vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(file1Written.vid, "", "", "", "", "", "", "");
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
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
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = vid;
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;
  ASSERT_THROW(m_catalogue->fileWrittenToTape(file2Written), exception::Exception);
}

TEST_F(cta_catalogue_InMemoryCatalogueTest, fileWrittenToTape_2_tape_files_corrupted_diskFilePath) {
  using namespace cta;

  const std::string vid = "VID123";
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
  ASSERT_EQ(m_cliSI.username, creationLog.username);
  ASSERT_EQ(m_cliSI.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  const uint64_t archiveFileId = 1234;

  ASSERT_TRUE(m_catalogue->getArchiveFiles("", "", "", "", "", "", "", "", "").empty());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

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
  file1Written.vid                  = vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.compressedSize       = 1;
  file1Written.copyNb               = 1;
  m_catalogue->fileWrittenToTape(file1Written);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileID);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.drData.drPath);
    ASSERT_EQ(file1Written.diskFileUser, archiveFile.drData.drOwner);
    ASSERT_EQ(file1Written.diskFileGroup, archiveFile.drData.drGroup);
    ASSERT_EQ(file1Written.diskFileRecoveryBlob, archiveFile.drData.drBlob);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = copyNbToTapeFile1Itor->second;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.compressedSize, tapeFile1.compressedSize);
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
  file2Written.storageClassName     = storageClassName;
  file2Written.vid                  = "VID123";
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.compressedSize       = 1;
  file2Written.copyNb               = 2;

  ASSERT_THROW(m_catalogue->fileWrittenToTape(file2Written), exception::Exception);
}

} // namespace unitTests
