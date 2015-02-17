#include "MockMiddleTierAdmin.hpp"
#include "MockMiddleTierUser.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockMiddleTierAdminTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockMiddleTierAdminTest, createAdminUser_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createAdminUser_already_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }

  ASSERT_THROW(adminApi.createAdminUser(requester, adminUser1, comment),
    std::exception);

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteAdminUser_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }

  ASSERT_NO_THROW(adminApi.deleteAdminUser(requester, adminUser1));
  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteAdminUser_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  } 
  
  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_THROW(adminApi.deleteAdminUser(requester, adminUser1), std::exception);

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = adminApi.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createAdminHost_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createAdminHost(requester, adminHost1, comment));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front().getName());
    ASSERT_EQ(comment, adminHosts.front().getComment());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteAdminHost_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createAdminHost(requester, adminHost1, comment));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front().getName());
    ASSERT_EQ(comment, adminHosts.front().getComment());
  }

  ASSERT_NO_THROW(adminApi.deleteAdminHost(requester, adminHost1));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteAdminHost_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;
  
  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  ASSERT_THROW(adminApi.deleteAdminHost(requester, adminHost1), std::exception);

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = adminApi.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createStorageClass_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  createStorageClass_already_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
  
  ASSERT_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment),
    std::exception);
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  createStorageClass_lexicographical_order) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  ASSERT_NO_THROW(adminApi.createStorageClass(requester, "d", 1, "Comment d"));
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, "b", 1, "Comment b"));
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, "a", 1, "Comment a"));
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, "c", 1, "Comment c"));
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(4, storageClasses.size());

    ASSERT_EQ(std::string("a"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("b"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("c"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("d"), storageClasses.front().getName());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteStorageClass_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());
  
    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());

    ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, name));
  }

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  deleteStorageClass_in_use_by_directory) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  MockMiddleTierUser userApi(db);
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, "/",
    storageClassName));

  ASSERT_THROW(adminApi.deleteStorageClass(requester, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(userApi.clearDirectoryStorageClass(requester, "/"));

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteStorageClass_in_use_by_route) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
  }

  const uint8_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }

  ASSERT_THROW(adminApi.deleteStorageClass(requester, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(adminApi.deleteMigrationRoute(requester, storageClassName,
    copyNb));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteStorageClass_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  ASSERT_THROW(adminApi.deleteStorageClass(requester, name), std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createTapePool_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives, 
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createTapePool_already_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives, 
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  }
  
  ASSERT_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment), std::exception);
}

TEST_F(cta_client_MockMiddleTierAdminTest, createTapePool_lexicographical_order) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;

  ASSERT_NO_THROW(adminApi.createTapePool(requester, "d", nbDrives, nbPartialTapes,
    "Comment d"));
  ASSERT_NO_THROW(adminApi.createTapePool(requester, "b", nbDrives, nbPartialTapes,
    "Comment b"));
  ASSERT_NO_THROW(adminApi.createTapePool(requester, "a", nbDrives, nbPartialTapes,
    "Comment a"));
  ASSERT_NO_THROW(adminApi.createTapePool(requester, "c", nbDrives, nbPartialTapes,
    "Comment c"));
  
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_EQ(4, tapePools.size());

    ASSERT_EQ(std::string("a"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("b"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("c"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("d"), tapePools.front().getName());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteTapePool_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());
  
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());

    ASSERT_NO_THROW(adminApi.deleteTapePool(requester, tapePoolName));
  }

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteTapePool_in_use) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint8_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint8_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }

  ASSERT_THROW(adminApi.deleteTapePool(requester, tapePoolName), std::exception);

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteTapePool_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  ASSERT_THROW(adminApi.deleteTapePool(requester, name), std::exception);

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = adminApi.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, createMigrationRoute_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint8_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint8_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  createMigrationRoute_already_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint8_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint8_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }

  ASSERT_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment), std::exception);
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteMigrationRoute_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint8_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint8_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createMigrationRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_EQ(1, migrationRoutes.size());

    MigrationRoute migrationRoute;
    ASSERT_NO_THROW(migrationRoute = migrationRoutes.front());
    ASSERT_EQ(storageClassName, migrationRoute.getStorageClassName());
    ASSERT_EQ(copyNb, migrationRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, migrationRoute.getTapePoolName());
  }

  ASSERT_NO_THROW(adminApi.deleteMigrationRoute(requester, storageClassName,
    copyNb));

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteMigrationRoute_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<MigrationRoute> migrationRoutes;
    ASSERT_NO_THROW(migrationRoutes = adminApi.getMigrationRoutes(requester));
    ASSERT_TRUE(migrationRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint8_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint8_t copyNb = 1;
  ASSERT_THROW(adminApi.deleteMigrationRoute(requester, tapePoolName, copyNb),
    std::exception);
}

TEST_F(cta_client_MockMiddleTierAdminTest, createLogicalLibrary_new) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Comment";
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, libraryName,
    libraryComment));

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_EQ(1, libraries.size());

    LogicalLibrary LogicalLibrary;
    ASSERT_NO_THROW(LogicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, LogicalLibrary.getName());
    ASSERT_EQ(libraryComment, LogicalLibrary.getComment());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  createLogicalLibrary_already_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Comment";
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, libraryName,
    libraryComment));

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_EQ(1, libraries.size());

    LogicalLibrary LogicalLibrary;
    ASSERT_NO_THROW(LogicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, LogicalLibrary.getName());
    ASSERT_EQ(libraryComment, LogicalLibrary.getComment());
  }
  
  ASSERT_THROW(adminApi.createLogicalLibrary(requester, libraryName,
    libraryComment), std::exception);
}

TEST_F(cta_client_MockMiddleTierAdminTest,
  createLogicalLibrary_lexicographical_order) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, "d", "Comment d"));
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, "b", "Comment b"));
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, "a", "Comment a"));
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, "c", "Comment c"));
  
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_EQ(4, libraries.size());

    ASSERT_EQ(std::string("a"), libraries.front().getName());
    libraries.pop_front();
    ASSERT_EQ(std::string("b"), libraries.front().getName());
    libraries.pop_front();
    ASSERT_EQ(std::string("c"), libraries.front().getName());
    libraries.pop_front();
    ASSERT_EQ(std::string("d"), libraries.front().getName());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteLogicalLibrary_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Comment";
  ASSERT_NO_THROW(adminApi.createLogicalLibrary(requester, libraryName,
    libraryComment));

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.getName());
    ASSERT_EQ(libraryComment, logicalLibrary.getComment());

    ASSERT_NO_THROW(adminApi.deleteLogicalLibrary(requester, libraryName));
  }

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }
}

TEST_F(cta_client_MockMiddleTierAdminTest, deleteLogicalLibrary_non_existing) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierAdmin adminApi(db);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  ASSERT_THROW(adminApi.deleteLogicalLibrary(requester, libraryName),
    std::exception);

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }
}

} // namespace unitTests
