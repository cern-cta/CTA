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

TEST_F(cta_client_MockMiddleTierAdminTest, createStorageClass_new) {
  using namespace cta;

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

TEST_F(cta_client_MockMiddleTierAdminTest, deleteTapePool_in_use) {
  using namespace cta;

  MockDatabase db;
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

TEST_F(cta_client_MockMiddleTierAdminTest, createMigrationRoute_new) {
  using namespace cta;

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

  MockDatabase db;
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

} // namespace unitTests
