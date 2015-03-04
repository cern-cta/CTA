#include "MockMiddleTierAdmin.hpp"
#include "MockMiddleTierUser.hpp"
#include "SqliteMiddleTierAdmin.hpp"
#include "SqliteMiddleTierUser.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_SqliteMiddleTierAdminTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_SqliteMiddleTierAdminTest, createStorageClass_new) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies,
    comment));

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

TEST_F(cta_client_SqliteMiddleTierAdminTest,
  createStorageClass_already_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
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

TEST_F(cta_client_SqliteMiddleTierAdminTest,
  createStorageClass_lexicographical_order) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
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

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteStorageClass_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
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

TEST_F(cta_client_SqliteMiddleTierAdminTest,
  deleteStorageClass_in_use_by_directory) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
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

  MockDatabase db;
  SqliteMiddleTierUser userApi(db,sqlitedb);
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

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteStorageClass_in_use_by_route) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
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

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
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

  ASSERT_NO_THROW(adminApi.deleteArchiveRoute(requester, storageClassName,
    copyNb));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = adminApi.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteStorageClass_non_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
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

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteTapePool_in_use) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
  }

  ASSERT_THROW(adminApi.deleteTapePool(requester, tapePoolName), std::exception);

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
  }
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, createArchiveRoute_new) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
  }
}

TEST_F(cta_client_SqliteMiddleTierAdminTest,
  createArchiveRoute_already_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
  }

  ASSERT_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteArchiveRoute_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archiveRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archiveRoute.getTapePoolName());
  }

  ASSERT_NO_THROW(adminApi.deleteArchiveRoute(requester, storageClassName,
    copyNb));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, deleteArchiveRoute_non_existing) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = adminApi.getArchiveRoutes(requester));
    ASSERT_TRUE(archiveRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_THROW(adminApi.deleteArchiveRoute(requester, tapePoolName, copyNb),
    std::exception);
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, createTape_new) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = adminApi.getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = adminApi.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
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
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
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

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(adminApi.createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment));
  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = adminApi.getTapes(requester));
    ASSERT_EQ(1, tapes.size()); 
  
    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(tapePoolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  } 
}

TEST_F(cta_client_SqliteMiddleTierAdminTest,
  createTape_new_non_existing_library) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = adminApi.getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = adminApi.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
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

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(adminApi.createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierAdminTest, createTape_new_non_existing_pool) {
  using namespace cta;

  SqliteDatabase sqlitedb;
  SqliteMiddleTierAdmin adminApi(sqlitedb);
  const SecurityIdentity requester;

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = adminApi.getLogicalLibraries(requester));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = adminApi.getTapePools(requester));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = adminApi.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
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
  }

  const std::string tapePoolName = "TestTapePool";

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(adminApi.createTape(requester, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

} // namespace unitTests
