#include "TestingMockClientAPI.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockClientAPITest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockClientAPITest, createAdminUser_new) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_NO_THROW(api.createAdminUser(requester, adminUser1));

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().uid);
    ASSERT_EQ(adminUser1Gid, adminUsers.front().gid);
  }
}

TEST_F(cta_client_MockClientAPITest, createAdminUser_already_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_NO_THROW(api.createAdminUser(requester, adminUser1));

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().uid);
    ASSERT_EQ(adminUser1Gid, adminUsers.front().gid);
  }

  ASSERT_THROW(api.createAdminUser(requester, adminUser1), std::exception);

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteAdminUser_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_NO_THROW(api.createAdminUser(requester, adminUser1));

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().uid);
    ASSERT_EQ(adminUser1Gid, adminUsers.front().gid);
  }

  ASSERT_NO_THROW(api.deleteAdminUser(requester, adminUser1));
  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteAdminUser_non_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  } 
  
  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_THROW(api.deleteAdminUser(requester, adminUser1), std::exception);

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, createAdminHost_new) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  ASSERT_NO_THROW(api.createAdminHost(requester, adminHost1));

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteAdminHost_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  ASSERT_NO_THROW(api.createAdminHost(requester, adminHost1));

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front());
  }

  ASSERT_NO_THROW(api.deleteAdminHost(requester, adminHost1));

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteAdminHost_non_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  
  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  ASSERT_THROW(api.deleteAdminHost(requester, adminHost1), std::exception);

  {
    std::list<std::string> adminHosts;
    ASSERT_NO_THROW(adminHosts = api.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, createStorageClass_new) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
}

TEST_F(cta_client_MockClientAPITest, createStorageClass_already_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
  
  ASSERT_THROW(api.createStorageClass(requester, name, nbCopies, comment),
    std::exception);
}

TEST_F(cta_client_MockClientAPITest, createStorageClass_lexicographical_order) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  ASSERT_NO_THROW(api.createStorageClass(requester, "d", 1, "Comment d"));
  ASSERT_NO_THROW(api.createStorageClass(requester, "b", 1, "Comment b"));
  ASSERT_NO_THROW(api.createStorageClass(requester, "a", 1, "Comment a"));
  ASSERT_NO_THROW(api.createStorageClass(requester, "c", 1, "Comment c"));
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
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

TEST_F(cta_client_MockClientAPITest, deleteStorageClass_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());
  
    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());

    ASSERT_NO_THROW(api.deleteStorageClass(requester, name));
  }

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteStorageClass_in_use) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, "/", name));

  ASSERT_THROW(api.deleteStorageClass(requester, name), std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteStorageClass_non_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  ASSERT_THROW(api.deleteStorageClass(requester, name), std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, createTapePool_new) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createTapePool(requester, name, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(name, tapePool.getName());
  }
}

TEST_F(cta_client_MockClientAPITest, createTapePool_already_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createTapePool(requester, name, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(name, tapePool.getName());
  }
  
  ASSERT_THROW(api.createTapePool(requester, name, comment), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createTapePool_lexicographical_order) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  ASSERT_NO_THROW(api.createTapePool(requester, "d", "Comment d"));
  ASSERT_NO_THROW(api.createTapePool(requester, "b", "Comment b"));
  ASSERT_NO_THROW(api.createTapePool(requester, "a", "Comment a"));
  ASSERT_NO_THROW(api.createTapePool(requester, "c", "Comment c"));
  
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
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

TEST_F(cta_client_MockClientAPITest, deleteTapePool_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createTapePool(requester, name, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());
  
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(name, tapePool.getName());

    ASSERT_NO_THROW(api.deleteTapePool(requester, name));
  }

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

/*
TEST_F(cta_client_MockClientAPITest, deleteTapePool_in_use) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createTapePool(requester, name, nbCopies, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(name, tapePool.getName());
    ASSERT_EQ(nbCopies, tapePool.getNbCopies());
  }

  ASSERT_NO_THROW(api.setDirectoryTapePool(requester, "/", name));

  ASSERT_THROW(api.deleteTapePool(requester, name), std::exception);

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(name, tapePool.getName());
    ASSERT_EQ(nbCopies, tapePool.getNbCopies());
  }
}
*/

TEST_F(cta_client_MockClientAPITest, deleteTapePool_non_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  ASSERT_THROW(api.deleteTapePool(requester, name), std::exception);

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = api.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, getDirectoryContents_root_dir_is_empty) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  DirectoryIterator itor;
  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_MockClientAPITest, createDirectory_empty_string) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_consecutive_slashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_invalid_chars) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());
}

TEST_F(cta_client_MockClientAPITest, createDirectory_second_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  ASSERT_TRUE(api.getDirectoryStorageClass(requester, "/").empty());

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(api.createDirectory(requester, topLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_TRUE(api.getDirectoryStorageClass(requester, "/grandparent").empty());

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(api.createDirectory(requester, secondLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_TRUE(api.getDirectoryStorageClass(requester, "/grandparent/parent").empty());
}

TEST_F(cta_client_MockClientAPITest, createDirectory_inherit_storage_class) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  ASSERT_TRUE(api.getDirectoryStorageClass(requester, "/").empty());

  {
    const std::string name = "TestStorageClass";
    const uint8_t nbCopies = 2;
    const std::string comment = "Comment";
    ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies, comment));
  }

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(api.createDirectory(requester, topLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());

    ASSERT_TRUE(api.getDirectoryStorageClass(requester, "/grandparent").empty());

    ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, "/grandparent",
      "TestStorageClass"));
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    api.getDirectoryStorageClass(requester, "/grandparent"));

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(api.createDirectory(requester, secondLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    api.getDirectoryStorageClass(requester, "/grandparent/parent"));
}

TEST_F(cta_client_MockClientAPITest, deleteDirectory_root) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  ASSERT_THROW(api.deleteDirectory(requester, "/"), std::exception);
}

TEST_F(cta_client_MockClientAPITest, deleteDirectory_existing_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_NO_THROW(api.deleteDirectory(requester, "/grandparent"));

  {
    DirectoryIterator itor;
  
    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));
  
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteDirectory_non_empty_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(api.createDirectory(requester, topLevelDirPath));

    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(api.createDirectory(requester, secondLevelDirPath));

    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_THROW(api.deleteDirectory(requester, "/grandparent"), std::exception);

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteDirectory_non_existing_top_level) {
  using namespace cta;
  
  TestingMockClientAPI api;
  const SecurityIdentity requester;

  ASSERT_THROW(api.deleteDirectory(requester, "/grandparent"), std::exception);
}

TEST_F(cta_client_MockClientAPITest, setDirectoryStorageClass_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
    const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_F(cta_client_MockClientAPITest, clearDirectoryStorageClass_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(api.createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(api.deleteStorageClass(requester, storageClassName),
    std::exception);

  ASSERT_NO_THROW(api.clearDirectoryStorageClass(requester, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(api.deleteStorageClass(requester, storageClassName));
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_emptyString) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s;
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_noSlashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("NO_SLASHES");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_oneLeftSlash) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("/VALUE");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_twoLeftSlashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("//VALUE");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_oneRightSlash) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("VALUE/");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_twoRightSlashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("VALUE//");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_oneLeftAndOneRightSlash) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("/VALUE/");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, trimSlashes_twoLeftAndTwoRightSlashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string s("//VALUE//");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_MockClientAPITest, getEnclosingDirPath_empty_string) {
  using namespace cta;
    
  TestingMockClientAPI api;
  const std::string dirPath = "";

  ASSERT_THROW(api.getEnclosingDirPath(dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, getEnclosingDirPath_root) {
  using namespace cta;
    
  TestingMockClientAPI api;
  const std::string dirPath = "/";

  std::string enclosingDirPath;
  ASSERT_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath),
    std::exception);
}

TEST_F(cta_client_MockClientAPITest, getEnclosingDirPath_grandparent) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string dirPath = "/grandparent";
    
  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/"), enclosingDirPath);
}

TEST_F(cta_client_MockClientAPITest, getEnclosingDirPath_grandparent_parent) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string dirPath = "/grandparent/parent";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/"), enclosingDirPath);
}

TEST_F(cta_client_MockClientAPITest, getEnclosingDirPath_grandparent_parent_child) {
  using namespace cta;

  TestingMockClientAPI api;
  const std::string dirPath = "/grandparent/parent/child";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/parent/"), enclosingDirPath);
}

TEST_F(cta_client_MockClientAPITest, splitString_goodDay) {
  using namespace cta;
  const std::string line("col0 col1 col2 col3 col4 col5 col6 col7");
  std::vector<std::string> columns;

  TestingMockClientAPI api;

  ASSERT_NO_THROW(api.splitString(line, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)8, columns.size());
  ASSERT_EQ(std::string("col0"), columns[0]);
  ASSERT_EQ(std::string("col1"), columns[1]);
  ASSERT_EQ(std::string("col2"), columns[2]);
  ASSERT_EQ(std::string("col3"), columns[3]);
  ASSERT_EQ(std::string("col4"), columns[4]);
  ASSERT_EQ(std::string("col5"), columns[5]);
  ASSERT_EQ(std::string("col6"), columns[6]);
  ASSERT_EQ(std::string("col7"), columns[7]);
}

TEST_F(cta_client_MockClientAPITest, splitString_emptyString) {
  using namespace cta;
  const std::string emptyString;
  std::vector<std::string> columns;

  TestingMockClientAPI api;

  ASSERT_NO_THROW(api.splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

TEST_F(cta_client_MockClientAPITest, splitString_noSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  TestingMockClientAPI api;

  ASSERT_NO_THROW(api.splitString(stringContainingNoSeparator, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

} // namespace unitTests
