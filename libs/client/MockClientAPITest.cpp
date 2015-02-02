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
  const UserIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid, "");
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
  const UserIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid, "");
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
  const UserIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid, "");
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
  const UserIdentity requester;

  {
    std::list<UserIdentity> adminUsers;
    ASSERT_NO_THROW(adminUsers = api.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  } 
  
  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid, "");
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
  const UserIdentity requester;

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
  const UserIdentity requester;

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
  const UserIdentity requester;
  
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
  const UserIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
  }
}

TEST_F(cta_client_MockClientAPITest, createStorageClass_already_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
  }
  
  ASSERT_THROW(api.createStorageClass(requester, name, nbCopies),
    std::exception);
}

TEST_F(cta_client_MockClientAPITest, createStorageClass_lexicographical_order) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  ASSERT_NO_THROW(api.createStorageClass(requester, "d", 1));
  ASSERT_NO_THROW(api.createStorageClass(requester, "b", 1));
  ASSERT_NO_THROW(api.createStorageClass(requester, "a", 1));
  ASSERT_NO_THROW(api.createStorageClass(requester, "c", 1));
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(4, storageClasses.size());

    ASSERT_EQ(std::string("a"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("b"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("c"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("d"), storageClasses.front().name);
  }
}

TEST_F(cta_client_MockClientAPITest, deleteStorageClass_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(requester, name, nbCopies));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_EQ(1, storageClasses.size());
  
    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);

    ASSERT_NO_THROW(api.deleteStorageClass(requester, name));
  }

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = api.getStorageClasses(requester));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_F(cta_client_MockClientAPITest, deleteStorageClass_non_existing) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;

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

TEST_F(cta_client_MockClientAPITest, getDirectoryContents_root_dir_is_empty) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;
  const std::string dirPath = "/";

  DirectoryIterator itor;
  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester,"/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_MockClientAPITest, createDirectory_empty_string) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_consecutive_slashes) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_invalid_chars) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_top_level) {
  using namespace cta;

  TestingMockClientAPI api;
  const UserIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.name);
}

} // namespace unitTests
