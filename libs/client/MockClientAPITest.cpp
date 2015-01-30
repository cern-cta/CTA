#include "MockClientAPI.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockClientAPITest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockClientAPITest, createStorageClass_new) {
  using namespace cta;

  MockClientAPI api;
  UserIdentity requester;

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

  MockClientAPI api;
  UserIdentity requester;

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

  MockClientAPI api;
  UserIdentity requester;

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

  MockClientAPI api;
  UserIdentity requester;

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

  MockClientAPI api;
  UserIdentity requester;

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

  MockClientAPI api;
  UserIdentity requester;
  const std::string dirPath = "/";

  DirectoryIterator itor;
  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester,"/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_MockClientAPITest, createDirectory_empty_string) {
  using namespace cta;

  MockClientAPI api;
  UserIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_consecutive_slashes) {
  using namespace cta;

  MockClientAPI api;
  UserIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_invalid_chars) {
  using namespace cta;

  MockClientAPI api;
  UserIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockClientAPITest, createDirectory_top_level) {
  using namespace cta;

  MockClientAPI api;
  UserIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(api.createDirectory(requester, dirPath));
}

} // namespace unitTests
