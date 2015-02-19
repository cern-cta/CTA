#include "MockMiddleTierAdmin.hpp"
#include "MockMiddleTierUser.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockMiddleTierUserTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockMiddleTierUserTest,
  getDirectoryContents_root_dir_is_empty) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  DirectoryIterator itor;
  ASSERT_NO_THROW(itor = api.getDirectoryContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_empty_string) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  createDirectory_consecutive_slashes) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_invalid_chars) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(api.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_top_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_second_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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

  ASSERT_TRUE(api.getDirectoryStorageClass(requester,
    "/grandparent/parent").empty());
}

TEST_F(cta_client_MockMiddleTierUserTest,
  createDirectory_inherit_storage_class) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  ASSERT_TRUE(userApi.getDirectoryStorageClass(requester, "/").empty());

  {
    MockMiddleTierAdmin adminApi(db);
    const std::string name = "TestStorageClass";
    const uint8_t nbCopies = 2;
    const std::string comment = "Comment";
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment));
  }

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(userApi.createDirectory(requester, topLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());

    ASSERT_TRUE(userApi.getDirectoryStorageClass(requester, "/grandparent").empty());

    ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, "/grandparent",
      "TestStorageClass"));
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    userApi.getDirectoryStorageClass(requester, "/grandparent"));

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(userApi.createDirectory(requester, secondLevelDirPath));
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    userApi.getDirectoryStorageClass(requester, "/grandparent/parent"));
}

TEST_F(cta_client_MockMiddleTierUserTest, deleteDirectory_root) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  ASSERT_THROW(api.deleteDirectory(requester, "/"), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, deleteDirectory_existing_top_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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

TEST_F(cta_client_MockMiddleTierUserTest,
  deleteDirectory_non_empty_top_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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

TEST_F(cta_client_MockMiddleTierUserTest,
  deleteDirectory_non_existing_top_level) {
  using namespace cta;
  
  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
  const SecurityIdentity requester;

  ASSERT_THROW(api.deleteDirectory(requester, "/grandparent"), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, setDirectoryStorageClass_top_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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
  {
    MockMiddleTierAdmin adminApi(db);
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  clearDirectoryStorageClass_top_level) {
  using namespace cta;

  MockMiddleTierDatabase db;
  MockMiddleTierUser api(db);
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
  MockMiddleTierAdmin adminApi(db);
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(api.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(adminApi.deleteStorageClass(requester, storageClassName),
    std::exception);

  ASSERT_NO_THROW(api.clearDirectoryStorageClass(requester, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = api.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));
}

} // namespace unitTests
