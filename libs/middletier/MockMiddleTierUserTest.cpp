#include "MockMiddleTierAdmin.hpp"
#include "MockMiddleTierUser.hpp"

#include <gtest/gtest.h>
#include <set>

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

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  DirectoryIterator itor;
  ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_empty_string) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(userApi.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  createDirectory_consecutive_slashes) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(userApi.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_invalid_chars) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(userApi.createDirectory(requester, dirPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_top_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());
}

TEST_F(cta_client_MockMiddleTierUserTest, createDirectory_second_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  ASSERT_TRUE(userApi.getDirectoryStorageClass(requester, "/").empty());

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
  }

  ASSERT_TRUE(userApi.getDirectoryStorageClass(requester, "/grandparent").empty());

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

  ASSERT_TRUE(userApi.getDirectoryStorageClass(requester,
    "/grandparent/parent").empty());
}

TEST_F(cta_client_MockMiddleTierUserTest,
  createDirectory_inherit_storage_class) {
  using namespace cta;

  MockDatabase db;
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

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  ASSERT_THROW(userApi.deleteDirectory(requester, "/"), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, deleteDirectory_existing_top_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_NO_THROW(userApi.deleteDirectory(requester, "/grandparent"));

  {
    DirectoryIterator itor;
  
    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));
  
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  deleteDirectory_non_empty_top_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(userApi.createDirectory(requester, topLevelDirPath));

    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(userApi.createDirectory(requester, secondLevelDirPath));

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

  ASSERT_THROW(userApi.deleteDirectory(requester, "/grandparent"), std::exception);

  {
    DirectoryIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirectoryEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  deleteDirectory_non_existing_top_level) {
  using namespace cta;
  
  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  ASSERT_THROW(userApi.deleteDirectory(requester, "/grandparent"), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, setDirectoryStorageClass_top_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirectoryStorageClass(requester, dirPath));
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

  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  clearDirectoryStorageClass_top_level) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  DirectoryIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirectoryEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string comment = "Comment";
  MockMiddleTierAdmin adminApi(db);
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirectoryStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(adminApi.deleteStorageClass(requester, storageClassName),
    std::exception);

  ASSERT_NO_THROW(userApi.clearDirectoryStorageClass(requester, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirectoryStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));
}

TEST_F(cta_client_MockMiddleTierUserTest, archive_to_new_file) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  const uint8_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_NO_THROW(userApi.archive(requester, srcUrls, dstPath));

  {
    DirectoryIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirectoryEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirectoryEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirectoryIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirectoryEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirectoryEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirectoryEntry entry;
    ASSERT_NO_THROW(entry = userApi.stat(requester, dstPath));
    ASSERT_EQ(DirectoryEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const std::map<TapePool, std::list<ArchivalJob> > allJobs =
      userApi.getArchivalJobs(requester);
    ASSERT_EQ(1, allJobs.size());
    std::map<TapePool, std::list<ArchivalJob> >::const_iterator
      poolItor = allJobs.begin();
    ASSERT_FALSE(poolItor == allJobs.end());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    const std::list<ArchivalJob> &poolJobs = poolItor->second;
    ASSERT_EQ(1, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getSrcUrl());
      dstPaths.insert(jobItor->getDstPath());
    }
    ASSERT_EQ(1, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl") == srcUrls.end());
    ASSERT_EQ(1, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/parent_file") == dstPaths.end());
  }

  {
    const std::list<ArchivalJob> poolJobs = userApi.getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(1, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getSrcUrl());
      dstPaths.insert(jobItor->getDstPath());
    }
    ASSERT_EQ(1, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl") == srcUrls.end());
    ASSERT_EQ(1, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/parent_file") == dstPaths.end());
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_new_file_with_zero_copy_storage_class) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, archive_to_new_file_with_no_route) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_new_file_with_incomplete_routing) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  const uint8_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, archive_to_directory) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  const uint8_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_NO_THROW(userApi.archive(requester, srcUrls, dstPath));

  {
    DirectoryIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirectoryEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirectoryEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    std::set<std::string> archiveFileNames;
    DirectoryIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirectoryContents(requester,
      "/grandparent"));
    while(itor.hasMore()) {
      const DirectoryEntry entry = itor.next();
      archiveFileNames.insert(entry.getName());
    }
    ASSERT_EQ(4, archiveFileNames.size());
    ASSERT_TRUE(archiveFileNames.find("diskUrl1") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("diskUrl2") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("diskUrl3") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("diskUrl4") != archiveFileNames.end());
  }

  {
    const std::map<TapePool, std::list<ArchivalJob> > allJobs =
      userApi.getArchivalJobs(requester);
    ASSERT_EQ(1, allJobs.size());
    std::map<TapePool, std::list<ArchivalJob> >::const_iterator
      poolItor = allJobs.begin();
    ASSERT_FALSE(poolItor == allJobs.end());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    const std::list<ArchivalJob> &poolJobs = poolItor->second;
    ASSERT_EQ(4, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getSrcUrl());
      dstPaths.insert(jobItor->getDstPath());
    }
    ASSERT_EQ(4, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl1") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl2") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl3") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl4") == srcUrls.end());
    ASSERT_EQ(4, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl1") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl2") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl3") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl4") == srcUrls.end());
  }

  {
    const std::list<ArchivalJob> poolJobs = userApi.getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(4, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getSrcUrl());
      dstPaths.insert(jobItor->getDstPath());
    }
    ASSERT_EQ(4, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl1") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl2") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl3") == srcUrls.end());
    ASSERT_FALSE(srcUrls.find("diskUrl4") == srcUrls.end());
    ASSERT_EQ(4, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl1") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl2") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl3") == srcUrls.end());
    ASSERT_FALSE(dstPaths.find("/grandparent/diskUrl4") == srcUrls.end());
  }
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_directory_without_storage_class) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_directory_with_zero_copy_storage_class) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest, archive_to_directory_with_no_route) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_MockMiddleTierUserTest,
  archive_to_directory_with_incomplete_routing) {
  using namespace cta;

  MockDatabase db;
  MockMiddleTierAdmin adminApi(db);
  MockMiddleTierUser userApi(db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint8_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDirectory(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirectoryStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, tapePoolComment));

  const uint8_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(adminApi.createArchiveRoute(requester, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

} // namespace unitTests
