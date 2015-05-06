#include "cta/SqliteMiddleTierAdmin.hpp"
#include "cta/SqliteMiddleTierUser.hpp"

#include <gtest/gtest.h>
#include <set>

namespace unitTests {

class cta_client_SqliteMiddleTierUserTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_SqliteMiddleTierUserTest,
  getDirContents_root_dir_is_empty) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  DirIterator itor;
  ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_F(cta_client_SqliteMiddleTierUserTest, createDir_empty_string) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(userApi.createDir(requester, dirPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  createDir_consecutive_slashes) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(userApi.createDir(requester, dirPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, createDir_invalid_chars) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(userApi.createDir(requester, dirPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, createDir_top_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());
}

TEST_F(cta_client_SqliteMiddleTierUserTest, createDir_second_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;

  ASSERT_TRUE(userApi.getDirStorageClass(requester, "/").empty());

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(userApi.createDir(requester, topLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_TRUE(userApi.getDirStorageClass(requester, "/grandparent").empty());

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(userApi.createDir(requester, secondLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_TRUE(userApi.getDirStorageClass(requester,
    "/grandparent/parent").empty());
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  createDir_inherit_storage_class) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;

  ASSERT_TRUE(userApi.getDirStorageClass(requester, "/").empty());

  {
    SqliteMiddleTierAdmin adminApi(vfs, db);
    const std::string name = "TestStorageClass";
    const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, name, nbCopies, comment));
  }

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(userApi.createDir(requester, topLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());

    ASSERT_TRUE(userApi.getDirStorageClass(requester, "/grandparent").empty());

    ASSERT_NO_THROW(userApi.setDirStorageClass(requester, "/grandparent",
      "TestStorageClass"));
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    userApi.getDirStorageClass(requester, "/grandparent"));

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(userApi.createDir(requester, secondLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    userApi.getDirStorageClass(requester, "/grandparent/parent"));
}

TEST_F(cta_client_SqliteMiddleTierUserTest, deleteDir_root) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  ASSERT_THROW(userApi.deleteDir(requester, "/"), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, deleteDir_existing_top_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_NO_THROW(userApi.deleteDir(requester, "/grandparent"));

  {
    DirIterator itor;
  
    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));
  
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  deleteDir_non_empty_top_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(userApi.createDir(requester, topLevelDirPath));

    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(userApi.createDir(requester, secondLevelDirPath));

    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_THROW(userApi.deleteDir(requester, "/grandparent"), std::exception);

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  deleteDir_non_existing_top_level) {
  using namespace cta;
  
  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;

  ASSERT_THROW(userApi.deleteDir(requester, "/grandparent"), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, setDirStorageClass_top_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
  {
    SqliteMiddleTierAdmin adminApi(vfs, db);
    ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  clearDirStorageClass_top_level) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  SqliteMiddleTierAdmin adminApi(vfs, db);
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(adminApi.deleteStorageClass(requester, storageClassName),
    std::exception);

  ASSERT_NO_THROW(userApi.clearDirStorageClass(requester, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = userApi.getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(adminApi.deleteStorageClass(requester, storageClassName));
}

TEST_F(cta_client_SqliteMiddleTierUserTest, archive_to_new_file) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(adminApi.createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_NO_THROW(userApi.archive(requester, srcUrls, dstPath));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirContents(requester,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = userApi.stat(requester, dstPath));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
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

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_new_file_with_zero_copy_storage_class) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, archive_to_new_file_with_no_route) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_new_file_with_incomplete_routing) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(adminApi.createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, archive_to_directory) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(adminApi.createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_NO_THROW(userApi.archive(requester, srcUrls, dstPath));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirContents(requester, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    std::set<std::string> archiveFileNames;
    DirIterator itor;
    ASSERT_NO_THROW(itor = userApi.getDirContents(requester,
      "/grandparent"));
    while(itor.hasMore()) {
      const DirEntry entry = itor.next();
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

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_directory_without_storage_class) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_directory_with_zero_copy_storage_class) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest, archive_to_directory_with_no_route) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

TEST_F(cta_client_SqliteMiddleTierUserTest,
  archive_to_directory_with_incomplete_routing) {
  using namespace cta;

  SqliteDatabase db;
  Vfs vfs;
  SqliteMiddleTierUser userApi(vfs, db);
  SqliteMiddleTierAdmin adminApi(vfs, db);
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(adminApi.createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(userApi.createDir(requester, dirPath));
  ASSERT_NO_THROW(userApi.setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(adminApi.createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(adminApi.createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(userApi.archive(requester, srcUrls, dstPath), std::exception);
}

} // namespace unitTests
