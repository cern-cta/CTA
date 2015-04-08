#include "cta/Vfs.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_VfsTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_VfsTest, constructor_consistency) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(itor = vfs.getDirectoryContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/"));
}

TEST_F(cta_VfsTest, mkdir_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1", 0777));
  ASSERT_THROW(vfs.createDirectory(requester, "/dir1", 0777), std::exception);
  ASSERT_NO_THROW(itor = vfs.getDirectoryContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "dir1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_VfsTest, createFile_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(vfs.createFile(requester, "/file1", 0666));
  ASSERT_THROW(vfs.createFile(requester, "/file1", 0666), std::exception);
  ASSERT_NO_THROW(itor = vfs.getDirectoryContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "file1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_VfsTest, rmdir_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1", 0777));
  ASSERT_NO_THROW(vfs.deleteDirectory(requester, "/dir1"));
  ASSERT_THROW(vfs.deleteDirectory(requester, "/dir1"), std::exception);
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir2", 0777));
  ASSERT_NO_THROW(vfs.createFile(requester, "/dir2/file1", 0666));
  ASSERT_THROW(vfs.deleteDirectory(requester, "/dir2"), std::exception);
  ASSERT_NO_THROW(vfs.deleteFile(requester, "/dir2/file1"));
  ASSERT_NO_THROW(vfs.deleteDirectory(requester, "/dir2"));
  ASSERT_NO_THROW(itor = vfs.getDirectoryContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/"));  
}

TEST_F(cta_VfsTest, storageClass_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.setDirectoryStorageClass(requester, "/dir1", "cms"));
  ASSERT_EQ(std::string("cms"), vfs.getDirectoryStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.clearDirectoryStorageClass(requester, "/dir1"));
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.setDirectoryStorageClass(requester, "/dir1", "atlas"));
  ASSERT_EQ(std::string("atlas"), vfs.getDirectoryStorageClass(requester, "/dir1"));
}

TEST_F(cta_VfsTest, storageClass_inheritance) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirectoryIterator itor;
  
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/dir1"));  
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1/dir1_1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/dir1/dir1_1"));  
  ASSERT_NO_THROW(vfs.setDirectoryStorageClass(requester, "/dir1", "cms"));
  ASSERT_EQ(std::string("cms"), vfs.getDirectoryStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.createDirectory(requester, "/dir1/dir1_2", 0777));
  ASSERT_EQ(std::string("cms"), vfs.getDirectoryStorageClass(requester, "/dir1/dir1_2"));
  ASSERT_EQ(std::string(""), vfs.getDirectoryStorageClass(requester, "/dir1/dir1_1"));
}

}