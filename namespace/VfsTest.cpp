/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "namespace/Vfs.hpp"

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
  DirIterator itor;
  
  ASSERT_NO_THROW(itor = vfs.getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/"));
}

TEST_F(cta_VfsTest, mkdir_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1", 0777));
  ASSERT_THROW(vfs.createDir(requester, "/dir1", 0777), std::exception);
  ASSERT_NO_THROW(itor = vfs.getDirContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "dir1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_VfsTest, createFile_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(vfs.createFile(requester, "/file1", 0666));
  ASSERT_THROW(vfs.createFile(requester, "/file1", 0666), std::exception);
  ASSERT_NO_THROW(itor = vfs.getDirContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "file1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_VfsTest, rmdir_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1", 0777));
  ASSERT_NO_THROW(vfs.deleteDir(requester, "/dir1"));
  ASSERT_THROW(vfs.deleteDir(requester, "/dir1"), std::exception);
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir2", 0777));
  ASSERT_NO_THROW(vfs.createFile(requester, "/dir2/file1", 0666));
  ASSERT_THROW(vfs.deleteDir(requester, "/dir2"), std::exception);
  ASSERT_NO_THROW(vfs.deleteFile(requester, "/dir2/file1"));
  ASSERT_NO_THROW(vfs.deleteDir(requester, "/dir2"));
  ASSERT_NO_THROW(itor = vfs.getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/"));  
}

TEST_F(cta_VfsTest, storageClass_functionality) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.setDirStorageClass(requester, "/dir1", "cms"));
  ASSERT_EQ(std::string("cms"), vfs.getDirStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.clearDirStorageClass(requester, "/dir1"));
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.setDirStorageClass(requester, "/dir1", "atlas"));
  ASSERT_EQ(std::string("atlas"), vfs.getDirStorageClass(requester, "/dir1"));
}

TEST_F(cta_VfsTest, storageClass_inheritance) {
  using namespace cta;

  Vfs vfs;
  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/dir1"));  
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1/dir1_1", 0777));
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/dir1/dir1_1"));  
  ASSERT_NO_THROW(vfs.setDirStorageClass(requester, "/dir1", "cms"));
  ASSERT_EQ(std::string("cms"), vfs.getDirStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(vfs.createDir(requester, "/dir1/dir1_2", 0777));
  ASSERT_EQ(std::string("cms"), vfs.getDirStorageClass(requester, "/dir1/dir1_2"));
  ASSERT_EQ(std::string(""), vfs.getDirStorageClass(requester, "/dir1/dir1_1"));
}

}