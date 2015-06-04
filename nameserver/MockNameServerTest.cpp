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

#include "nameserver/MockNameServer.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_MockNameServerTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_MockNameServerTest, constructor_consistency) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(itor = ns->getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(requester, "/"));
}

TEST_F(cta_MockNameServerTest, mkdir_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1", 0777));
  ASSERT_THROW(ns->createDir(requester, "/dir1", 0777), std::exception);
  ASSERT_NO_THROW(itor = ns->getDirContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "dir1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_MockNameServerTest, createFile_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(ns->createFile(requester, "/file1", 0666));
  ASSERT_THROW(ns->createFile(requester, "/file1", 0666), std::exception);
  ASSERT_NO_THROW(itor = ns->getDirContents(requester, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().getName(), "file1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_MockNameServerTest, rmdir_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1", 0777));
  ASSERT_NO_THROW(ns->deleteDir(requester, "/dir1"));
  ASSERT_THROW(ns->deleteDir(requester, "/dir1"), std::exception);
  ASSERT_NO_THROW(ns->createDir(requester, "/dir2", 0777));
  ASSERT_NO_THROW(ns->createFile(requester, "/dir2/file1", 0666));
  ASSERT_THROW(ns->deleteDir(requester, "/dir2"), std::exception);
  ASSERT_NO_THROW(ns->deleteFile(requester, "/dir2/file1"));
  ASSERT_NO_THROW(ns->deleteDir(requester, "/dir2"));
  ASSERT_NO_THROW(itor = ns->getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(requester, "/"));  
}

TEST_F(cta_MockNameServerTest, storageClass_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1", 0777));
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(requester, "/dir1"));
  ASSERT_NO_THROW(ns->setDirStorageClass(requester, "/dir1", "cms"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  ASSERT_NO_THROW(ns->clearDirStorageClass(requester, "/dir1"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1"));
    ASSERT_EQ(std::string(""), storageClass);
  }
  ASSERT_NO_THROW(ns->setDirStorageClass(requester, "/dir1", "atlas"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1"));
    ASSERT_EQ(std::string("atlas"), storageClass);
  }
}

TEST_F(cta_MockNameServerTest, storageClass_inheritance) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  cta::SecurityIdentity requester;
  DirIterator itor;
  
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1"));
    ASSERT_EQ(std::string(""), storageClass);  
  }
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1/dir1_1", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1/dir1_1"));
    ASSERT_EQ(std::string(""), storageClass);  
  }
  ASSERT_NO_THROW(ns->setDirStorageClass(requester, "/dir1", "cms"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  ASSERT_NO_THROW(ns->createDir(requester, "/dir1/dir1_2", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1/dir1_2"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(requester, "/dir1/dir1_1"));
    ASSERT_EQ(std::string(""), storageClass);
  }
}

} // namespace unitTests
