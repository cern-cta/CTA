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

#include "nameserver/mockNS/MockNameServer.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_MockNameServerTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  static const std::string s_adminHost;
  static const std::string s_userHost;

  static const cta::UserIdentity s_admin;
  static const cta::UserIdentity s_user;

  static const cta::SecurityIdentity s_systemOnSystemHost;

  static const cta::SecurityIdentity s_adminOnAdminHost;
  static const cta::SecurityIdentity s_adminOnUserHost;

  static const cta::SecurityIdentity s_userOnAdminHost;
  static const cta::SecurityIdentity s_userOnUserHost;

}; // class cta_MockNameServerTest

const std::string cta_MockNameServerTest::s_adminHost = "adminhost";
const std::string cta_MockNameServerTest::s_userHost = "userhost";

const cta::UserIdentity cta_MockNameServerTest::s_admin(2222, 2222);
const cta::UserIdentity cta_MockNameServerTest::s_user(getuid(), getgid());

const cta::SecurityIdentity cta_MockNameServerTest::s_adminOnAdminHost(cta_MockNameServerTest::s_admin, cta_MockNameServerTest::s_adminHost);
const cta::SecurityIdentity cta_MockNameServerTest::s_adminOnUserHost(cta_MockNameServerTest::s_admin, cta_MockNameServerTest::s_userHost);

const cta::SecurityIdentity cta_MockNameServerTest::s_userOnAdminHost(cta_MockNameServerTest::s_user, cta_MockNameServerTest::s_adminHost);
const cta::SecurityIdentity cta_MockNameServerTest::s_userOnUserHost(cta_MockNameServerTest::s_user, cta_MockNameServerTest::s_userHost);

TEST_F(cta_MockNameServerTest, constructor_consistency) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;
  
  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(itor = ns->getDirContents(s_userOnUserHost, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(s_userOnUserHost, "/"));
}

TEST_F(cta_MockNameServerTest, mkdir_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;

  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1", 0777));
  ASSERT_THROW(ns->createDir(s_userOnUserHost, "/dir1", 0777), std::exception);
  ASSERT_NO_THROW(itor = ns->getDirContents(s_userOnUserHost, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().name, "dir1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_MockNameServerTest, createFile_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;

  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createFile(s_userOnUserHost, "/file1", 0666, 0));
  ASSERT_THROW(ns->createFile(s_userOnUserHost, "/file1", 0666, 0), std::exception);
  ASSERT_NO_THROW(itor = ns->getDirContents(s_userOnUserHost, "/"));
  ASSERT_EQ(itor.hasMore(), true);
  ASSERT_EQ(itor.next().name, "file1");
  ASSERT_EQ(itor.hasMore(), false);
}

TEST_F(cta_MockNameServerTest, rmdir_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;
  
  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1", 0777));
  ASSERT_NO_THROW(ns->deleteDir(s_userOnUserHost, "/dir1"));
  ASSERT_THROW(ns->deleteDir(s_userOnUserHost, "/dir1"), std::exception);
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir2", 0777));
  ASSERT_NO_THROW(ns->createFile(s_userOnUserHost, "/dir2/file1", 0666, 0));
  ASSERT_THROW(ns->deleteDir(s_userOnUserHost, "/dir2"), std::exception);
  ASSERT_NO_THROW(ns->deleteFile(s_userOnUserHost, "/dir2/file1"));
  ASSERT_NO_THROW(ns->deleteDir(s_userOnUserHost, "/dir2"));
  ASSERT_NO_THROW(itor = ns->getDirContents(s_userOnUserHost, "/"));
  ASSERT_FALSE(itor.hasMore());
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(s_userOnUserHost, "/"));  
}

TEST_F(cta_MockNameServerTest, storageClass_functionality) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;
  
  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1", 0777));
  ASSERT_EQ(std::string(""), ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
  ASSERT_NO_THROW(ns->setDirStorageClass(s_userOnUserHost, "/dir1", "cms"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  ASSERT_NO_THROW(ns->clearDirStorageClass(s_userOnUserHost, "/dir1"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
    ASSERT_EQ(std::string(""), storageClass);
  }
  ASSERT_NO_THROW(ns->setDirStorageClass(s_userOnUserHost, "/dir1", "atlas"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
    ASSERT_EQ(std::string("atlas"), storageClass);
  }
}

TEST_F(cta_MockNameServerTest, storageClass_inheritance) {
  using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  ArchiveDirIterator itor;
  
  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
    ASSERT_EQ(std::string(""), storageClass);  
  }
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1/dir1_1", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1/dir1_1"));
    ASSERT_EQ(std::string(""), storageClass);  
  }
  ASSERT_NO_THROW(ns->setDirStorageClass(s_userOnUserHost, "/dir1", "cms"));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  ASSERT_NO_THROW(ns->createDir(s_userOnUserHost, "/dir1/dir1_2", 0777));
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1/dir1_2"));
    ASSERT_EQ(std::string("cms"), storageClass);
  }
  {
    std::string storageClass;
    ASSERT_NO_THROW(storageClass = ns->getDirStorageClass(s_userOnUserHost, "/dir1/dir1_1"));
    ASSERT_EQ(std::string(""), storageClass);
  }
}

TEST_F(cta_MockNameServerTest, adding_and_getting_tapeFiles) {
    using namespace cta;

  std::unique_ptr<MockNameServer> ns;
  ASSERT_NO_THROW(ns.reset(new MockNameServer()));

  const std::string archiveFileName = "file1";
  const std::string archiveFilePath = std::string("/") + archiveFileName;
  const uint64_t archiveFileSize = 256 * 1024;
  const std::string vid = "V12345";
  const uint8_t copyNb = 1;
  const uint64_t fseq = 1;
  // block 0: VOL
  // block 1: HDR1
  // block 2: HDR2
  // block 3: UHL1
  // block 4: File mark
  // block 5: First block of user file
  const uint64_t blockId = 5;
  const uint32_t adlerChecksum = 0x11223344;

  // Create a file entry in the archive namespace
  ASSERT_NO_THROW(ns->setOwner(s_adminOnAdminHost, "/", s_user));
  ASSERT_NO_THROW(ns->createFile(s_userOnUserHost, "/file1", 0666,
    archiveFileSize));
  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = ns->getDirContents(s_userOnUserHost, "/"));
    ASSERT_EQ(itor.hasMore(), true);
    ASSERT_EQ(itor.next().name, archiveFileName);
    ASSERT_EQ(itor.hasMore(), false);
  }

  // Add a tape file entry to the file entry in the archive namespace
  NameServerTapeFile tapeFile;
  tapeFile.copyNb = copyNb;
  tapeFile.tapeFileLocation.fSeq = fseq;
  tapeFile.tapeFileLocation.blockId = blockId;
  tapeFile.tapeFileLocation.vid = vid;
  tapeFile.tapeFileLocation.copyNb = copyNb;
  tapeFile.size = archiveFileSize;
  tapeFile.compressedSize = archiveFileSize; // No compression
  cta::Checksum tapeFileChecksum(Checksum::CHECKSUMTYPE_ADLER32,
    cta::ByteArray(adlerChecksum));
  tapeFile.checksum = tapeFileChecksum;
  ASSERT_NO_THROW(ns->addTapeFile(
    s_userOnUserHost,
    archiveFilePath,
    tapeFile));

  // Get back the tape file entry
  std::list<NameServerTapeFile> tapeFiles;
  ASSERT_NO_THROW(tapeFiles = ns->getTapeFiles(s_userOnUserHost,
    archiveFilePath));
  ASSERT_EQ(1, tapeFiles.size());
  ASSERT_TRUE(tapeFile == tapeFiles.front());
}

} // namespace unitTests
