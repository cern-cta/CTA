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

#include "MiddleTierAbstractTest.hpp"
#include <gtest/gtest.h>
#include <set>
#include <memory>

namespace unitTests {

class cta_client_SqliteMiddleTierUserTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_P(MiddleTierAbstractTest,
  user_getDirContents_root_dir_is_empty) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  DirIterator itor;
  ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_P(MiddleTierAbstractTest, user_createDir_empty_string) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "";

  ASSERT_THROW(m_middleTier->user().createDir(requester, dirPath), std::exception);
}

TEST_P(MiddleTierAbstractTest,
  user_createDir_consecutive_slashes) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "//";

  ASSERT_THROW(m_middleTier->user().createDir(requester, dirPath), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_createDir_invalid_chars) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent/?parent";
  
  ASSERT_THROW(m_middleTier->user().createDir(requester, dirPath), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_createDir_top_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());
}

TEST_P(MiddleTierAbstractTest, user_createDir_second_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  ASSERT_TRUE(m_middleTier->user().getDirStorageClass(requester, "/").empty());

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, topLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_TRUE(m_middleTier->user().getDirStorageClass(requester, "/grandparent").empty());

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, secondLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_TRUE(m_middleTier->user().getDirStorageClass(requester,
    "/grandparent/parent").empty());
}

TEST_P(MiddleTierAbstractTest,
  user_createDir_inherit_storage_class) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  ASSERT_TRUE(m_middleTier->user().getDirStorageClass(requester, "/").empty());

  {
    const std::string name = "TestStorageClass";
    const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, name, nbCopies, comment));
  }

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, topLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());

    ASSERT_TRUE(m_middleTier->user().getDirStorageClass(requester, "/grandparent").empty());

    ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, "/grandparent",
      "TestStorageClass"));
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    m_middleTier->user().getDirStorageClass(requester, "/grandparent"));

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, secondLevelDirPath));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    m_middleTier->user().getDirStorageClass(requester, "/grandparent/parent"));
}

TEST_P(MiddleTierAbstractTest, user_deleteDir_root) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/";

  ASSERT_THROW(m_middleTier->user().deleteDir(requester, "/"), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_deleteDir_existing_top_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";
  
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_NO_THROW(m_middleTier->user().deleteDir(requester, "/grandparent"));

  {
    DirIterator itor;
  
    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));
  
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(MiddleTierAbstractTest,
  user_deleteDir_non_empty_top_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  {
    const std::string topLevelDirPath = "/grandparent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, topLevelDirPath));

    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";

    ASSERT_NO_THROW(m_middleTier->user().createDir(requester, secondLevelDirPath));

    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_THROW(m_middleTier->user().deleteDir(requester, "/grandparent"), std::exception);

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }
}

TEST_P(MiddleTierAbstractTest,
  user_deleteDir_non_existing_top_level) {
  using namespace cta;
  
  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  ASSERT_THROW(m_middleTier->user().deleteDir(requester, "/grandparent"), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_setDirStorageClass_top_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = m_middleTier->user().getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
  {
    ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
      nbCopies, comment));
  }

  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = m_middleTier->user().getDirStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_P(MiddleTierAbstractTest,
  user_clearDirStorageClass_top_level) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;
  const std::string dirPath = "/grandparent";

  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  DirIterator itor;

  ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = m_middleTier->user().getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = m_middleTier->user().getDirStorageClass(requester, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName),
    std::exception);

  ASSERT_NO_THROW(m_middleTier->user().clearDirStorageClass(requester, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = m_middleTier->user().getDirStorageClass(requester, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(m_middleTier->admin().deleteStorageClass(requester, storageClassName));
}

TEST_P(MiddleTierAbstractTest, user_archive_to_new_file) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_NO_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester,
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
    ASSERT_NO_THROW(entry = m_middleTier->user().stat(requester, dstPath));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const std::map<TapePool, std::list<ArchivalJob> > allJobs =
      m_middleTier->user().getArchivalJobs(requester);
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
      srcUrls.insert(jobItor->getRemoteFile());
      dstPaths.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl") == srcUrls.end());
    ASSERT_EQ(1, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/parent_file") == dstPaths.end());
  }

  {
    const std::list<ArchivalJob> poolJobs = m_middleTier->user().getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(1, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getRemoteFile());
      dstPaths.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, srcUrls.size());
    ASSERT_FALSE(srcUrls.find("diskUrl") == srcUrls.end());
    ASSERT_EQ(1, dstPaths.size());
    ASSERT_FALSE(dstPaths.find("/grandparent/parent_file") == dstPaths.end());
  }
}

TEST_P(MiddleTierAbstractTest,
  user_archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest,
  user_archive_to_new_file_with_zero_copy_storage_class) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_archive_to_new_file_with_no_route) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest,
  user_archive_to_new_file_with_incomplete_routing) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl");
  const std::string dstPath  = "/grandparent/parent_file";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_archive_to_directory) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester, "/"));
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
    ASSERT_NO_THROW(itor = m_middleTier->user().getDirContents(requester,
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
      m_middleTier->user().getArchivalJobs(requester);
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
      srcUrls.insert(jobItor->getRemoteFile());
      dstPaths.insert(jobItor->getArchiveFile());
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
    const std::list<ArchivalJob> poolJobs = m_middleTier->user().getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(4, poolJobs.size());
    std::set<std::string> srcUrls;
    std::set<std::string> dstPaths;
    for(std::list<ArchivalJob>::const_iterator jobItor = poolJobs.begin();
      jobItor != poolJobs.end(); jobItor++) {
      ASSERT_EQ(ArchivalJobState::PENDING, jobItor->getState());
      srcUrls.insert(jobItor->getRemoteFile());
      dstPaths.insert(jobItor->getArchiveFile());
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

TEST_P(MiddleTierAbstractTest,
  user_archive_to_directory_without_storage_class) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest,
  user_archive_to_directory_with_zero_copy_storage_class) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest, user_archive_to_directory_with_no_route) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

TEST_P(MiddleTierAbstractTest,
  user_archive_to_directory_with_incomplete_routing) {
  using namespace cta;

  std::unique_ptr<localMiddleTier> m_middleTier(GetParam()->allocateLocalMiddleTier());
  const SecurityIdentity requester;

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(m_middleTier->admin().createStorageClass(requester, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  ASSERT_NO_THROW(m_middleTier->user().createDir(requester, dirPath));
  ASSERT_NO_THROW(m_middleTier->user().setDirStorageClass(requester, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(m_middleTier->admin().createTapePool(requester, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(m_middleTier->admin().createArchivalRoute(requester, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> srcUrls;
  srcUrls.push_back("diskUrl1");
  srcUrls.push_back("diskUrl2");
  srcUrls.push_back("diskUrl3");
  srcUrls.push_back("diskUrl4");
  const std::string dstPath  = "/grandparent";
  ASSERT_THROW(m_middleTier->user().archive(requester, srcUrls, dstPath), std::exception);
}

} // namespace unitTests
