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

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/DbConn.hpp"
#include "catalogue/RdbmsCatalogue.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/ArchiveRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MountRequest.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/TapeMount.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/RetrieveMount.hpp"

#include <exception>
#include <gtest/gtest.h>
#include <memory>
#include <utility>

namespace unitTests {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct SchedulerTestParam {
  cta::SchedulerDatabaseFactory &dbFactory;

  SchedulerTestParam(
    cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {
 }
}; // struct SchedulerTestParam

/**
 * The scheduler test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class SchedulerTest: public ::testing::TestWithParam<SchedulerTestParam> {
public:

  SchedulerTest() throw() {
  }

  class FailedToGetCatalogue: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    const SchedulerTestParam &param = GetParam();
    m_db = param.dbFactory.create();
    catalogue::DbLogin catalogueLogin(catalogue::DbLogin::DBTYPE_IN_MEMORY, "", "", "");
    m_catalogue.reset(catalogue::CatalogueFactory::create(catalogueLogin));

    m_scheduler.reset(new cta::Scheduler(*m_catalogue, *m_db, 5, 2*1000*1000));
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
  }

  cta::catalogue::Catalogue &getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    if(NULL == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }
    
  cta::Scheduler &getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if(NULL == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }

private:

  // Prevent copying
  SchedulerTest(const SchedulerTest &);

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &);

  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;

}; // class SchedulerTest

TEST_P(SchedulerTest, archive_to_new_file) {
  using namespace cta;

  catalogue::Catalogue &catalogue = getCatalogue();
  Scheduler &scheduler = getScheduler();
  
  cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost;
  s_adminOnAdminHost.username="admin1";
  s_adminOnAdminHost.host="host1";

  const std::string mountPolicyName = "mount_group";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string mountPolicyComment = "create mount group";

  ASSERT_TRUE(catalogue.getMountPolicies().empty());

  catalogue.createMountPolicy(
    s_adminOnAdminHost,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    mountPolicyComment);

  const std::list<common::dataStructures::MountPolicy> groups =
    catalogue.getMountPolicies();
  ASSERT_EQ(1, groups.size());
  const common::dataStructures::MountPolicy group = groups.front();
  ASSERT_EQ(mountPolicyName, group.name);
  ASSERT_EQ(archivePriority, group.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
  ASSERT_EQ(retrievePriority, group.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
  ASSERT_EQ(maxDrivesAllowed, group.maxDrivesAllowed);
  ASSERT_EQ(mountPolicyComment, group.comment);

  const std::string userComment = "create user";
  const std::string userName = "user_name";
  const std::string userGroup = "group";
  cta::common::dataStructures::UserIdentity userIdentity;
  userIdentity.name=userName;
  userIdentity.group=userGroup;
  catalogue.createRequester(s_adminOnAdminHost, userIdentity, mountPolicyName, userComment);

  std::list<common::dataStructures::Requester> users;
  users = catalogue.getRequesters();
  ASSERT_EQ(1, users.size());

  const common::dataStructures::Requester user = users.front();

  ASSERT_EQ(userName, user.name);
  ASSERT_EQ(mountPolicyName, user.mountPolicy);
  ASSERT_EQ(userComment, user.comment);
  ASSERT_EQ(s_adminOnAdminHost.username, user.creationLog.username);
  ASSERT_EQ(s_adminOnAdminHost.host, user.creationLog.host);
  ASSERT_EQ(user.creationLog, user.lastModificationLog);

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(catalogue.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  const bool tapePoolEncryption = false;
  ASSERT_NO_THROW(catalogue.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolEncryption, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(catalogue.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));
  
  cta::common::dataStructures::EntryLog creationLog;
  creationLog.host="host2";
  creationLog.time=0;
  creationLog.username="admin1";
  cta::common::dataStructures::DRData drData;
  drData.drBlob="blob";
  drData.drGroup="group2";
  drData.drOwner="cms_user";
  drData.drPath="path/to/file";
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumType="Adler32";
  request.checksumValue="1111";
  request.creationLog=creationLog;
  request.diskpoolName="diskpool1";
  request.diskpoolThroughput=200*1000*1000;
  request.drData=drData;
  request.diskFileID="diskFileID";
  request.instance="cms";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::UserIdentity requester;
  requester.name = userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=storageClassName;

  scheduler.queueArchive(s_adminOnAdminHost, request);

  {
    auto rqsts = scheduler.getPendingArchiveJobs();
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const std::string pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->request.drData.drPath);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(request.drData.drPath) == remoteFiles.end());
  }
}

/*
TEST_P(SchedulerTest, archive_to_new_user_file_as_admin) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_adminOnAdminHost, remoteFiles, archiveFile), std::exception);

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_FALSE(itor.hasMore());
  }

  {
    ASSERT_NO_THROW(ASSERT_TRUE(scheduler.getArchiveRequests(s_adminOnAdminHost).empty()));
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_TRUE(poolRqsts.empty());
  }
}

TEST_P(SchedulerTest, archive_twice_to_same_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<common::archiveNS::ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<common::archiveNS::ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }
}

TEST_P(SchedulerTest, delete_archive_request) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<common::archiveNS::ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  ASSERT_NO_THROW(scheduler.deleteArchiveRequest(s_userOnUserHost,
    "/grandparent/parent_file"));

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_TRUE(rqsts.empty());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_TRUE(poolRqsts.empty());
  }

  {
    std::unique_ptr<common::archiveNS::ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      "/grandparent/parent_file"));
    ASSERT_FALSE(status.get());
  }
}

TEST_P(SchedulerTest, archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, create_zero_copy_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment), std::exception);
}

TEST_P(SchedulerTest, archive_to_new_file_with_no_route) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest,
  archive_to_new_file_with_incomplete_routing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::set<std::string> archiveFileNames;
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    while(itor.hasMore()) {
      const common::archiveNS::ArchiveDirEntry entry = itor.next();
      archiveFileNames.insert(entry.name);
    }
    ASSERT_EQ(4, archiveFileNames.size());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename1) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename2) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename3) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename4) != archiveFileNames.end());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    const auto poolRqsts = poolItor->second;
    ASSERT_EQ(4, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath2) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath3) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath4) == remoteFiles.end());
    ASSERT_EQ(4, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile4") == remoteFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(4, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath2) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath3) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath4) == remoteFiles.end());
    ASSERT_EQ(4, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile4") == remoteFiles.end());
  }
}

TEST_P(SchedulerTest,
  archive_to_directory_without_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles,
    archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory_with_no_route) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles,
    archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory_with_incomplete_routing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_and_retrieve_new_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));
  MountCriteriaByDirection mcbd(MountCriteria(1,1,0,1), MountCriteria(1,1,0,1));
  ASSERT_NO_THROW(scheduler.setTapePoolMountCriteria("TestTapePool", mcbd));

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(scheduler.createLogicalLibrary(s_adminOnAdminHost, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.name);
    ASSERT_EQ(libraryComment, logicalLibrary.creationLog.comment);
  }

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";  
  ASSERT_NO_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName,
    tapePoolName, capacityInBytes, tapeComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    common::archiveNS::ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(common::archiveNS::ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<common::archiveNS::ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_adminOnAdminHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  // The file has not yet been archived, there is no 'm' bit in the archive
  // namespace and therefore an attempted retrive request should fail at this point
  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/grandparent/parent_file");
    ASSERT_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost, archiveFiles,
      s_remoteTargetRawPath1), std::exception);
  }

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed
    // the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    ASSERT_NO_THROW(mount.reset(scheduler.getNextMount(libraryName, "drive0").release()));
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::ARCHIVE, mount.get()->getMountType());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    ASSERT_NO_THROW(archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release())));
    ASSERT_NE((cta::ArchiveMount*)NULL, archiveMount.get());
    std::unique_ptr<cta::ArchiveJob> archiveJob;
    ASSERT_NO_THROW(archiveJob.reset(archiveMount->getNextJob().release()));
    ASSERT_NE((cta::ArchiveJob*)NULL, archiveJob.get());
    archiveJob->nameServerTapeFile.tapeFileLocation.blockId = 1;
    archiveJob->nameServerTapeFile.copyNb = archiveJob->nameServerTapeFile.tapeFileLocation.copyNb;
    cta::Checksum checksum(cta::Checksum::CHECKSUMTYPE_ADLER32, ntohl(0x12345687));
    archiveJob->nameServerTapeFile.checksum = checksum;
    ASSERT_NO_THROW(archiveJob->complete());
    ASSERT_NO_THROW(archiveJob.reset(archiveMount->getNextJob().release()));
    ASSERT_EQ((cta::ArchiveJob*)NULL, archiveJob.get());
    ASSERT_NO_THROW(archiveMount->complete());
  }

  {
    
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/grandparent/parent_file");
    ASSERT_NO_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost,
      archiveFiles, s_remoteTargetRawPath1));
  }

  {
    decltype(scheduler.getRetrieveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getRetrieveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto tapeItor = rqsts.cbegin();
    ASSERT_FALSE(tapeItor == rqsts.cend());
    const Tape &tape = tapeItor->first;
    ASSERT_TRUE(vid == tape.vid);
    auto tapeRqsts = tapeItor->second;
    ASSERT_EQ(1, tapeRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = tapeRqsts.cbegin(); rqstItor != tapeRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile);
      archiveFiles.insert(rqstItor->archiveFile.path);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteTargetRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }
  
  {
    // Emulate a tape server by asking for a mount and then a file (and succeed
    // the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    ASSERT_NO_THROW(mount.reset(scheduler.getNextMount(libraryName, "drive0").release()));
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::RETRIEVE, mount.get()->getMountType());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    ASSERT_NO_THROW(retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release())));
    ASSERT_NE((cta::RetrieveMount*)NULL, retrieveMount.get());
    std::unique_ptr<cta::RetrieveJob> retrieveJob;
    ASSERT_NO_THROW(retrieveJob.reset(retrieveMount->getNextJob().release()));
    ASSERT_NE((cta::RetrieveJob*)NULL, retrieveJob.get());
    ASSERT_NO_THROW(retrieveJob->complete());
    ASSERT_NO_THROW(retrieveJob.reset(retrieveMount->getNextJob().release()));
    ASSERT_EQ((cta::RetrieveJob*)NULL, retrieveJob.get());
  }
}

TEST_P(SchedulerTest, retry_archive_until_max_reached) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));
  MountCriteriaByDirection mcbd(MountCriteria(1,1,0,1), MountCriteria(1,1,0,1));
  ASSERT_NO_THROW(scheduler.setTapePoolMountCriteria("TestTapePool", mcbd));

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(scheduler.createLogicalLibrary(s_adminOnAdminHost, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.name);
    ASSERT_EQ(libraryComment, logicalLibrary.creationLog.comment);
  }

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";  
  ASSERT_NO_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName,
    tapePoolName, capacityInBytes, tapeComment));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));
  
  {
    // Emulate a tape server by asking for a mount and then a file
    std::unique_ptr<cta::TapeMount> mount;
    ASSERT_NO_THROW(mount.reset(scheduler.getNextMount(libraryName, "drive0").release()));
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::ARCHIVE, mount.get()->getMountType());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    ASSERT_NO_THROW(archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release())));
    ASSERT_NE((cta::ArchiveMount*)NULL, archiveMount.get());
    // The file should be retried 10 times
    for (int i=0; i<5; i++) {
      std::unique_ptr<cta::ArchiveJob> archiveJob;
      ASSERT_NO_THROW(archiveJob.reset(archiveMount->getNextJob().release()));
      ASSERT_NE((cta::ArchiveJob*)NULL, archiveJob.get());
      ASSERT_NO_THROW(archiveJob->failed(cta::exception::Exception("Archive failed")));
    }
    // Then the request should be gone
    std::unique_ptr<cta::ArchiveJob> archiveJob;
    ASSERT_NO_THROW(archiveJob.reset(archiveMount->getNextJob().release()));
    ASSERT_EQ((cta::ArchiveJob*)NULL, archiveJob.get());
  }
}

TEST_P(SchedulerTest, retrieve_non_existing_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  {
    common::archiveNS::ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_FALSE(itor.hasMore());
  }

  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/non_existing_file");
    ASSERT_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost,
      archiveFiles, "mock:non_existing_file"), std::exception);
  }
}
*/
#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;
INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryVFS)));
#endif

#undef TEST_RADOS
#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryRados)));
#endif
} // namespace unitTests

