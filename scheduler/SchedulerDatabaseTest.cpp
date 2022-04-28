/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "objectstore/BackendRadosTestSwitch.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "OStoreDB/OStoreDBFactory.hpp"
#include "objectstore/BackendRados.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/range.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

#include <exception>
#include <gtest/gtest.h>
#include <algorithm>
#include <uuid/uuid.h>

namespace unitTests {

const uint32_t DISK_FILE_OWNER_UID = 9751;
const uint32_t DISK_FILE_GID = 9752;

/**
 * This structure is used to parameterize scheduler database tests.
 */
struct SchedulerDatabaseTestParam {
  cta::SchedulerDatabaseFactory &dbFactory;

  SchedulerDatabaseTestParam(cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {
 }
}; // struct SchedulerDatabaseTestParam

/**
 * The scheduler database test is a parameterized test.  It takes a
 * scheduler database factory as a parameter.
 */
class SchedulerDatabaseTest: public
  ::testing::TestWithParam<SchedulerDatabaseTestParam> {
public:

  SchedulerDatabaseTest() throw() {
  }

  class FailedToGetDatabase: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler database";
    }
  };

  virtual void SetUp() {
    using namespace cta;
    cta::log::DummyLogger logger("", "");
    const SchedulerDatabaseFactory &factory = GetParam().dbFactory;
    //m_catalogue = std::make_unique<cta::catalogue::InMemoryCatalogue>(logger, 1, 1);
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    
    m_db.reset(factory.create(m_catalogue).release());
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::SchedulerDatabase &getDb() {
    cta::SchedulerDatabase *const ptr = m_db.get();
    if(NULL == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  cta::catalogue::Catalogue &getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    return *ptr;

  }

  static const std::string s_systemHost;
  static const std::string s_adminHost;
  static const std::string s_userHost;

  static const std::string s_system;
  static const std::string s_admin;
  static const std::string s_user;

  static const cta::common::dataStructures::SecurityIdentity s_systemOnSystemHost;

  static const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_adminOnUserHost;

  static const cta::common::dataStructures::SecurityIdentity s_userOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_userOnUserHost;

private:

  // Prevent copying
  SchedulerDatabaseTest(const SchedulerDatabaseTest &);

  // Prevent assignment
  SchedulerDatabaseTest & operator= (const SchedulerDatabaseTest &);

  std::unique_ptr<cta::SchedulerDatabase> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

}; // class SchedulerDatabaseTest

const std::string SchedulerDatabaseTest::s_systemHost = "systemhost";
const std::string SchedulerDatabaseTest::s_adminHost = "adminhost";
const std::string SchedulerDatabaseTest::s_userHost = "userhost";

const std::string SchedulerDatabaseTest::s_system = "systemuser";
const std::string SchedulerDatabaseTest::s_admin = "adminuser";
const std::string SchedulerDatabaseTest::s_user = "user";

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_systemOnSystemHost(SchedulerDatabaseTest::s_system, SchedulerDatabaseTest::s_systemHost);

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_adminOnAdminHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_adminHost);
const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_adminOnUserHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_userHost);

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_userOnAdminHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_adminHost);
const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_userOnUserHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_userHost);

TEST_P(SchedulerDatabaseTest, createManyArchiveJobs) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();
  // Inject 100 archive jobs to the db.
  const size_t filesToDo = 100;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
#ifdef LOOPING_TEST
  do {
#endif
  for (size_t i=0; i<filesToDo; i++) {
    lambdas.emplace_back(
    [i,&db,&lc](){
      cta::common::dataStructures::ArchiveRequest ar;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;
      afqc.copyToPoolMap.insert({1, "tapePool"});
      afqc.fileId = 0;
      afqc.mountPolicy.name = "mountPolicy";
      afqc.mountPolicy.archivePriority = 1;
      afqc.mountPolicy.archiveMinRequestAge = 0;
      afqc.mountPolicy.retrievePriority = 1;
      afqc.mountPolicy.retrieveMinRequestAge = 0;
      afqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.comment = "comment";
      afqc.fileId = i;
      ar.archiveReportURL="";
      ar.checksumBlob.insert(cta::checksum::NONE, "");
      ar.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      ar.diskFileID = fileUUIDStr;
      ar.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      ar.fileSize = 1000;
      ar.requester = { "user", "group" };
      ar.srcURL = std::string("root:/") + ar.diskFileInfo.path;
      ar.storageClass = "storageClass";
      db.queueArchive("eosInstance", ar, afqc, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all archive jobs into memory
  // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  cta::catalogue::TapeForWriting tfw;
  tfw.tapePool = "tapePool";
  tfw.vid = "vid";
  auto am = moutInfo->createArchiveMount(common::dataStructures::MountType::ArchiveForUser, tfw, "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr));
  bool done = false;
  size_t count = 0;
  while (!done) {
    auto aj = am->getNextJobBatch(1,1,lc);
    if (aj.size()) {
      std::list <std::unique_ptr<cta::SchedulerDatabase::ArchiveJob> > jobBatch;
      jobBatch.emplace_back(std::move(aj.front()));
      aj.pop_front();
      count++;
      am->setJobBatchTransferred(jobBatch, lc);
    }
    else
      done = true;
  }
#ifdef LOOPING_TEST
  if (filesToDo != count) {
    std::cout << "ERROR_DETECTED!!! ********************************************* BLocking test" << std::endl;
    std::cout << "Missing=" << filesToDo - count << " count=" << count << " filesToDo=" << filesToDo << std::endl;
    while (true) { sleep(1);}
  } else {
    std::cout << "**************************************************************************************************** END OF RUN *******************************************************\n" << std::endl;
  }
#endif
  ASSERT_EQ(filesToDo, count);
  am->complete(time(nullptr));
  am.reset(nullptr);
  moutInfo.reset(nullptr);
#ifdef LOOPING_TEST
  } while (true);
#endif
  const size_t filesToDo2 = 200;
  for (size_t i=0; i<filesToDo2; i++) {
    lambdas.emplace_back(
    [i,&db,&lc](){
      cta::common::dataStructures::ArchiveRequest ar;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;
      afqc.copyToPoolMap.insert({1, "tapePool"});
      afqc.fileId = 0;
      afqc.mountPolicy.name = "mountPolicy";
      afqc.mountPolicy.archivePriority = 1;
      afqc.mountPolicy.archiveMinRequestAge = 0;
      afqc.mountPolicy.retrievePriority = 1;
      afqc.mountPolicy.retrieveMinRequestAge = 0;
      afqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.comment = "comment";
      afqc.fileId = i;
      ar.archiveReportURL="";
      ar.checksumBlob.insert(cta::checksum::NONE, "");
      ar.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      ar.diskFileID = fileUUIDStr;
      ar.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      ar.fileSize = 1000;
      ar.requester = { "user", "group" };
      ar.srcURL = std::string("root:/") + ar.diskFileInfo.path;
      ar.storageClass = "storageClass";
      db.queueArchive("eosInstance", ar, afqc, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();

  // Then load all archive jobs into memory (2nd pass)
  // Create mount.
#ifdef LOOPING_TEST
  auto moutInfo = db.getMountInfo();
  cta::catalogue::TapeForWriting tfw;
  tfw.tapePool = "tapePool";
  tfw.vid = "vid";
  auto am = moutInfo->createArchiveMount(tfw, "drive", "library", "host","vo","mediaType", "vendor",123456789, time(nullptr));
  auto done = false;
  auto count = 0;
#else
  moutInfo = db.getMountInfo(lc);
  am = moutInfo->createArchiveMount(common::dataStructures::MountType::ArchiveForUser, tfw, "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr));
  done = false;
  count = 0;
#endif
  while (!done) {
    auto aj = am->getNextJobBatch(1,1,lc);
    if (aj.size()) {
      std::list <std::unique_ptr <cta::SchedulerDatabase::ArchiveJob> > jobBatch;
      jobBatch.emplace_back(std::move(aj.front()));
      aj.pop_front();
      count++;
      am->setJobBatchTransferred(jobBatch, lc);
    }
    else
      done = true;
  }
  ASSERT_EQ(filesToDo2, count);
  am->complete(time(nullptr));
  am.reset(nullptr);
  moutInfo.reset(nullptr);
}

TEST_P(SchedulerDatabaseTest, putExistingQueueToSleep) {
    using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();

   // Create the disk system list
  cta::disk::DiskSystemList diskSystemList;
  cta::common::dataStructures::DiskInstanceSpace diskInstanceSpace{"dis-A", "di-A", "constantFreeSpace:999999999999", 60, 60, 0,
      "No Comment", common::dataStructures::EntryLog(), common::dataStructures::EntryLog{}};
  
  cta::disk::DiskSystem diskSystem{"ds-A", diskInstanceSpace, "$root://a.disk.system/", 10UL*1000*1000*1000,
      15*60, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{},"No comment"};
  diskSystemList.push_back(diskSystem);

  // Inject a retrieve job to the db.
  std::function<void()> lambda = [&db,&lc,diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = 0;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      rr.dstURL = std::string ("root://") + "a" + ".disk.system/" + std::to_string(0);
      std::string dsName = "ds-A";
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    };
  
  std::future<void> jobInsertion = std::async(std::launch::async,lambda);
  jobInsertion.get();
  db.waitSubthreadsComplete();

    // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  auto rm=moutInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);

  rm->putQueueToSleep(diskSystem.name, diskSystem.sleepTime, lc);

  rm.reset(nullptr);
  moutInfo.reset(nullptr);
  auto mi = db.getMountInfoNoLock(cta::SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
  ASSERT_EQ(1, mi->potentialMounts.size());
  ASSERT_TRUE(mi->potentialMounts.begin()->sleepingMount);
  ASSERT_EQ("ds-A", mi->potentialMounts.begin()->diskSystemSleptFor);
}

TEST_P(SchedulerDatabaseTest, createQueueAndPutToSleep) {
    using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();

   // Create the disk system list
  cta::disk::DiskSystemList diskSystemList;
  cta::common::dataStructures::DiskInstanceSpace diskInstanceSpace{"dis-A", "di-A", "constantFreeSpace:999999999999", 60, 60, 0,
      "No Comment", common::dataStructures::EntryLog(), common::dataStructures::EntryLog{}};
  
  cta::disk::DiskSystem diskSystem{"ds-A", diskInstanceSpace, "$root://a.disk.system/", 10UL*1000*1000*1000,
      15*60, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{},"No comment"};
  diskSystemList.push_back(diskSystem);

  // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  auto rm=moutInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);

  rm->putQueueToSleep(diskSystem.name, diskSystem.sleepTime, lc);

    // Inject a retrieve job to the db (so we can retrieve it)
  std::function<void()> lambda = [&db,&lc,diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = 0;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      rr.dstURL = std::string ("root://") + "a" + ".disk.system/" + std::to_string(0);
      std::string dsName = "ds-A";
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    };
  
  std::future<void> jobInsertion = std::async(std::launch::async,lambda);
  jobInsertion.get();
  db.waitSubthreadsComplete();

  rm.reset(nullptr);
  moutInfo.reset(nullptr);
  auto mi = db.getMountInfoNoLock(cta::SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
  ASSERT_EQ(1, mi->potentialMounts.size());
  ASSERT_TRUE(mi->potentialMounts.begin()->sleepingMount);
  ASSERT_EQ("ds-A", mi->potentialMounts.begin()->diskSystemSleptFor);
}


TEST_P(SchedulerDatabaseTest, popAndRequeueArchiveRequests) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();

  // Inject 10 archive jobs to the db.
  const size_t filesToDo = 10;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  auto creationTime =  time(nullptr);
  for (auto i: cta::range<size_t>(filesToDo)) {
    lambdas.emplace_back(
    [i,&db, creationTime, &lc](){
      cta::common::dataStructures::ArchiveRequest ar;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;
      afqc.copyToPoolMap.insert({1, "tapePool"});
      afqc.fileId = 0;
      afqc.mountPolicy.name = "mountPolicy";
      afqc.mountPolicy.archivePriority = 1;
      afqc.mountPolicy.archiveMinRequestAge = 0;
      afqc.mountPolicy.retrievePriority = 1;
      afqc.mountPolicy.retrieveMinRequestAge = 0;
      afqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      afqc.mountPolicy.comment = "comment";
      afqc.fileId = i;
      ar.archiveReportURL="";
      ar.checksumBlob.insert(cta::checksum::NONE, "");
      ar.creationLog = { "user", "host", creationTime};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      ar.diskFileID = fileUUIDStr;
      ar.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      ar.fileSize = 1000;
      ar.requester = { "user", "group" };
      ar.srcURL = std::string("root:/") + ar.diskFileInfo.path;
      ar.storageClass = "storageClass";
      db.queueArchive("eosInstance", ar, afqc, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all archive jobs into memory
  // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  cta::catalogue::TapeForWriting tfw;
  tfw.tapePool = "tapePool";
  tfw.vid = "vid";
  auto am = moutInfo->createArchiveMount(common::dataStructures::MountType::ArchiveForUser, tfw, "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr));
  auto ajb = am->getNextJobBatch(filesToDo, 1000 * filesToDo, lc);
  //Files with successful fetch should be popped
  ASSERT_EQ(10, ajb.size());
  //failing the jobs should requeue them
  for (auto &aj: ajb) {
    aj->failTransfer("test", lc);
  }
  //Jobs in queue should have been requeued with original creationlog time 
  auto pendingArchiveJobs = db.getArchiveJobs();
  ASSERT_EQ(pendingArchiveJobs["tapePool"].size(), filesToDo);
  for(auto &aj: pendingArchiveJobs["tapePool"]) {
    ASSERT_EQ(aj.request.creationLog.time, creationTime);
  }

  am->complete(time(nullptr));
  am.reset(nullptr);
  moutInfo.reset(nullptr);
}

TEST_P(SchedulerDatabaseTest, popAndRequeueRetrieveRequests) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();
  //cta::catalogue::Catalogue &catalogue = getCatalogue();

  
 // Create the disk system list
  cta::disk::DiskSystemList diskSystemList;
  cta::common::dataStructures::DiskInstanceSpace diskInstanceSpace{"dis-A", "di-A", "constantFreeSpace:999999999999", 60, 60, 0,
      "No Comment", common::dataStructures::EntryLog(), common::dataStructures::EntryLog{}};
  
  cta::disk::DiskSystem diskSystem{"ds-A", diskInstanceSpace, "$root://a.disk.system/", 10UL*1000*1000*1000,
      15*60, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{},"No comment"};
  diskSystemList.push_back(diskSystem);

  // Inject 10 retrieve jobs to the db.
  const size_t filesToDo = 10;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  auto creationTime =  time(nullptr);
  for (auto i: cta::range<size_t>(filesToDo)) {
    lambdas.emplace_back(
    [i,&db,&lc, creationTime, diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", creationTime};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", creationTime};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = i;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", creationTime};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      std::string dsName = "ds-A";
      rr.dstURL = std::string ("root://") + "a" + ".disk.system/" + std::to_string(0);
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all retrieve jobs into memory
  // Create mount.
  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1, mountInfo->potentialMounts.size());
  auto rm=mountInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);
  {
    auto rjb = rm->getNextJobBatch(10,20*1000,lc);
    //Files with successful fetch should be popped
    ASSERT_EQ(10, rjb.size());
    //jobs retain their creation time after being popped
    for (auto &rj: rjb) {
      ASSERT_EQ(creationTime, rj->retrieveRequest.lifecycleTimings.creation_time);
    }
    //requeing and repopping the jobs does not change the creation time
    rm->requeueJobBatch(rjb, lc);
    rjb = rm->getNextJobBatch(10,20*1000,lc);
    //Files with successful fetch should be popped
    ASSERT_EQ(10, rjb.size());
    //jobs retain their creation time after being popped
    for (auto &rj: rjb) {
      ASSERT_EQ(creationTime, rj->retrieveRequest.lifecycleTimings.creation_time);
    }
    
  }
  rm->complete(time(nullptr));
  rm.reset(nullptr);
  mountInfo.reset(nullptr);
}


TEST_P(SchedulerDatabaseTest, popRetrieveRequestsWithDisksytem) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();
  cta::catalogue::Catalogue &catalogue = getCatalogue();

  catalogue.createDiskInstance(common::dataStructures::SecurityIdentity(), "di", "No comment");
  catalogue.createDiskInstanceSpace(common::dataStructures::SecurityIdentity(), "dis-A", "di", "constantFreeSpace:999999999999", 60, "No comment");
  catalogue.createDiskSystem(common::dataStructures::SecurityIdentity(), "ds-A", "di", "dis-A", "$root://a.disk.system/", 10UL*1000*1000*1000, 15*60, "No comment");

  catalogue.createDiskInstanceSpace(common::dataStructures::SecurityIdentity(), "dis-B", "di", "constantFreeSpace:999999999999", 60, "No comment");
  catalogue.createDiskSystem(common::dataStructures::SecurityIdentity(), "ds-B", "di", "dis-B", "$root://b.disk.system/", 10UL*1000*1000*1000, 15*60,"No comment");

  auto diskSystemList = catalogue.getAllDiskSystems();

  // Inject 10 retrieve jobs to the db.
  const size_t filesToDo = 10;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  for (auto i: cta::range<size_t>(filesToDo)) {
    lambdas.emplace_back(
    [i,&db,&lc,diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = i;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      rr.dstURL = std::string ("root://") + (i%2?"b":"a") + ".disk.system/" + std::to_string(i);
      std::string dsName = (i%2?"ds-B":"ds-A");
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all archive jobs into memory
  // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  ASSERT_EQ(1, moutInfo->potentialMounts.size());
  auto rm=moutInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);
  auto rjb = rm->getNextJobBatch(20,20*1000, lc);
  ASSERT_EQ(filesToDo, rjb.size());
  
  std::list <cta::SchedulerDatabase::RetrieveJob*> jobBatch;
  cta::DiskSpaceReservationRequest reservationRequest;
  for (auto &rj: rjb) {
    rj->asyncSetSuccessful();
    ASSERT_TRUE((bool)rj->diskSystemName);
    ASSERT_EQ(rj->archiveFile.tapeFiles.front().fSeq%2?"ds-B":"ds-A", rj->diskSystemName.value());
    jobBatch.emplace_back(rj.get());
    if (rj->diskSystemName) {
      reservationRequest.addRequest(rj->diskSystemName.value(), rj->archiveFile.fileSize);
    } 
  }
  rm->reserveDiskSpace(reservationRequest, "", lc);
  rm->flushAsyncSuccessReports(jobBatch, lc);
  rjb.clear();
  ASSERT_EQ(0, rm->getNextJobBatch(20,20*1000, lc).size());
  rm->complete(time(nullptr));
  rm.reset(nullptr);
  moutInfo.reset(nullptr);
}

TEST_P(SchedulerDatabaseTest, popRetrieveRequestsWithBackpressure) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();

  // Files are 5 * 1000 byte per disk system. Make sure the batch does not fit.
  // We should then be able to get the queue slept.
  cta::catalogue::Catalogue &catalogue = getCatalogue();

  // Create the disk system list
  // only one disk system per queue, like in the production

  catalogue.createDiskInstance(common::dataStructures::SecurityIdentity(), "di", "No comment");
  catalogue.createDiskInstanceSpace(common::dataStructures::SecurityIdentity(), "dis-A", "di", "constantFreeSpace:6000", 60, "No comment");
  catalogue.createDiskSystem(common::dataStructures::SecurityIdentity(), "ds-A", "di", "dis-A", "$root://a.disk.system/", 0UL, 15*60, "No comment");

  auto diskSystemList = catalogue.getAllDiskSystems();

  // Inject 10 retrieve jobs to the db.
  const size_t filesToDo = 10;
  std::atomic<size_t> aFiles(0);
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  for (auto i: cta::range<size_t>(filesToDo)) {
    lambdas.emplace_back(
    [i,&db,&lc, diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = i;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      std::string dsName;
      rr.dstURL = std::string("root://a.disk.system/") + std::to_string(i);
      dsName = "ds-A";
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all retrieve jobs into memory
  // Create mount.
  auto moutInfo = db.getMountInfo(lc);
  ASSERT_EQ(1, moutInfo->potentialMounts.size());
  auto rm=moutInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);
  {
    //Batch fails and puts the queue to sleep (not enough space in disk system)
    //leave one job in the queue for the potential mount
    auto rjb = rm->getNextJobBatch(9,20*1000,lc);
    ASSERT_EQ(9, rjb.size());
    cta::DiskSpaceReservationRequest reservationRequest;
    for (auto &rj: rjb) {
      rj->asyncSetSuccessful();
      ASSERT_TRUE((bool)rj->diskSystemName);
      ASSERT_EQ("ds-A", rj->diskSystemName.value());
      if (rj->diskSystemName) {
        reservationRequest.addRequest(rj->diskSystemName.value(), rj->archiveFile.fileSize);
      } 
    }
    //reserving disk space will fail (not enough disk space, backpressure is triggered)
    ASSERT_FALSE(rm->reserveDiskSpace(reservationRequest, "", lc));
  }
  rm->complete(time(nullptr));
  rm.reset(nullptr);
  moutInfo.reset(nullptr);
  auto mi = db.getMountInfoNoLock(cta::SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
  ASSERT_EQ(1, mi->potentialMounts.size()); //all jobs were requeued
  //did not requeue the job batch (the retrive mount normally does this, but cannot do it in the tests due to BackendVFS)
  ASSERT_EQ(1, mi->potentialMounts.begin()->filesQueued); 
  ASSERT_TRUE(mi->potentialMounts.begin()->sleepingMount);
  ASSERT_EQ("ds-A", mi->potentialMounts.begin()->diskSystemSleptFor);
}


TEST_P(SchedulerDatabaseTest, popRetrieveRequestsWithDiskSystemNotFetcheable) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase &db = getDb();
  cta::catalogue::Catalogue &catalogue = getCatalogue();

  catalogue.createDiskInstance(common::dataStructures::SecurityIdentity(), "di", "No comment");
  catalogue.createDiskInstanceSpace(common::dataStructures::SecurityIdentity(), "dis-error", "di", "constantFreeSpace-6000", 60, "No comment");

  catalogue.createDiskSystem(common::dataStructures::SecurityIdentity(), "ds-Error", "di", "dis-error", "$root://error.disk.system/", 0UL,
    15*60,"No comment");

  auto diskSystemList = catalogue.getAllDiskSystems();
  // Inject 10 retrieve jobs to the db.
  const size_t filesToDo = 10;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  for (auto i: cta::range<size_t>(filesToDo)) {
    lambdas.emplace_back(
    [i,&db,&lc, diskSystemList](){
      cta::common::dataStructures::RetrieveRequest rr;
      cta::log::LogContext locallc=lc;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;
      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
      rfqc.mountPolicy.comment = "comment";
      rfqc.archiveFile.fileSize = 1000;
      rfqc.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
      rfqc.archiveFile.tapeFiles.back().fSeq = i;
      rfqc.archiveFile.tapeFiles.back().vid = "vid";
      rr.creationLog = { "user", "host", time(nullptr)};
      uuid_t fileUUID;
      uuid_generate(fileUUID);
      char fileUUIDStr[37];
      uuid_unparse(fileUUID, fileUUIDStr);
      rr.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
      rr.requester = { "user", "group" };
      std::string dsName;
      rr.dstURL = std::string("root://error.disk.system/") + std::to_string(i);
      dsName = "ds-Error";
      db.queueRetrieve(rr, rfqc, dsName, locallc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async,lambdas.back()));
  }
  for (auto &j: jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();
  db.waitSubthreadsComplete();

  // Then load all retrieve jobs into memory
  // Create mount.
  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1, mountInfo->potentialMounts.size());
  auto rm=mountInfo->createRetrieveMount("vid", "tapePool", "drive", "library", "host", "vo","mediaType", "vendor",123456789,time(nullptr), cta::nullopt);
  {
    //leave one job in the queue for the potential mount
    auto rjb = rm->getNextJobBatch(9,20*1000,lc);
    //Files with successful fetch should be popped
    ASSERT_EQ(9, rjb.size());

    cta::DiskSpaceReservationRequest reservationRequest;
    for (auto &rj: rjb) {
      ASSERT_TRUE((bool)rj->diskSystemName);
      ASSERT_EQ("ds-Error", rj->diskSystemName.value());
      reservationRequest.addRequest(rj->diskSystemName.value(), rj->archiveFile.fileSize);
    }
    //reserving disk space will fail because the disk instance is not reachable, causing backpressure
    ASSERT_FALSE(rm->reserveDiskSpace(reservationRequest, "", lc)); 
  }
  rm->complete(time(nullptr));
  rm.reset(nullptr);
  mountInfo.reset(nullptr);
  auto mi = db.getMountInfoNoLock(cta::SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
  ASSERT_EQ(1, mi->potentialMounts.size());
  //did not requeue the job batch (the retrive mount normally does this, but cannot do it in the tests due to BackendVFS)
  ASSERT_EQ(1, mi->potentialMounts.begin()->filesQueued); 
  ASSERT_TRUE(mi->potentialMounts.begin()->sleepingMount);
  ASSERT_EQ("ds-Error", mi->potentialMounts.begin()->diskSystemSleptFor);
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerDatabaseTest, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreSchedulerDatabaseTestVFS, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreSchedulerDatabaseTestRados, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(OStoreDBFactoryRados)));
#endif

} // namespace unitTests
