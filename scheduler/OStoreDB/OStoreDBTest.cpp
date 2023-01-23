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

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendRadosTestSwitch.hpp"
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/MemQueues.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/OStoreDB/OStoreDBTest.hpp"

namespace unitTests {

/**
 * This structure is used to parameterize OStore database tests.
 */
struct OStoreDBTestParams {
  cta::SchedulerDatabaseFactory &dbFactory;

  explicit OStoreDBTestParams(cta::SchedulerDatabaseFactory *dbFactory) :
    dbFactory(*dbFactory) {}
};  // struct OStoreDBTestParams


/**
 * The OStore database test is a parameterized test.  It takes an
 * OStore database factory as a parameter.
 */
class OStoreDBTest: public
  ::testing::TestWithParam<OStoreDBTestParams> {
 public:
  OStoreDBTest() throw() {
  }

  class FailedToGetDatabase: public std::exception {
   public:
    const char *what() const throw() {
      return "Failed to get scheduler database";
    }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto &factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the OStore DB from the factory.
    auto osdb = std::move(factory.create(m_catalogue));
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface).
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::objectstore::OStoreDBWrapperInterface &getDb() {
    cta::objectstore::OStoreDBWrapperInterface *const ptr = m_db.get();
    if (nullptr == ptr) {
      throw FailedToGetDatabase();
    }
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
  OStoreDBTest(const OStoreDBTest &) = delete;

  // Prevent assignment
  OStoreDBTest & operator= (const OStoreDBTest &) = delete;

  std::unique_ptr<cta::objectstore::OStoreDBWrapperInterface> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};  // class SchedulerDatabaseTest

TEST_P(OStoreDBTest, getBatchArchiveJob) {
  cta::log::StringLogger logger("dummy", "OStoreAbstractTest", cta::log::DEBUG);
  cta::log::LogContext lc(logger);
  // Get the OStoreBDinterface
  cta::objectstore::OStoreDBWrapperInterface & osdbi = getDb();
  // Add jobs to an archive queue.
  for (size_t i = 0; i < 10; i++) {
    cta::common::dataStructures::ArchiveRequest ar;
    cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;
    ar.fileSize = 123 * (i + 1);
    afqc.copyToPoolMap[1] = "Tapepool1";
    afqc.fileId = i;
    afqc.mountPolicy.name = "policy";
    afqc.mountPolicy.archivePriority = 1;
    afqc.mountPolicy.retrieveMinRequestAge = 1;
    afqc.mountPolicy.retrievePriority = 1;
    afqc.mountPolicy.retrieveMinRequestAge = 1;
    osdbi.queueArchive("testInstance", ar, afqc, lc);
    osdbi.waitSubthreadsComplete();
  }
  // Delete the first job from the queue, change owner of second.
  // They will be automatically skipped when getting jobs
  std::string aqAddr;
  {
    // Get hold of the queue
    cta::objectstore::RootEntry re(osdbi.getBackend());
    cta::objectstore::ScopedSharedLock rel(re);
    re.fetch();
    aqAddr = re.getArchiveQueueAddress("Tapepool1", cta::common::dataStructures::JobQueueType::JobsToTransferForUser);
    rel.release();
    cta::objectstore::ArchiveQueue aq(aqAddr, osdbi.getBackend());
    cta::objectstore::ScopedSharedLock aql(aq);
    aq.fetch();
    auto jd = aq.dumpJobs();
    auto j = jd.begin();
    // Delete the first job
    osdbi.getBackend().remove(j->address);
    // Change ownership of second
    j++;
    cta::objectstore::ArchiveRequest ar(j->address, osdbi.getBackend());
    cta::objectstore::ScopedExclusiveLock arl(ar);
    ar.fetch();
    ar.setJobOwner(1, "NoRealOwner");
    ar.commit();
  }
  // We can now fetch all jobs.
  auto mountInfo = osdbi.getMountInfo(lc);
  cta::catalogue::TapeForWriting tape;
  tape.capacityInBytes = 1;
  tape.dataOnTapeInBytes = 0;
  tape.lastFSeq = 1;
  tape.tapePool = "Tapepool1";
  tape.vid = "tape";
  auto mount = mountInfo->createArchiveMount(cta::common::dataStructures::MountType::ArchiveForUser,
    tape, "drive", "library", "host", "vo", "mediaType", "vendor", 123456789, std::nullopt, cta::common::dataStructures::Label::Format::CTA);
  auto giveAll = std::numeric_limits<uint64_t>::max();
  auto jobs = mount->getNextJobBatch(giveAll, giveAll, lc);
  ASSERT_EQ(8, jobs.size());
  // With the first 2 jobs removed from queue, we get the 3 and next. (i=2...)
  size_t i = 2;
  for (auto & j : jobs) {
    ASSERT_EQ(123*(i++ + 1), j->archiveFile.fileSize);
  }
  // Check the queue has been emptied, and hence removed.
  ASSERT_EQ(false, osdbi.getBackend().exists(aqAddr));
}

TEST_P(OStoreDBTest, MemQueuesSharedAddToArchiveQueue) {
  using cta::objectstore::ArchiveQueue;
  using cta::objectstore::ArchiveRequest;
  cta::log::StringLogger logger("dummy", "OStoreAbstractTest", cta::log::DEBUG);
  cta::log::LogContext lc(logger);
  // Get the OStoreBDinterface
  cta::objectstore::OStoreDBWrapperInterface & osdbi = getDb();
  // Create many archive jobs and enqueue them in the same archive queue.
  const size_t filesToDo = 100;
  std::list<std::future<void>> jobInsertions;
  std::list<std::function<void()>> lambdas;
  for (size_t i = 0; i < filesToDo; i++) {
    lambdas.emplace_back(
    [i, &osdbi, &lc](){
      cta::log::LogContext localLc = lc;
      // We need to pass an archive request and an archive request job dump to sharedAddToArchiveQueue.
      ArchiveRequest aReq(osdbi.getAgentReference().nextId("ArchiveRequest"), osdbi.getBackend());
      aReq.initialize();
      cta::common::dataStructures::ArchiveFile aFile;
      cta::common::dataStructures::MountPolicy mountPolicy;
      cta::common::dataStructures::RequesterIdentity requester;
      cta::common::dataStructures::EntryLog entryLog;
      aFile.archiveFileID = i;
      aReq.setArchiveFile(aFile);
      aReq.setMountPolicy(mountPolicy);
      aReq.setArchiveReportURL("");
      aReq.setArchiveErrorReportURL("");
      aReq.setRequester(requester);
      aReq.setSrcURL("");
      aReq.setEntryLog(entryLog);
      aReq.addJob(1, "tapepool", "archive queue address to be set later", 2, 2, 2);
      aReq.insert();
      cta::objectstore::ScopedExclusiveLock aql(aReq);
      ArchiveRequest::JobDump jd;
      jd.tapePool = "tapepool";
      jd.copyNb = 1;
      auto sharedLock = cta::ostoredb::MemQueue<ArchiveRequest, ArchiveQueue>::sharedAddToQueue(jd, jd.tapePool, aReq,
          osdbi.getOstoreDB(), localLc);
    });
    jobInsertions.emplace_back(std::async(std::launch::async, lambdas.back()));
  }
  for (auto &j : jobInsertions) { j.get(); }
  jobInsertions.clear();
  lambdas.clear();

  // Now make sure the files made it to the queue.
  ASSERT_EQ(filesToDo, osdbi.getArchiveJobs("tapepool").size());
}

static cta::objectstore::BackendVFS osVFS(__LINE__, __FILE__);
#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");
INSTANTIATE_TEST_CASE_P(OStoreTestRados, OStoreDBTest,
    ::testing::Values(OStoreDBTestParams(&OStoreDBFactoryRados)));
#endif
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;
INSTANTIATE_TEST_CASE_P(OStoreTestVFS, OStoreDBTest,
    ::testing::Values(OStoreDBTestParams(&OStoreDBFactoryVFS)));

}  // namespace unitTests
