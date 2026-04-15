/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>

namespace unitTests {

const uint32_t DISK_FILE_OWNER_UID = 9751;
const uint32_t DISK_FILE_GID = 9752;
const std::string s_storageClassName = "TestStorageClass";

/**
 * This structure is used to parameterize RelationalDB database tests.
 */
struct RelationalDBTestParams {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit RelationalDBTestParams(cta::SchedulerDatabaseFactory* dbFactory) : dbFactory(*dbFactory) {}
};  // struct RelationalDBTestParams

/**
 * The RelationalDB database test is a parameterized test.  It takes an
 * RelationalDB database factory as a parameter.
 */
class RelationalDBTest : public ::testing::TestWithParam<RelationalDBTestParams> {
public:
  RelationalDBTest() noexcept {}

  class FailedToGetDatabase : public std::exception {
  public:
    const char* what() const noexcept { return "Failed to get scheduler database"; }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto& factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the PostgresSched DB from the factory.
    auto psdb = std::move(factory.create(m_catalogue));
    // Make sure the type of the SchedulerDatabase is correct (it should be an RelationalDBTestWrapper).
    dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::RelationalDBTestWrapper& getDb() {
    cta::RelationalDBTestWrapper* const ptr = m_db.get();
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
  RelationalDBTest(const RelationalDBTest&) = delete;

  // Prevent assignment
  RelationalDBTest& operator=(const RelationalDBTest&) = delete;

  std::unique_ptr<cta::RelationalDBTestWrapper> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};  // class RelationalDBTest

TEST_P(RelationalDBTest, DISABLED_getBatchArchiveJob) {
  ASSERT_EQ(0, 1);
}

TEST_P(RelationalDBTest, getArchiveJobs) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  // No archive jobs were queued, so result must be empty
  const auto jobs = db.getArchiveJobs();
  ASSERT_TRUE(jobs.empty());
}

TEST_P(RelationalDBTest, getPendingRetrieveJobs) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  // No retrieve jobs were queued, so result should be empty
  const auto jobs = db.getPendingRetrieveJobs();
  ASSERT_TRUE(jobs.empty());
}

TEST_P(RelationalDBTest, getArchiveJobs_withTapePoolFilter) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  auto queueArchiveJob =
    [&db, &lc](uint64_t fileId, const std::string& tapePool, const std::string& suffix, time_t creationTime) {
      cta::common::dataStructures::ArchiveRequest ar;
      cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

      // Put this archive request into the requested tape pool
      // This is the value later used by getArchiveJobs(tapePoolName) to filter the jobs
      afqc.copyToPoolMap.insert({1, tapePool});
      afqc.fileId = fileId;
      afqc.mountPolicy.name = "mountPolicy";
      afqc.mountPolicy.archivePriority = 1;
      afqc.mountPolicy.archiveMinRequestAge = 0;
      afqc.mountPolicy.retrievePriority = 1;
      afqc.mountPolicy.retrieveMinRequestAge = 0;
      afqc.mountPolicy.creationLog = {"u", "h", creationTime};
      afqc.mountPolicy.lastModificationLog = {"u", "h", creationTime};
      afqc.mountPolicy.comment = "comment";

      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      ar.checksumBlob.insert(cta::checksum::NONE, "");
      ar.creationLog = {"user", "host", creationTime};
      ar.diskFileID = "diskFile-" + suffix;
      ar.diskFileInfo.path = "/path/" + suffix;
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      ar.fileSize = 1000 + fileId;
      ar.requester = {"user", "group"};
      ar.srcURL = "root:/path/" + suffix;
      ar.storageClass = "storageClass";

      // Queue a real archive job through the database API
      db.queueArchive("eosInstance", ar, afqc, lc);
    };

  // Insert jobs into two different tape pools
  // Queue one job in tapePoolA and one in tapePoolB
  // We need one matching job and one non-matching job to be sure that filtering works
  queueArchiveJob(111, "tapePoolA", "A", 1000);
  queueArchiveJob(222, "tapePoolB", "B", 2000);

  db.waitSubthreadsComplete();

  // Filter by tapePoolA
  // This should return only the job in tapePoolA
  const auto jobs = db.getArchiveJobs(std::string("tapePoolA"));
  ASSERT_EQ(1u, jobs.size());

  // Verify returned job is really the tapePoolA and its fields are filled correctly
  const auto& job = jobs.front();
  ASSERT_EQ("tapePoolA", job.tapePool);
  ASSERT_EQ(111u, job.archiveFileID);
  ASSERT_EQ(1u, job.copyNumber);
  ASSERT_EQ(1000u, job.request.creationLog.time);
  ASSERT_EQ("test://archive-report-url", job.request.archiveReportURL);
  ASSERT_EQ("test://error-report-url", job.request.archiveErrorReportURL);
  ASSERT_EQ("diskFile-A", job.request.diskFileID);
  ASSERT_EQ("/path/A", job.request.diskFileInfo.path);
  ASSERT_EQ(DISK_FILE_OWNER_UID, job.request.diskFileInfo.owner_uid);
  ASSERT_EQ(DISK_FILE_GID, job.request.diskFileInfo.gid);
  ASSERT_EQ(1111u, job.request.fileSize);
  ASSERT_EQ("storageClass", job.request.storageClass);
  ASSERT_EQ("root:/path/A", job.request.srcURL);
  ASSERT_EQ("user", job.request.requester.name);
  ASSERT_EQ("group", job.request.requester.group);
}

TEST_P(RelationalDBTest, getPendingRetrieveJobs_withVidFilter) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  auto queueRetrieveJob =
    [&db, &lc](uint64_t archiveFileId, const std::string& vid, const std::string& suffix, time_t creationTime) {
      cta::common::dataStructures::RetrieveRequest rr;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;

      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = {"u", "h", creationTime};
      rfqc.mountPolicy.lastModificationLog = {"u", "h", creationTime};
      rfqc.mountPolicy.comment = "comment";

      // Fill the archive file metadata required by queueRetrieve()
      rfqc.archiveFile.archiveFileID = archiveFileId;
      rfqc.archiveFile.fileSize = 4000 + archiveFileId;
      rfqc.archiveFile.storageClass = s_storageClassName;
      rfqc.archiveFile.diskInstance = "diskInstance";
      rfqc.archiveFile.diskFileId = "diskFile-" + suffix;
      rfqc.archiveFile.diskFileInfo.path = "/retrieve/" + suffix;
      rfqc.archiveFile.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      rfqc.archiveFile.diskFileInfo.gid = DISK_FILE_GID;
      rfqc.archiveFile.checksumBlob.insert(cta::checksum::ADLER32, 0x12345678);

      // Put this retrieve request on the requested tape VID
      // This VID is what getPendingRetrieveJobs(vid) later uses to filter the jobs
      rfqc.archiveFile.tapeFiles.emplace_back();
      rfqc.archiveFile.tapeFiles.back().fSeq = archiveFileId;
      rfqc.archiveFile.tapeFiles.back().vid = vid;
      rfqc.archiveFile.tapeFiles.back().copyNb = 1;
      rfqc.archiveFile.tapeFiles.back().fileSize = 4000 + archiveFileId;
      rfqc.archiveFile.tapeFiles.back().blockId = archiveFileId;
      rfqc.archiveFile.tapeFiles.back().checksumBlob.insert(cta::checksum::ADLER32, 0x12345678);

      rr.archiveFileID = archiveFileId;
      rr.creationLog = {"user", "host", creationTime};
      rr.lifecycleTimings.creation_time = creationTime;
      rr.diskFileInfo.path = "/retrieve/" + suffix;
      rr.requester = {"user", "group"};
      rr.dstURL = "root://disk/" + suffix;
      rr.errorReportURL = "test://retrieve-error-report-url";
      rr.retrieveReportURL = "null:";

      std::string dsName = "ds-A";

      // Queue a real retrieve job through the database API
      db.queueRetrieve(rr, rfqc, dsName, lc);
    };

  // Insert jobs on two different VIDs
  // Queue one job on vidA and one on vidB
  // We need one job on the requested VID and one on another VID to be sure that filtering works
  queueRetrieveJob(333, "vidA", "A", 3000);
  queueRetrieveJob(444, "vidB", "B", 4000);

  db.waitSubthreadsComplete();

  // Filter by vidA
  // This should return only the retrieve job on vidA
  const auto jobs = db.getPendingRetrieveJobs(std::string("vidA"));
  ASSERT_EQ(1u, jobs.size());

  // Verify the returned job is the vidA job and its fields are filled correctly
  const auto& job = jobs.front();
  ASSERT_EQ(333u, job.request.archiveFileID);
  ASSERT_EQ(3000u, job.request.creationLog.time);
  ASSERT_EQ("root://disk/A?oss.asize=4333", job.request.dstURL);
  ASSERT_EQ("test://retrieve-error-report-url", job.request.errorReportURL);
  ASSERT_EQ("/retrieve/A", job.request.diskFileInfo.path);
  ASSERT_EQ(DISK_FILE_OWNER_UID, job.request.diskFileInfo.owner_uid);
  ASSERT_EQ(DISK_FILE_GID, job.request.diskFileInfo.gid);
  ASSERT_EQ(4333u, job.fileSize);
  ASSERT_EQ("diskInstance", job.diskInstance);
  ASSERT_EQ(s_storageClassName, job.storageClass);
  ASSERT_EQ("diskFile-A", job.diskFileId);
  ASSERT_EQ(1u, job.tapeCopies.size());
  ASSERT_TRUE(job.tapeCopies.find("vidA") != job.tapeCopies.end());
  ASSERT_EQ(1u, job.tapeCopies.at("vidA").first);
}

TEST_P(RelationalDBTest, getArchiveJobs_groupsJobsByTapePool) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  auto queueArchiveJob =
    [&db, &lc](uint64_t fileId, const std::string& tapePool, const std::string& suffix, time_t creationTime) {
      cta::common::dataStructures::ArchiveRequest ar;
      cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

      afqc.copyToPoolMap.insert({1, tapePool});
      afqc.fileId = fileId;
      afqc.mountPolicy.name = "mountPolicy";
      afqc.mountPolicy.archivePriority = 1;
      afqc.mountPolicy.archiveMinRequestAge = 0;
      afqc.mountPolicy.retrievePriority = 1;
      afqc.mountPolicy.retrieveMinRequestAge = 0;
      afqc.mountPolicy.creationLog = {"u", "h", creationTime};
      afqc.mountPolicy.lastModificationLog = {"u", "h", creationTime};
      afqc.mountPolicy.comment = "comment";

      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      ar.checksumBlob.insert(cta::checksum::NONE, "");
      ar.creationLog = {"user", "host", creationTime};
      ar.diskFileID = "diskFile-" + suffix;
      ar.diskFileInfo.path = "/path/" + suffix;
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      ar.fileSize = 1000 + fileId;
      ar.requester = {"user", "group"};
      ar.srcURL = "root:/path/" + suffix;
      ar.storageClass = "storageClass";

      db.queueArchive("eosInstance", ar, afqc, lc);
    };

  // Insert jobs into two different tape pools
  // Queue two jobs in tapePoolA and one job in tapePoolB
  // We need to be sure that grouping by tape pool works and that jobs are grouped under the correct tape pool
  queueArchiveJob(111, "tapePoolA", "A1", 1000);
  queueArchiveJob(112, "tapePoolA", "A2", 1001);
  queueArchiveJob(221, "tapePoolB", "B1", 2000);

  db.waitSubthreadsComplete();

  // Query all archive jobs
  // This should return jobs grouped by tape pool
  const auto jobs = db.getArchiveJobs();
  ASSERT_EQ(2u, jobs.size());
  ASSERT_EQ(2u, jobs.at("tapePoolA").size());
  ASSERT_EQ(1u, jobs.at("tapePoolB").size());
}

TEST_P(RelationalDBTest, getPendingRetrieveJobs_groupsJobsByVid) {
  using namespace cta;
#ifndef STDOUT_LOGGING
  cta::log::DummyLogger dl("", "");
#else
  cta::log::StdoutLogger dl("", "");
#endif
  cta::log::LogContext lc(dl);

  cta::SchedulerDatabase& db = getDb();

  auto queueRetrieveJob =
    [&db, &lc](uint64_t archiveFileId, const std::string& vid, const std::string& suffix, time_t creationTime) {
      cta::common::dataStructures::RetrieveRequest rr;
      cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;

      rfqc.mountPolicy.name = "mountPolicy";
      rfqc.mountPolicy.archivePriority = 1;
      rfqc.mountPolicy.archiveMinRequestAge = 0;
      rfqc.mountPolicy.retrievePriority = 1;
      rfqc.mountPolicy.retrieveMinRequestAge = 0;
      rfqc.mountPolicy.creationLog = {"u", "h", creationTime};
      rfqc.mountPolicy.lastModificationLog = {"u", "h", creationTime};
      rfqc.mountPolicy.comment = "comment";

      rfqc.archiveFile.archiveFileID = archiveFileId;
      rfqc.archiveFile.fileSize = 4000 + archiveFileId;
      rfqc.archiveFile.storageClass = s_storageClassName;
      rfqc.archiveFile.diskInstance = "diskInstance";
      rfqc.archiveFile.diskFileId = "diskFile-" + suffix;
      rfqc.archiveFile.diskFileInfo.path = "/retrieve/" + suffix;
      rfqc.archiveFile.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      rfqc.archiveFile.diskFileInfo.gid = DISK_FILE_GID;
      rfqc.archiveFile.checksumBlob.insert(cta::checksum::ADLER32, 0x12345678);

      rfqc.archiveFile.tapeFiles.emplace_back();
      rfqc.archiveFile.tapeFiles.back().fSeq = archiveFileId;
      rfqc.archiveFile.tapeFiles.back().vid = vid;
      rfqc.archiveFile.tapeFiles.back().copyNb = 1;
      rfqc.archiveFile.tapeFiles.back().fileSize = 4000 + archiveFileId;
      rfqc.archiveFile.tapeFiles.back().blockId = archiveFileId;
      rfqc.archiveFile.tapeFiles.back().checksumBlob.insert(cta::checksum::ADLER32, 0x12345678);

      rr.archiveFileID = archiveFileId;
      rr.creationLog = {"user", "host", creationTime};
      rr.lifecycleTimings.creation_time = creationTime;
      rr.diskFileInfo.path = "/retrieve/" + suffix;
      rr.requester = {"user", "group"};
      rr.dstURL = "root://disk/" + suffix;
      rr.errorReportURL = "test://retrieve-error-report-url";
      rr.retrieveReportURL = "null:";

      std::string dsName = "ds-A";
      db.queueRetrieve(rr, rfqc, dsName, lc);
    };

  // Insert jobs on two different VIDs
  // Queue two jobs on vidA and one job on vidB
  // We need two jobs on one VID and one on another VID
  queueRetrieveJob(333, "vidA", "A1", 3000);
  queueRetrieveJob(334, "vidA", "A2", 3001);
  queueRetrieveJob(444, "vidB", "B1", 4000);

  db.waitSubthreadsComplete();

  // Query all retrieve jobs
  // This should return jobs grouped by VID
  const auto jobs = db.getPendingRetrieveJobs();
  ASSERT_EQ(2u, jobs.size());
  ASSERT_EQ(2u, jobs.at("vidA").size());
  ASSERT_EQ(1u, jobs.at("vidB").size());
}

static cta::RelationalDBTestFactory RelationalDBTestFactoryStatic;
INSTANTIATE_TEST_CASE_P(RelationalDBTest,
                        RelationalDBTest,
                        ::testing::Values(RelationalDBTestParams(&RelationalDBTestFactoryStatic)));

}  // namespace unitTests
