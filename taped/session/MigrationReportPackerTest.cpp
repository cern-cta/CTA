/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MigrationReportPacker.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"
#include "taped/drive/DriveInterface.hpp"

#include <catalogue/Catalogue.hpp>
#include <gtest/gtest.h>

using ::testing::_;
using ::testing::Invoke;
using namespace cta::tape;

namespace unitTests {

const uint32_t TEST_USER_1 = 9751;
const uint32_t TEST_GROUP_1 = 9752;
const uint32_t TEST_USER_2 = 9753;
const uint32_t TEST_GROUP_2 = 9754;

class cta_tape_daemon_MigrationReportPackerTest : public ::testing::Test {
public:
  cta_tape_daemon_MigrationReportPackerTest() : m_dummyLog("dummy", "dummy") {}

protected:
  void SetUp() {
    using namespace cta;
    using namespace cta::catalogue;

    rdbms::Login catalogueLogin(rdbms::Login::DBTYPE_IN_MEMORY, "", "", "", "", 0, "");
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 0;
    auto catalogueFactory =
      CatalogueFactoryFactory::create(m_dummyLog, catalogueLogin, nbConns, nbArchiveFileListingConns);
    m_catalogue = catalogueFactory->create();
  }

  void createMediaType(const std::string& name) {
    cta::common::dataStructures::SecurityIdentity admin =
      cta::common::dataStructures::SecurityIdentity("admin", "localhost");
    cta::catalogue::MediaType mediaType;
    mediaType.name = name;
    mediaType.capacityInBytes = 10;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    m_catalogue->MediaType()->createMediaType(admin, mediaType);
  }

  const cta::common::dataStructures::DiskInstance getDefaultDiskInstance() const {
    cta::common::dataStructures::DiskInstance di;
    di.name = "di";
    di.comment = "comment";
    return di;
  }

  cta::common::dataStructures::VirtualOrganization getDefaultVo() {
    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = "vo";
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.comment = "comment";
    vo.diskInstanceName = getDefaultDiskInstance().name;
    vo.isRepackVo = false;
    return vo;
  }

  void TearDown() { m_catalogue.reset(); }

  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

};  // class cta_tape_daemon_MigrationReportPackerTest

class MockArchiveJobExternalStats : public cta::MockArchiveJob {
public:
  MockArchiveJobExternalStats(cta::ArchiveMount& am,
                              cta::catalogue::Catalogue& catalogue,
                              int& completes,
                              int& failures)
      : MockArchiveJob(&am, catalogue),
        completesRef(completes),
        failuresRef(failures) {}

  virtual void validate() override {}

  virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten() override {
    auto fileReportUP = std::make_unique<cta::catalogue::TapeFileWritten>();
    auto& fileReport = *fileReportUP;
    fileReport.archiveFileId = archiveFile.archiveFileID;
    fileReport.blockId = tapeFile.blockId;
    fileReport.checksumBlob = tapeFile.checksumBlob;
    fileReport.copyNb = tapeFile.copyNb;
    fileReport.diskFileId = archiveFile.diskFileId;
    fileReport.diskFileOwnerUid = archiveFile.diskFileInfo.owner_uid;
    fileReport.diskFileGid = archiveFile.diskFileInfo.gid;
    fileReport.diskInstance = archiveFile.diskInstance;
    fileReport.fSeq = tapeFile.fSeq;
    fileReport.size = archiveFile.fileSize;
    fileReport.storageClassName = archiveFile.storageClass;
    fileReport.tapeDrive = std::string("testDrive");
    fileReport.vid = tapeFile.vid;
    return cta::catalogue::TapeItemWrittenPointer(fileReportUP.release());
  }

  void transferFailed(const std::string& failureReason, cta::log::LogContext& lc) override { failuresRef++; }

  void reportJobSucceeded() override { completesRef++; }

private:
  int& completesRef;
  int& failuresRef;
};

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerNominal) {
  cta::MockArchiveMount tam(*m_catalogue);

  const std::string vid1 = "VTEST001";
  const std::string vid2 = "VTEST002";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName = "tape_pool_name";
  const std::string encryptionKeyName = "encryption_key_name";
  const std::vector<std::string> supply_list;
  const bool fullValue = false;
  const std::string createTapeComment = "Create tape";
  cta::common::dataStructures::VirtualOrganization vo = getDefaultVo();
  const auto di = getDefaultDiskInstance();
  cta::common::dataStructures::SecurityIdentity admin =
    cta::common::dataStructures::SecurityIdentity("admin", "localhost");
  m_catalogue->DiskInstance()->createDiskInstance(admin, di.name, di.comment);
  m_catalogue->VO()->createVirtualOrganization(admin, vo);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(admin,
                                                      logicalLibraryName,
                                                      logicalLibraryIsDisabled,
                                                      physicalLibraryName,
                                                      "Create logical library");
  m_catalogue->TapePool()
    ->createTapePool(admin, tapePoolName, vo.name, 2, encryptionKeyName, supply_list, "Create tape pool");
  createMediaType(mediaType);

  {
    cta::catalogue::CreateTapeAttributes tape;
    tape.vid = vid1;
    tape.mediaType = mediaType;
    tape.vendor = vendor;
    tape.logicalLibraryName = logicalLibraryName;
    tape.tapePoolName = tapePoolName;
    tape.full = fullValue;
    tape.comment = createTapeComment;
    tape.state = cta::common::dataStructures::Tape::DISABLED;
    tape.stateReason = "Test";
    m_catalogue->Tape()->createTape(admin, tape);
  }

  cta::common::dataStructures::StorageClass storageClass;

  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.vo.name = vo.name;
  storageClass.comment = "Create storage class";
  m_catalogue->StorageClass()->createStorageClass(admin, storageClass);

  ::testing::InSequence dummy;
  std::unique_ptr<cta::ArchiveJob> job1;
  int job1completes(0), job1failures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, job1completes, job1failures));
    job1.reset(mockJob.release());
  }
  job1->archiveFile.archiveFileID = 1;
  job1->archiveFile.diskInstance = di.name;
  job1->archiveFile.diskFileId = "diskFileId1";
  job1->archiveFile.diskFileInfo.path = "filePath1";
  job1->archiveFile.diskFileInfo.owner_uid = TEST_USER_1;
  job1->archiveFile.diskFileInfo.gid = TEST_GROUP_1;
  job1->archiveFile.fileSize = 1024;
  job1->archiveFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
  job1->archiveFile.storageClass = "storage_class";
  job1->tapeFile.vid = "VTEST001";
  job1->tapeFile.fSeq = 1;
  job1->tapeFile.blockId = 256;
  job1->tapeFile.fileSize = 768;
  job1->tapeFile.copyNb = 1;
  job1->tapeFile.checksumBlob.insert(cta::checksum::MD5,
                                     cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

  std::unique_ptr<cta::ArchiveJob> job2;
  int job2completes(0), job2failures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, job2completes, job2failures));
    job2.reset(mockJob.release());
  }
  job2->archiveFile.archiveFileID = 2;
  job2->archiveFile.diskInstance = di.name;
  job2->archiveFile.diskFileId = "diskFileId2";
  job2->archiveFile.diskFileInfo.path = "filePath2";
  job2->archiveFile.diskFileInfo.owner_uid = TEST_USER_2;
  job2->archiveFile.diskFileInfo.gid = TEST_GROUP_2;
  job2->archiveFile.fileSize = 1024;
  job2->archiveFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
  job2->archiveFile.storageClass = "storage_class";
  job2->tapeFile.vid = "VTEST001";
  job2->tapeFile.fSeq = 2;
  job2->tapeFile.blockId = 512;
  job2->tapeFile.fileSize = 768;
  job2->tapeFile.copyNb = 1;
  job2->tapeFile.checksumBlob.insert(cta::checksum::MD5,
                                     cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

  cta::log::StringLogger log("dummy", "cta_tape_daemon_MigrationReportPackerNominal", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  mrp.reportCompletedJob(std::move(job1), lc);
  mrp.reportCompletedJob(std::move(job2), lc);

  const drive::compressionStats statsCompress;
  mrp.reportFlush(statsCompress, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
  ASSERT_EQ(1, tam.completes);
  ASSERT_EQ(1, job1completes);
  ASSERT_EQ(1, job2completes);
}

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerFailure) {
  cta::MockArchiveMount tam(*m_catalogue);

  ::testing::InSequence dummy;
  std::unique_ptr<cta::ArchiveJob> job1;
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(new cta::MockArchiveJob(&tam, *m_catalogue));
    job1.reset(mockJob.release());
  }
  std::unique_ptr<cta::ArchiveJob> job2;
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(new cta::MockArchiveJob(&tam, *m_catalogue));
    job2.reset(mockJob.release());
  }
  std::unique_ptr<cta::ArchiveJob> job3;
  int job3completes(0), job3failures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, job3completes, job3failures));
    job3.reset(mockJob.release());
  }

  cta::log::StringLogger log("dummy", "cta_tape_daemon_MigrationReportPackerFailure", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  mrp.reportCompletedJob(std::move(job1), lc);
  mrp.reportCompletedJob(std::move(job2), lc);

  const std::string error_msg = "ERROR_TEST_MSG";
  const cta::exception::Exception ex(error_msg);
  mrp.reportFailedJob(std::move(job3), ex, lc);

  const drive::compressionStats statsCompress;
  mrp.reportFlush(statsCompress, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
  ASSERT_EQ(1, tam.completes);
#ifndef CTA_PGSCHED
  // In pgsched mode, transferFailed() is never called — the packer
  // accumulates jobs and delegates to failTransferBatch() instead.
  ASSERT_EQ(1, job3failures);
#endif
}

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerBadFile) {
  cta::MockArchiveMount tam(*m_catalogue);

  const std::string vid1 = "VTEST001";
  const std::string vid2 = "VTEST002";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName = "tape_pool_name";
  const uint64_t nbPartialTapes = 2;
  const std::string encryptionKeyName = "encryption_key_name";
  const std::vector<std::string> supply_list;
  const bool fullValue = false;
  const std::string createTapeComment = "Create tape";
  cta::common::dataStructures::SecurityIdentity admin =
    cta::common::dataStructures::SecurityIdentity("admin", "localhost");

  cta::common::dataStructures::VirtualOrganization vo = getDefaultVo();
  const auto& di = getDefaultDiskInstance();
  m_catalogue->DiskInstance()->createDiskInstance(admin, di.name, di.comment);
  m_catalogue->VO()->createVirtualOrganization(admin, vo);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(admin,
                                                      logicalLibraryName,
                                                      logicalLibraryIsDisabled,
                                                      physicalLibraryName,
                                                      "Create logical library");
  m_catalogue->TapePool()
    ->createTapePool(admin, tapePoolName, vo.name, nbPartialTapes, encryptionKeyName, supply_list, "Create tape pool");
  createMediaType(mediaType);

  {
    cta::catalogue::CreateTapeAttributes tape;
    tape.vid = vid1;
    tape.mediaType = mediaType;
    tape.vendor = vendor;
    tape.logicalLibraryName = logicalLibraryName;
    tape.tapePoolName = tapePoolName;
    tape.full = fullValue;
    tape.comment = createTapeComment;
    tape.state = cta::common::dataStructures::Tape::DISABLED;
    tape.stateReason = "test";
    m_catalogue->Tape()->createTape(admin, tape);
  }

  cta::common::dataStructures::StorageClass storageClass;

  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.vo.name = vo.name;
  storageClass.comment = "Create storage class";
  m_catalogue->StorageClass()->createStorageClass(admin, storageClass);

  ::testing::InSequence dummy;
  std::unique_ptr<cta::ArchiveJob> migratedBigFile;
  int migratedBigFileCompletes(0), migratedBigFileFailures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, migratedBigFileCompletes, migratedBigFileFailures));
    migratedBigFile.reset(mockJob.release());
  }
  std::unique_ptr<cta::ArchiveJob> migratedFileSmall;
  int migratedFileSmallCompletes(0), migratedFileSmallFailures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, migratedFileSmallCompletes, migratedFileSmallFailures));
    migratedFileSmall.reset(mockJob.release());
  }
  std::unique_ptr<cta::ArchiveJob> migratedNullFile;
  int migratedNullFileCompletes(0), migratedNullFileFailures(0);
  {
    std::unique_ptr<cta::MockArchiveJob> mockJob(
      new MockArchiveJobExternalStats(tam, *m_catalogue, migratedNullFileCompletes, migratedNullFileFailures));
    migratedNullFile.reset(mockJob.release());
  }

  migratedBigFile->archiveFile.archiveFileID = 4;
  migratedBigFile->archiveFile.diskInstance = di.name;
  migratedBigFile->archiveFile.diskFileId = "diskFileId2";
  migratedBigFile->archiveFile.diskFileInfo.path = "filePath2";
  migratedBigFile->archiveFile.diskFileInfo.owner_uid = TEST_USER_2;
  migratedBigFile->archiveFile.diskFileInfo.gid = TEST_GROUP_2;
  migratedBigFile->archiveFile.fileSize = 100000;
  migratedBigFile->archiveFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
  migratedBigFile->archiveFile.storageClass = "storage_class";
  migratedBigFile->tapeFile.vid = "VTEST001";
  migratedBigFile->tapeFile.fSeq = 1;
  migratedBigFile->tapeFile.blockId = 256;
  migratedBigFile->tapeFile.fileSize = 768;
  migratedBigFile->tapeFile.copyNb = 1;
  migratedBigFile->tapeFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

  migratedFileSmall->archiveFile.archiveFileID = 5;
  migratedFileSmall->archiveFile.diskInstance = di.name;
  migratedFileSmall->archiveFile.diskFileId = "diskFileId3";
  migratedFileSmall->archiveFile.diskFileInfo.path = "filePath3";
  migratedFileSmall->archiveFile.diskFileInfo.owner_uid = TEST_USER_2;
  migratedFileSmall->archiveFile.diskFileInfo.gid = TEST_GROUP_2;
  migratedFileSmall->archiveFile.fileSize = 1;
  migratedFileSmall->archiveFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
  migratedFileSmall->archiveFile.storageClass = "storage_class";
  migratedFileSmall->tapeFile.vid = "VTEST001";
  migratedFileSmall->tapeFile.fSeq = 2;
  migratedFileSmall->tapeFile.blockId = 512;
  migratedFileSmall->tapeFile.fileSize = 1;
  migratedFileSmall->tapeFile.copyNb = 1;
  migratedFileSmall->tapeFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

  migratedNullFile->archiveFile.archiveFileID = 6;
  migratedNullFile->archiveFile.diskInstance = di.name;
  migratedNullFile->archiveFile.diskFileId = "diskFileId4";
  migratedNullFile->archiveFile.diskFileInfo.path = "filePath4";
  migratedNullFile->archiveFile.diskFileInfo.owner_uid = TEST_USER_2;
  migratedNullFile->archiveFile.diskFileInfo.gid = TEST_GROUP_2;
  migratedNullFile->archiveFile.fileSize = 0;
  migratedNullFile->archiveFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
  migratedNullFile->archiveFile.storageClass = "storage_class";
  migratedNullFile->tapeFile.vid = "VTEST001";
  migratedNullFile->tapeFile.fSeq = 3;
  migratedNullFile->tapeFile.blockId = 768;
  migratedNullFile->tapeFile.fileSize = 0;
  migratedNullFile->tapeFile.copyNb = 1;
  migratedNullFile->tapeFile.checksumBlob.insert(
    cta::checksum::MD5,
    cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

  cta::log::StringLogger log("dummy", "cta_tape_daemon_MigrationReportPackerOneByteFile", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  mrp.reportCompletedJob(std::move(migratedBigFile), lc);
  mrp.reportCompletedJob(std::move(migratedFileSmall), lc);
  mrp.reportCompletedJob(std::move(migratedNullFile), lc);
  drive::compressionStats stats;
  stats.toTape = (100000 + 1) / 3;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("TapeFileWrittenEvent is invalid"));
  ASSERT_NE(std::string::npos, temp.find("Received a CTA exception while reporting archive mount results"));
  ASSERT_EQ(0, tam.completes);
  ASSERT_EQ(0, migratedBigFileCompletes);
  ASSERT_EQ(0, migratedFileSmallCompletes);
  ASSERT_EQ(0, migratedNullFileCompletes);
}

#ifdef CTA_PGSCHED

class MockArchiveJobWithPrepare : public cta::MockArchiveJob {
public:
  int prepareCount = 0;

  MockArchiveJobWithPrepare(cta::ArchiveMount* am, cta::catalogue::Catalogue& catalogue)
      : cta::MockArchiveJob(am, catalogue) {}

  void prepareFailTransfer(const std::string&) override { prepareCount++; }
};

class MockArchiveMountWithBatch : public cta::MockArchiveMount {
public:
  int flushCount = 0;
  size_t lastBatchSize = 0;
  int successCount = 0;
  int failureCount = 0;
  int skippedFilesCount = 0;
  int totalPrepareCount = 0;  // sum of prepareCount across all jobs seen in failTransferBatch

  // When set, reportJobsBatchTransferred throws this exception type instead of succeeding.
  enum class ThrowMode { None, CatalogueUpdate, MoveToQueue };
  ThrowMode throwMode = ThrowMode::None;

  explicit MockArchiveMountWithBatch(cta::catalogue::Catalogue& catalogue)
      : cta::MockArchiveMount(catalogue) {}

  void reportJobsBatchTransferred(
      std::queue<std::unique_ptr<cta::ArchiveJob>>& successfulArchiveJobs,
      std::queue<cta::catalogue::TapeItemWritten>& skippedFiles,
      std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& failedToReportArchiveJobs,
      cta::log::LogContext& lc) override {
    while (!successfulArchiveJobs.empty()) {
      if (auto* mj = dynamic_cast<cta::MockArchiveJob*>(successfulArchiveJobs.front().get())) {
        mj->reportJobSucceeded();
      }
      successCount++;
      successfulArchiveJobs.pop();
    }
    while (!skippedFiles.empty()) {
      skippedFilesCount++;
      skippedFiles.pop();
    }
    if (throwMode == ThrowMode::CatalogueUpdate) {
      throw cta::ArchiveMount::FailedReportCatalogueUpdate("simulated catalogue update failure");
    }
    if (throwMode == ThrowMode::MoveToQueue) {
      throw cta::ArchiveMount::FailedReportMoveToQueue("simulated move-to-queue failure");
    }
  }

  void failTransferBatch(std::vector<std::unique_ptr<cta::ArchiveJob>>& jobs,
                         cta::log::LogContext&) override {
    flushCount++;
    lastBatchSize = jobs.size();
    failureCount += static_cast<int>(jobs.size());
    for (auto& j : jobs) {
      if (auto* mj = dynamic_cast<MockArchiveJobWithPrepare*>(j.get())) {
        totalPrepareCount += mj->prepareCount;
      }
    }
  }

};

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedAccumulateSmallBatch) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedAccumulateSmallBatch", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // 3 failed jobs — well below c_failedJobsBatchMax, so no mid-session flush
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    mrp.reportFailedJob(std::move(job), ex, lc);
  }

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  // reportFlush takes the error path (m_errorHappened=true); no pending successes so
  // reportJobsBatchTransferred is skipped, but flushFailedJobs is piggybacked → one
  // failTransferBatch call with all 3 jobs; end-of-session finds the accumulator empty
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(3, tam.failureCount);
  ASSERT_EQ(0, tam.successCount);
  ASSERT_EQ(1, tam.completes);
}

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedBatchSizeFlush) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedBatchSizeFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // Send 601 failed jobs: the 600th triggers an immediate mid-session flush,
  // leaving 1 job accumulated. 600 matches c_failedJobsBatchMax in MigrationReportPacker.
  for (int i = 0; i < 601; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    mrp.reportFailedJob(std::move(job), ex, lc);
  }

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);  // flushes remaining 1 job
  mrp.reportEndOfSession(lc);  // flushFailedJobs called again but empty → no-op
  mrp.waitThread();

  // Two flushes: mid-session at batch size 600 (batch-size threshold), then at reportFlush
  // which takes the error path and piggybacks flushFailedJobs for the remaining 1 job
  ASSERT_EQ(2, tam.flushCount);
  ASSERT_EQ(601, tam.failureCount);
  ASSERT_EQ(0, tam.successCount);
  ASSERT_EQ(1, tam.completes);
}

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedEndOfSessionFlush) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedEndOfSessionFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // 3 failed jobs, no reportFlush at all — failures must be flushed by reportEndOfSession.
  // This verifies the guarantee: even if the tape drive never produces a flush event
  // (e.g. the session aborts before any tape block is committed), failures are not lost.
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    mrp.reportFailedJob(std::move(job), ex, lc);
  }

  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  // One flush at end-of-session with all 3 accumulated jobs
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(3, tam.failureCount);
  ASSERT_EQ(0, tam.successCount);
  ASSERT_EQ(1, tam.completes);
}

TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedSuccessPiggybackOnFlush) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedSuccessPiggybackOnFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // Realistic scenario: 3 successful jobs and 3 failed jobs are all reported before the tape
  // block flush. In pgsched mode, individual file failures are soft — the session continues and
  // the successful jobs must still be committed to the catalogue alongside the failure batch.
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    mrp.reportCompletedJob(std::move(job), lc);
  }
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    const cta::exception::Exception ex("failure " + std::to_string(i));
    mrp.reportFailedJob(std::move(job), ex, lc);
  }

  // Single tape flush: m_errorHappened=true (set by the failures), so ReportFlush::execute takes
  // the error path. In pgsched mode, it still calls reportJobsBatchTransferred for the 3 pending
  // successes (new behaviour), then piggybacks flushFailedJobs for the 3 failures.
  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);

  mrp.reportEndOfSession(lc);  // both accumulators empty → no additional calls
  mrp.waitThread();

  // Single flush reported both 3 successes and 3 failures; end-of-session is clean
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(3, tam.failureCount);
  ASSERT_EQ(3, tam.successCount);
  ASSERT_EQ(1, tam.completes);
}

// Verify that reportSkippedJob in pgsched mode accumulates the job into
// m_failedArchiveJobs (via prepareFailTransfer + failTransferBatch) rather than
// calling transferFailed() immediately, and that m_skippedFiles is populated so
// the tape catalogue record is written at the next flush.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedSkippedJobAccumulated) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedSkippedJobAccumulated", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // 3 skipped jobs — below c_failedJobsBatchMax, so no mid-session flush.
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
    auto& raw = static_cast<MockArchiveJobWithPrepare&>(*job);
    raw.tapeFile.fSeq = static_cast<uint64_t>(i + 1);
    raw.tapeFile.vid = "VTEST001";
    const std::string failureMsg = "skipped " + std::to_string(i);
    mrp.reportSkippedJob(std::move(job), failureMsg, lc);
  }

  // A flush causes reportJobsBatchTransferred (for the skipped tape records) and
  // flushFailedJobs (for the accumulated pgsched failure batch).
  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  // prepareFailTransfer must have been called on each of the 3 skipped jobs exactly once.
  // The mock accumulates this in totalPrepareCount inside failTransferBatch before jobs are destroyed.
  ASSERT_EQ(3, tam.totalPrepareCount);
  // failTransferBatch must have been called exactly once (at reportFlush's piggyback flush)
  // with all 3 accumulated skipped jobs.
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(3, tam.failureCount);
  // The tape catalogue records (m_skippedFiles) must have been delivered to
  // reportJobsBatchTransferred; the mock counts them in skippedFilesCount.
  ASSERT_EQ(3, tam.skippedFilesCount);
  ASSERT_EQ(1, tam.completes);
}

// Verify that once m_failedArchiveJobs reaches c_failedJobsBatchMax the packer
// triggers an immediate mid-session failTransferBatch flush, keeping the
// accumulator bounded.  Uses skipped jobs as the source of failures.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedSkippedJobBatchMaxFlush) {
  MockArchiveMountWithBatch tam(*m_catalogue);

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedSkippedJobBatchMaxFlush", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // Send c_failedJobsBatchMax + 1 = 601 skipped jobs: the 600th triggers an
  // immediate mid-session flush (leaving 1 job in the accumulator).
  for (int i = 0; i < 601; i++) {
    auto* raw = new MockArchiveJobWithPrepare(&tam, *m_catalogue);
    raw->tapeFile.fSeq = static_cast<uint64_t>(i + 1);
    raw->tapeFile.vid = "VTEST001";
    std::unique_ptr<cta::ArchiveJob> job(raw);
    const std::string msg = "skipped " + std::to_string(i);
    mrp.reportSkippedJob(std::move(job), msg, lc);
  }

  // reportEndOfSession flushes the remaining 1 job; total: 2 calls, 601 jobs.
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  ASSERT_EQ(2, tam.flushCount);
  ASSERT_EQ(601, tam.failureCount);
  ASSERT_EQ(1, tam.completes);
}

// Verify that when reportJobsBatchTransferred throws FailedReportCatalogueUpdate the
// exception propagates out of the worker thread (session-fatal) and the pgsched path
// calls failTransferBatch(queue) for the failed jobs rather than looping individually.
// In the unit-test context failedToReportArchiveJobs is always empty (no real DB jobs),
// so the test focuses on correct exception propagation and absence of crash.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedCatalogueUpdateFailurePropagates) {
  MockArchiveMountWithBatch tam(*m_catalogue);
  tam.throwMode = MockArchiveMountWithBatch::ThrowMode::CatalogueUpdate;

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedCatalogueUpdateFailurePropagates",
                             cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // Queue a completed job, then flush — reportJobsBatchTransferred will throw.
  std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  mrp.reportCompletedJob(std::move(job), lc);

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  // Worker thread terminates after catching the exception; push end-of-session so
  // the blocking queue unblocks waitThread() (it will be drained, not executed).
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  // The exception must have been logged by the worker thread.
  const std::string logOutput = log.getLog();
  ASSERT_NE(std::string::npos,
            logOutput.find("Received a CTA exception while reporting archive mount results"))
    << "Expected CTA exception log from worker thread; got: " << logOutput;
}

// Verify that when reportJobsBatchTransferred throws FailedReportMoveToQueue the
// exception propagates correctly and the pgsched path calls failReportBatch(queue).
// Same limitations as the CatalogueUpdate test above apply.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedMoveToQueueFailurePropagates) {
  MockArchiveMountWithBatch tam(*m_catalogue);
  tam.throwMode = MockArchiveMountWithBatch::ThrowMode::MoveToQueue;

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedMoveToQueueFailurePropagates",
                             cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  std::unique_ptr<cta::ArchiveJob> job(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  mrp.reportCompletedJob(std::move(job), lc);

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  const std::string logOutput = log.getLog();
  ASSERT_NE(std::string::npos,
            logOutput.find("Received a CTA exception while reporting archive mount results"))
    << "Expected CTA exception log from worker thread; got: " << logOutput;
}

// Verify that in the error-recovery path (m_errorHappened=true) a FailedReportMoveToQueue
// thrown by reportJobsBatchTransferred is also absorbed, with the same guarantee that
// flushFailedJobs() still drains the accumulated failure batch.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedMoveToQueueFailureInErrorRecovery) {
  MockArchiveMountWithBatch tam(*m_catalogue);
  tam.throwMode = MockArchiveMountWithBatch::ThrowMode::MoveToQueue;

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedMoveToQueueFailureInErrorRecovery",
                             cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  std::unique_ptr<cta::ArchiveJob> failedJob(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  const cta::exception::Exception ex("test failure");
  mrp.reportFailedJob(std::move(failedJob), ex, lc);

  std::unique_ptr<cta::ArchiveJob> successJob(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  mrp.reportCompletedJob(std::move(successJob), lc);

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  const std::string logOutput = log.getLog();
  ASSERT_NE(std::string::npos,
            logOutput.find("failed to report successful jobs in error-recovery path"))
    << "Expected error-recovery absorption log; got: " << logOutput;
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(1, tam.failureCount);
  ASSERT_EQ(1, tam.completes);
}

// Verify that in the error-recovery path (m_errorHappened=true) a catalogue update
// failure from reportJobsBatchTransferred is absorbed (logged as ERR) so that
// flushFailedJobs() still runs and accumulated failures are not lost.
TEST_F(cta_tape_daemon_MigrationReportPackerTest, MigrationReportPackerPgschedCatalogueFailureInErrorRecovery) {
  MockArchiveMountWithBatch tam(*m_catalogue);
  tam.throwMode = MockArchiveMountWithBatch::ThrowMode::CatalogueUpdate;

  cta::log::StringLogger log("dummy", "MigrationReportPackerPgschedCatalogueFailureInErrorRecovery",
                             cta::log::DEBUG);
  cta::log::LogContext lc(log);
  daemon::MigrationReportPacker mrp(&tam, lc);
  mrp.startThreads();

  // One failed job sets m_errorHappened=true; ReportFlush then takes the
  // error-recovery path which absorbs reportSuccessfulJobsBatch exceptions.
  std::unique_ptr<cta::ArchiveJob> failedJob(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  const cta::exception::Exception ex("test failure");
  mrp.reportFailedJob(std::move(failedJob), ex, lc);

  // One pending successful job — reportJobsBatchTransferred will throw for it,
  // but the exception must be absorbed in the error-recovery path.
  std::unique_ptr<cta::ArchiveJob> successJob(new MockArchiveJobWithPrepare(&tam, *m_catalogue));
  mrp.reportCompletedJob(std::move(successJob), lc);

  const drive::compressionStats stats;
  mrp.reportFlush(stats, lc);
  mrp.reportEndOfSession(lc);
  mrp.waitThread();

  const std::string logOutput = log.getLog();
  // The absorbed catalogue failure must be ERR-logged by ReportFlush::execute.
  ASSERT_NE(std::string::npos,
            logOutput.find("failed to report successful jobs in error-recovery path"))
    << "Expected error-recovery absorption log; got: " << logOutput;
  // The accumulated failed job must still have been flushed via flushFailedJobs().
  ASSERT_EQ(1, tam.flushCount);
  ASSERT_EQ(1, tam.failureCount);
  ASSERT_EQ(1, tam.completes);
}

#endif  // CTA_PGSCHED

}  // namespace unitTests
