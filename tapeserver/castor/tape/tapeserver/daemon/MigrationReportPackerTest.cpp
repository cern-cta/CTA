/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"

#include <gtest/gtest.h>

using ::testing::_;
using ::testing::Invoke;
using namespace castor::tape;

namespace unitTests {

const uint32_t TEST_USER_1  = 9751;
const uint32_t TEST_GROUP_1 = 9752;
const uint32_t TEST_USER_2  = 9753;
const uint32_t TEST_GROUP_2 = 9754;

  class castor_tape_tapeserver_daemon_MigrationReportPackerTest: public ::testing::Test {
  public:
    castor_tape_tapeserver_daemon_MigrationReportPackerTest():
      m_dummyLog("dummy", "dummy") {
    }

  protected:

    void SetUp() {
      using namespace cta;
      using namespace cta::catalogue;

      rdbms::Login catalogueLogin(rdbms::Login::DBTYPE_IN_MEMORY, "", "", "", "", 0);
      const uint64_t nbConns = 1;
      const uint64_t nbArchiveFileListingConns = 0;
      auto catalogueFactory = CatalogueFactoryFactory::create(m_dummyLog, catalogueLogin, nbConns,
        nbArchiveFileListingConns);
      m_catalogue = catalogueFactory->create();
    }
    
    void createMediaType(const std::string & name){
      cta::common::dataStructures::SecurityIdentity admin = cta::common::dataStructures::SecurityIdentity("admin","localhost");
      cta::catalogue::MediaType mediaType;
      mediaType.name = name;
      mediaType.capacityInBytes = 10;
      mediaType.cartridge = "cartridge";
      mediaType.comment = "comment";
      m_catalogue->createMediaType(admin,mediaType);
    }
    
    cta::common::dataStructures::VirtualOrganization getDefaultVo(){
      cta::common::dataStructures::VirtualOrganization vo;
      vo.name = "vo";
      vo.readMaxDrives = 1;
      vo.writeMaxDrives = 1;
      vo.maxFileSize = 0;
      vo.comment = "comment";
      return vo;
    }

    void TearDown() {
      m_catalogue.reset();
    }

    cta::log::DummyLogger m_dummyLog;
    std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  }; // class castor_tape_tapeserver_daemon_MigrationReportPackerTest

  class MockArchiveJobExternalStats: public cta::MockArchiveJob {
  public:
    MockArchiveJobExternalStats(cta::ArchiveMount & am, cta::catalogue::Catalogue & catalogue, 
       int & completes, int &failures):
    MockArchiveJob(&am, catalogue), completesRef(completes), failuresRef(failures) {}

    virtual void validate() override {}
    virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten() override {
      auto fileReportUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileReport = *fileReportUP;
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


    void transferFailed(const std::string& failureReason, cta::log::LogContext& lc) override {
      failuresRef++;
    }

    void reportJobSucceeded() override {
      completesRef++;
    }

  private:
    int & completesRef;
    int & failuresRef;
  };

  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerNominal) {
    cta::MockArchiveMount tam(*m_catalogue);

    const std::string vid1 = "VTEST001";
    const std::string vid2 = "VTEST002";
    const std::string mediaType = "media_type";
    const std::string vendor = "vendor";
    const std::string logicalLibraryName = "logical_library_name";
    const bool logicalLibraryIsDisabled = false;
    const std::string tapePoolName = "tape_pool_name";
    const cta::optional<std::string> supply("value for the supply pool mechanism");
    const bool fullValue = false;
    const std::string createTapeComment = "Create tape";
    cta::common::dataStructures::VirtualOrganization vo = getDefaultVo();
    
    cta::common::dataStructures::SecurityIdentity admin = cta::common::dataStructures::SecurityIdentity("admin","localhost");
    m_catalogue->createVirtualOrganization(admin,vo);
    
    m_catalogue->createLogicalLibrary(admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
    m_catalogue->createTapePool(admin, tapePoolName, vo.name, 2, true, supply, "Create tape pool");
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
      m_catalogue->createTape(admin, tape);
    }

    cta::common::dataStructures::StorageClass storageClass;
    
    storageClass.name = "storage_class";
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "Create storage class";
    m_catalogue->createStorageClass(admin, storageClass);

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> job1;
    int job1completes(0), job1failures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, job1completes, job1failures));
      job1.reset(mockJob.release());
    }
    job1->archiveFile.archiveFileID=1;
    job1->archiveFile.diskInstance="disk_instance";
    job1->archiveFile.diskFileId="diskFileId1";
    job1->archiveFile.diskFileInfo.path="filePath1";
    job1->archiveFile.diskFileInfo.owner_uid=TEST_USER_1;
    job1->archiveFile.diskFileInfo.gid=TEST_GROUP_1;
    job1->archiveFile.fileSize=1024;        
    job1->archiveFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
    job1->archiveFile.storageClass="storage_class";
    job1->tapeFile.vid="VTEST001";
    job1->tapeFile.fSeq=1;
    job1->tapeFile.blockId=256;
    job1->tapeFile.fileSize=768;
    job1->tapeFile.copyNb=1;
    job1->tapeFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

    std::unique_ptr<cta::ArchiveJob> job2;
    int job2completes(0), job2failures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, job2completes, job2failures));
      job2.reset(mockJob.release());
    }
    job2->archiveFile.archiveFileID=2;
    job2->archiveFile.diskInstance="disk_instance";
    job2->archiveFile.diskFileId="diskFileId2";
    job2->archiveFile.diskFileInfo.path="filePath2";
    job2->archiveFile.diskFileInfo.owner_uid=TEST_USER_2;
    job2->archiveFile.diskFileInfo.gid=TEST_GROUP_2;
    job2->archiveFile.fileSize=1024;        
    job2->archiveFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
    job2->archiveFile.storageClass="storage_class";
    job2->tapeFile.vid="VTEST001";
    job2->tapeFile.fSeq=2;
    job2->tapeFile.blockId=512;
    job2->tapeFile.fileSize=768;
    job2->tapeFile.copyNb=1;
    job2->tapeFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_MigrationReportPackerNominal",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1), lc);
    mrp.reportCompletedJob(std::move(job2), lc);

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread(); //here

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
    ASSERT_EQ(1, tam.completes);
    ASSERT_EQ(1, job1completes);
    ASSERT_EQ(1, job2completes);
  }

  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerFailure) {
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

    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_MigrationReportPackerFailure",cta::log::DEBUG);
    cta::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1), lc);
    mrp.reportCompletedJob(std::move(job2), lc);

    const std::string error_msg = "ERROR_TEST_MSG";
    const cta::exception::Exception ex(error_msg);
    mrp.reportFailedJob(std::move(job3),ex, lc);

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find(error_msg));
    ASSERT_EQ(1, tam.completes);
    ASSERT_EQ(1, job3failures);
  }

  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerBadFile) {
    cta::MockArchiveMount tam(*m_catalogue);

    const std::string vid1 = "VTEST001";
    const std::string vid2 = "VTEST002";
    const std::string mediaType = "media_type";
    const std::string vendor = "vendor";
    const std::string logicalLibraryName = "logical_library_name";
    const bool logicalLibraryIsDisabled = false;
    const std::string tapePoolName = "tape_pool_name";
    const uint64_t nbPartialTapes = 2;
    const bool isEncrypted = true;
    const cta::optional<std::string> supply("value for the supply pool mechanism");
    const bool fullValue = false;
    const std::string createTapeComment = "Create tape";
    cta::common::dataStructures::SecurityIdentity admin = cta::common::dataStructures::SecurityIdentity("admin","localhost");

    cta::common::dataStructures::VirtualOrganization vo = getDefaultVo();
    
    m_catalogue->createVirtualOrganization(admin,vo);
    
    m_catalogue->createLogicalLibrary(admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
    m_catalogue->createTapePool(admin, tapePoolName, vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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
      m_catalogue->createTape(admin, tape);
    }

    cta::common::dataStructures::StorageClass storageClass;
    
    storageClass.name = "storage_class";
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "Create storage class";
    m_catalogue->createStorageClass(admin, storageClass);

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

    migratedBigFile->archiveFile.archiveFileID=4;
    migratedBigFile->archiveFile.diskInstance="disk_instance";
    migratedBigFile->archiveFile.diskFileId="diskFileId2";
    migratedBigFile->archiveFile.diskFileInfo.path="filePath2";
    migratedBigFile->archiveFile.diskFileInfo.owner_uid=TEST_USER_2;
    migratedBigFile->archiveFile.diskFileInfo.gid=TEST_GROUP_2;
    migratedBigFile->archiveFile.fileSize=100000;        
    migratedBigFile->archiveFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
    migratedBigFile->archiveFile.storageClass="storage_class";
    migratedBigFile->tapeFile.vid="VTEST001";
    migratedBigFile->tapeFile.fSeq=1;
    migratedBigFile->tapeFile.blockId=256;
    migratedBigFile->tapeFile.fileSize=768;
    migratedBigFile->tapeFile.copyNb=1;
    migratedBigFile->tapeFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

    migratedFileSmall->archiveFile.archiveFileID=5;
    migratedFileSmall->archiveFile.diskInstance="disk_instance";
    migratedFileSmall->archiveFile.diskFileId="diskFileId3";
    migratedFileSmall->archiveFile.diskFileInfo.path="filePath3";
    migratedFileSmall->archiveFile.diskFileInfo.owner_uid=TEST_USER_2;
    migratedFileSmall->archiveFile.diskFileInfo.gid=TEST_GROUP_2;
    migratedFileSmall->archiveFile.fileSize=1;        
    migratedFileSmall->archiveFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
    migratedFileSmall->archiveFile.storageClass="storage_class";
    migratedFileSmall->tapeFile.vid="VTEST001";
    migratedFileSmall->tapeFile.fSeq=2;
    migratedFileSmall->tapeFile.blockId=512;
    migratedFileSmall->tapeFile.fileSize=1;
    migratedFileSmall->tapeFile.copyNb=1;
    migratedFileSmall->tapeFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

    migratedNullFile->archiveFile.archiveFileID=6;
    migratedNullFile->archiveFile.diskInstance="disk_instance";
    migratedNullFile->archiveFile.diskFileId="diskFileId4";
    migratedNullFile->archiveFile.diskFileInfo.path="filePath4";
    migratedNullFile->archiveFile.diskFileInfo.owner_uid=TEST_USER_2;
    migratedNullFile->archiveFile.diskFileInfo.gid=TEST_GROUP_2;
    migratedNullFile->archiveFile.fileSize=0;        
    migratedNullFile->archiveFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));
    migratedNullFile->archiveFile.storageClass="storage_class";
    migratedNullFile->tapeFile.vid="VTEST001";
    migratedNullFile->tapeFile.fSeq=3;
    migratedNullFile->tapeFile.blockId=768;
    migratedNullFile->tapeFile.fileSize=0;
    migratedNullFile->tapeFile.copyNb=1;
    migratedNullFile->tapeFile.checksumBlob.insert(cta::checksum::MD5, cta::checksum::ChecksumBlob::HexToByteArray("b170288bf1f61b26a648358866f4d6c6"));

    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_MigrationReportPackerOneByteFile",cta::log::DEBUG);
    cta::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(migratedBigFile), lc);
    mrp.reportCompletedJob(std::move(migratedFileSmall), lc);
    mrp.reportCompletedJob(std::move(migratedNullFile), lc);
    tapeserver::drive::compressionStats stats;
    stats.toTape=(100000+1)/3;
    mrp.reportFlush(stats, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("TapeFileWrittenEvent is invalid"));
    ASSERT_NE(std::string::npos, temp.find("Received a CTA exception while reporting archive mount results"));
    ASSERT_EQ(0, tam.completes);
    ASSERT_EQ(0, migratedBigFileCompletes);
    ASSERT_EQ(0, migratedFileSmallCompletes);
    ASSERT_EQ(0, migratedNullFileCompletes);
  } 
}
