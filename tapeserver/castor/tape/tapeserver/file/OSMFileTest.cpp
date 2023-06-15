#include <gtest/gtest.h>

#include <memory>

#include <cstdio>

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "castor/tape/tapeserver/file/FileWriter.hpp"
#include "castor/tape/tapeserver/file/LabelSession.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "castor/tape/tapeserver/file/WriteSession.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "disk/DiskFile.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "disk/RadosStriperPool.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace unitTests {

class TestingRetrieveJob : public cta::RetrieveJob {
public:
  TestingRetrieveJob() :
  cta::RetrieveJob(nullptr,
                   cta::common::dataStructures::RetrieveRequest(),
                   cta::common::dataStructures::ArchiveFile(),
                   1,
                   cta::PositioningMethod::ByBlock) {}
};

class TestingArchiveJob : public cta::ArchiveJob {
public:
  TestingArchiveJob() :
  cta::ArchiveJob(nullptr,
                  *(static_cast<cta::catalogue::Catalogue*>(nullptr)),
                  cta::common::dataStructures::ArchiveFile(),
                  "",
                  cta::common::dataStructures::TapeFile()) {}
};

class OSMTapeFileTest : public ::testing::TestWithParam<cta::common::dataStructures::Label::Format> {
protected:
  virtual void SetUp() {
    // Gets information about the currently running test.
    // Do NOT delete the returned object - it's managed by the UnitTest class.
    const testing::TestInfo* const pTestInfo = testing::UnitTest::GetInstance()->current_test_info();
    /*
     * pTestInfo->name() - test name
     * pTestInfo->test_case_name() - suite name
     */
    m_strTestName = {pTestInfo->name()};

    m_block_size = 262144;
    m_label = "K00001";
    m_fileToRecall.selectedCopyNb = 1;
    cta::common::dataStructures::TapeFile tf;
    tf.blockId = 0;
    tf.fSeq = 1;
    tf.copyNb = 1;
    m_fileToRecall.archiveFile.tapeFiles.push_back(tf);
    m_fileToRecall.retrieveRequest.archiveFileID = 1;
    m_fileToMigrate.archiveFile.fileSize = 500;
    m_fileToMigrate.archiveFile.archiveFileID = 1;
    m_fileToMigrate.tapeFile.fSeq = 1;
    m_volInfo.vid = m_label;
    m_volInfo.labelFormat = cta::common::dataStructures::Label::Format::OSM;

    // Write OSM label
    castor::tape::tapeFile::osm::LABEL osmLabel;
    osmLabel.encode(0, 1, castor::tape::tapeFile::osm::LIMITS::MAXMRECSIZE, 1, m_label, "DESY", "1.1");
    m_drive.writeBlock(reinterpret_cast<void*>(osmLabel.rawLabel()), castor::tape::tapeFile::osm::LIMITS::MAXMRECSIZE);
    m_drive.writeBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + castor::tape::tapeFile::osm::LIMITS::MAXMRECSIZE),
                       castor::tape::tapeFile::osm::LIMITS::MAXMRECSIZE);

    m_drive.writeSyncFileMarks(1);
    // Write OSM file
    if (m_strTestName == "throwsWhenUsingSessionTwice" || m_strTestName == "throwsWhenWrongBlockSizeOrEOF" ||
        m_strTestName == "canProperlyVerifyLabelWriteAndReadTape") {
      const std::string strTestString = {"Hello World!"};
      std::stringstream file;
      const std::string strMagic = "070707";
      uint32_t uiDev = 0;
      uint32_t uiIno = 1;
      uint32_t uiMode = 33188;
      uint32_t uiUid = 0;
      uint32_t uiGid = 0;
      uint32_t uiNlink = 1;
      uint32_t uiRdev = 0;
      uint64_t ulMtime = 1660901455;
      uint32_t uiNameSize = 11;
      uint64_t ui64FileSize = strTestString.size();
      const std::string strFid = "01234567890";
      const size_t PAYLOAD_BLOCK_SIZE = 262144;

      // Preparing CPIO-ASCII-header
      file << strMagic << std::setfill('0') << std::oct << std::setw(6) << uiDev << std::setw(6) << uiIno
           << std::setw(6) << uiMode << std::setw(6) << uiUid << std::setw(6) << uiGid << std::setw(6) << uiNlink
           << std::setw(6) << uiRdev << std::setw(11) << ulMtime << std::setw(6) << uiNameSize << "H" << std::hex
           << std::setw(10) << ui64FileSize << strFid;
      // Write data
      file << strTestString;
      //Preparing trailer
      file << strMagic << std::setfill('0') << std::oct << std::setw(6) << uiDev << std::setw(6) << uiIno
           << std::setw(6) << uiMode << std::setw(6) << uiUid << std::setw(6) << uiGid << std::setw(6) << uiNlink
           << std::setw(6) << uiRdev << std::setw(11) << ulMtime << std::setw(6) << 10 << "H" << std::hex
           << std::setw(10)
           << 0
           //   << std::setw(1024)
           << strFid << "TRAILER!!" << 0;

      char acBuffer[PAYLOAD_BLOCK_SIZE] = {'\0'};

      while (file.rdbuf()->sgetn(acBuffer, PAYLOAD_BLOCK_SIZE)) {
        m_drive.writeBlock(reinterpret_cast<void*>(acBuffer), PAYLOAD_BLOCK_SIZE);
        // Nullify
        std::fill(acBuffer, acBuffer + PAYLOAD_BLOCK_SIZE, NULL);  // where NULL = 0
      }

      m_drive.writeSyncFileMarks(1);
    }
  }

  virtual void TearDown() {}

  castor::tape::tapeserver::drive::FakeDrive m_drive;
  uint32_t m_block_size;
  std::string m_label;
  TestingRetrieveJob m_fileToRecall;
  TestingArchiveJob m_fileToMigrate;
  castor::tape::tapeserver::daemon::VolumeInfo m_volInfo;

  std::string m_strTestName;
  std::vector<std::string> m_vTestToSkipp;
};

TEST_F(OSMTapeFileTest, throwsWhenReadingAnEmptyTape) {
  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  ASSERT_NE(readSession, nullptr);
  m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
  // cannot read a file on an empty tape
  ASSERT_THROW(castor::tape::tapeFile::FileReaderFactory::create(readSession, m_fileToRecall),
               cta::exception::Exception);
}

TEST_F(OSMTapeFileTest, throwsWhenUnexpectedLabelFormat) {
  m_volInfo.labelFormat = static_cast<cta::common::dataStructures::Label::Format>(0xFF);
  std::unique_ptr<castor::tape::tapeFile::ReadSession> readSession;
  ASSERT_THROW(readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false),
               castor::tape::tapeFile::TapeFormatError);
  ASSERT_EQ(readSession, nullptr);
}

TEST_F(OSMTapeFileTest, throwsWhenUsingSessionTwice) {
  const std::string testString("Hello World!");

  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, m_fileToRecall);
    // cannot have two FileReader's on the same session
    ASSERT_THROW(castor::tape::tapeFile::FileReaderFactory::create(readSession, m_fileToRecall),
                 castor::tape::tapeFile::SessionAlreadyInUse);
  }
}

TEST_F(OSMTapeFileTest, throwsWhenWrongBlockSizeOrEOF) {
  const std::string testString("Hello World!");

  auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, true);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, m_fileToRecall);
    size_t blockSize = reader->getBlockSize();
    char* data = new char[blockSize + 1];
    // block size needs to be the same provided by the headers
    ASSERT_THROW(reader->readNextDataBlock(data, 1), castor::tape::tapeFile::WrongBlockSize);
    // it is normal to reach end of file after a loop of reads
    ASSERT_THROW(
      while (true) { reader->readNextDataBlock(data, blockSize); }, castor::tape::tapeFile::EndOfFile);
    delete[] data;
  }
}

TEST_F(OSMTapeFileTest, canProperlyVerifyLabelWriteAndReadTape) {
  // Verify label
  {
    const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
    ASSERT_NE(readSession, nullptr);
    ASSERT_EQ(readSession->getCurrentFilePart(), castor::tape::tapeFile::PartOfFile::Header);
    ASSERT_EQ(readSession->getCurrentFseq(), static_cast<uint32_t>(1));
    ASSERT_EQ(readSession->isCorrupted(), false);
    ASSERT_EQ(readSession->m_vid.compare(m_label), 0);
  }

  const std::string testString("Hello World!");
  // Read it back and compare
  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, true);
  ASSERT_NE(readSession, nullptr);
  ASSERT_EQ(readSession->getCurrentFilePart(), castor::tape::tapeFile::PartOfFile::Header);
  ASSERT_EQ(readSession->getCurrentFseq(), static_cast<uint32_t>(1));
  ASSERT_EQ(readSession->isCorrupted(), false);
  ASSERT_EQ(readSession->m_vid.compare(m_label), 0);
  ASSERT_EQ(readSession->m_useLbp, true);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, m_fileToRecall);
    size_t blockSize = reader->getBlockSize();
    ASSERT_EQ(blockSize, m_block_size);
    char* data = new char[blockSize + 1];
    size_t bytes_read = reader->readNextDataBlock(data, blockSize);
    data[bytes_read] = '\0';
    ASSERT_EQ(bytes_read, static_cast<size_t>(testString.size()));
    ASSERT_EQ(testString.compare(data), 0);
    delete[] data;
  }
}

}  // namespace unitTests
