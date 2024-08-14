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

#include <gtest/gtest.h>

#include <memory>

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
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "disk/DiskFile.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "disk/RadosStriperPool.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace unitTests {

class TestingRetrieveJob: public cta::RetrieveJob {
public:
  TestingRetrieveJob() : cta::RetrieveJob(nullptr,
  cta::common::dataStructures::RetrieveRequest(),
  cta::common::dataStructures::ArchiveFile(), 1,
  cta::PositioningMethod::ByBlock) {}
};

class TestingArchiveJob: public cta::ArchiveJob {
public:
  TestingArchiveJob(): cta::ArchiveJob(nullptr,
      *(static_cast<cta::catalogue::Catalogue *>(nullptr)), cta::common::dataStructures::ArchiveFile(),
      "", cta::common::dataStructures::TapeFile()) {
  }
};

class castorTapeFileTest : public ::testing::TestWithParam<cta::common::dataStructures::Label::Format> {
protected:
  virtual void SetUp() {
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
    // Label
    castor::tape::tapeFile::LabelSession::label(&m_drive, m_label, false);
    castor::tape::tapeFile::LabelSession::label(&m_drive, m_label, true);
  }

  virtual void TearDown() {}

  castor::tape::tapeserver::drive::FakeDrive m_drive;
  uint32_t m_block_size;
  std::string m_label;
  TestingRetrieveJob m_fileToRecall;
  TestingArchiveJob m_fileToMigrate;
  castor::tape::tapeserver::daemon::VolumeInfo m_volInfo;
};

TEST_P(castorTapeFileTest, throwsWhenReadingAnEmptyTape) {
  m_volInfo.labelFormat = GetParam();
  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  ASSERT_NE(readSession, nullptr);
  m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
  // cannot read a file on an empty tape
  ASSERT_THROW(castor::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall),
    cta::exception::Exception);
}

TEST_F(castorTapeFileTest, throwsWhenUnexpectedLabelFormat) {
  m_volInfo.labelFormat = static_cast<cta::common::dataStructures::Label::Format>(0xFF);
  std::unique_ptr<castor::tape::tapeFile::ReadSession> readSession;
  ASSERT_THROW(readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false),
    castor::tape::tapeFile::TapeFormatError);
  ASSERT_EQ(readSession, nullptr);
}

TEST_P(castorTapeFileTest, throwsWhenUsingSessionTwice) {
  m_volInfo.labelFormat = GetParam();
  const std::string testString("Hello World!");
  std::unique_ptr<castor::tape::tapeFile::WriteSession> writeSession;
  ASSERT_NO_THROW(writeSession = std::make_unique<castor::tape::tapeFile::WriteSession>(
    castor::tape::tapeFile::WriteSession(m_drive, m_volInfo, 0, true, false)));
  ASSERT_EQ(writeSession->m_compressionEnabled, true);
  ASSERT_EQ(writeSession->m_vid.compare(m_label), 0);
  ASSERT_EQ(writeSession->isCorrupted(), false);
  {
    std::unique_ptr<castor::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer = std::make_unique<castor::tape::tapeFile::FileWriter>(writeSession, m_fileToMigrate,
      m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }
  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
    // cannot have two FileReader's on the same session
    ASSERT_THROW(castor::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall),
      castor::tape::tapeFile::SessionAlreadyInUse);
  }
}

TEST_F(castorTapeFileTest, throwsWhenWritingAnEmptyFileOrSessionCorrupted) {
  const auto writeSession = std::make_unique<castor::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, false);
  ASSERT_EQ(writeSession->isCorrupted(), false);
  {
    std::unique_ptr<castor::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer = std::make_unique<castor::tape::tapeFile::FileWriter>(writeSession, m_fileToMigrate,
      m_block_size));
    ASSERT_THROW(writer->close(), castor::tape::tapeFile::ZeroFileWritten);
  }
  ASSERT_EQ(writeSession->isCorrupted(), true);
  {
    ASSERT_THROW(castor::tape::tapeFile::FileWriter writer(writeSession, m_fileToMigrate, m_block_size),
      castor::tape::tapeFile::SessionCorrupted);
  }
}

TEST_F(castorTapeFileTest, throwsWhenClosingTwice) {
  const std::string testString("Hello World!");
  const auto writeSession = std::make_unique<castor::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, false);
  std::unique_ptr<castor::tape::tapeFile::FileWriter> writer;
  ASSERT_NO_THROW(writer = std::make_unique<castor::tape::tapeFile::FileWriter>(writeSession, m_fileToMigrate,
    m_block_size));
  writer->write(testString.c_str(), testString.size());
  writer->close();
  ASSERT_THROW(writer->close(), castor::tape::tapeFile::FileClosedTwice);
}

TEST_P(castorTapeFileTest, throwsWhenWrongBlockSizeOrEOF) {
  m_volInfo.labelFormat = GetParam();
  const std::string testString("Hello World!");
  {
    const auto writeSession = std::make_unique<castor::tape::tapeFile::WriteSession>(
      m_drive, m_volInfo, 0, true, false);
    std::unique_ptr<castor::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer = std::make_unique<castor::tape::tapeFile::FileWriter>(writeSession, m_fileToMigrate,
      m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }

  auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, true);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
    size_t blockSize = reader->getBlockSize();
    char *data = new char[blockSize+1];
    // block size needs to be the same provided by the headers
    ASSERT_THROW(reader->readNextDataBlock(data, 1), castor::tape::tapeFile::WrongBlockSize);
    // it is normal to reach end of file after a loop of reads
    ASSERT_THROW(while(true) {
        reader->readNextDataBlock(data, blockSize);
    },
    castor::tape::tapeFile::EndOfFile);
    delete[] data;
  }
}

TEST_P(castorTapeFileTest, canProperlyVerifyLabelWriteAndReadTape) {
  // Verify label
  m_volInfo.labelFormat = GetParam();
  {
    const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
    ASSERT_NE(readSession, nullptr);
    ASSERT_EQ(readSession->getCurrentFilePart(), castor::tape::tapeFile::PartOfFile::Header);
    ASSERT_EQ(readSession->getCurrentFseq(), static_cast<uint32_t>(1));
    ASSERT_EQ(readSession->isCorrupted(), false);
    ASSERT_EQ(readSession->m_vid.compare(m_label), 0);
  }

  // Write AULFile with Hello World
  const std::string testString("Hello World!");
  {
    const auto writeSession = std::make_unique<castor::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, true);
    ASSERT_EQ(writeSession->m_compressionEnabled, true);
    ASSERT_EQ(writeSession->m_useLbp, true);
    ASSERT_EQ(writeSession->m_vid.compare(m_label), 0);
    ASSERT_EQ(writeSession->isCorrupted(), false);
    std::unique_ptr<castor::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer = std::make_unique<castor::tape::tapeFile::FileWriter>(writeSession, m_fileToMigrate,
      m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }

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
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
    size_t blockSize = reader->getBlockSize();
    ASSERT_EQ(blockSize, m_block_size);
    char *data = new char[blockSize+1];
    size_t bytes_read = reader->readNextDataBlock(data, blockSize);
    data[bytes_read] = '\0';
    ASSERT_EQ(bytes_read, static_cast<size_t>(testString.size()));
    ASSERT_EQ(testString.compare(data), 0);
    delete[] data;
  }
}

TEST_F(castorTapeFileTest, tapeSessionThrowsOnWrongSequence) {
  castor::tape::tapeFile::WriteSession writeSession(m_drive, m_volInfo, 0, true, false);
  EXPECT_NO_THROW(writeSession.validateNextFSeq(1));
  EXPECT_THROW(writeSession.reportWrittenFSeq(2), cta::exception::Exception);
  EXPECT_NO_THROW(writeSession.reportWrittenFSeq(1));
  EXPECT_NO_THROW(writeSession.validateNextFSeq(2));
  EXPECT_THROW(writeSession.validateNextFSeq(1), cta::exception::Exception);
}

INSTANTIATE_TEST_CASE_P(FormatLabelsParam, castorTapeFileTest,
  ::testing::Values(cta::common::dataStructures::Label::Format::CTA
                  // , cta::common::dataStructures::Label::Format::OSM
));

// Class creating a temporary file of 1kB and deleting it
// automatically
class TempFile {
public:
  TempFile() {
    char path[] = "/tmp/testCTA-XXXXXX";
    int fd = ::mkstemp(path);
    cta::exception::Errnum::throwOnMinusOne(fd, "In TempFile::TempFile: failed to mkstemp: ");
    ::close(fd);
    m_path = path;
  }

  explicit TempFile(const std::string& path) : m_path(path) {}
  std::string path() { return m_path; }
  void randomFill(size_t size) {
    std::ofstream out(m_path, std::ios::out | std::ios::binary);
    std::ifstream in("/dev/urandom", std::ios::in|std::ios::binary);
    std::unique_ptr<char[]> buff(new char[size]);
    in.read(buff.get(), size);
    out.write(buff.get(), size);
  }
  ~TempFile() {
    if (m_path.size()) {
      ::unlink(m_path.c_str());
    }
  }

private:
  std::string m_path;
};

TEST(castorTapeDiskFile, canWriteAndReadDisk) {
  const uint32_t block_size = 1024;
  char *data1 = new char[block_size];
  char *data2 = new char[block_size];
  cta::disk::RadosStriperPool striperPool;
  cta::disk::DiskFileFactory fileFactory(0, striperPool);
  TempFile sourceFile;
  sourceFile.randomFill(1000);
  std::string src_path = std::string(sourceFile.path());
  std::string dst_path = src_path + "_dst";
  TempFile destinationFile(dst_path);
  // host part of file location
  std::string lh_src = "localhost:" + src_path;
  std::string lh_dst = lh_src + "_dst";
  {
    std::unique_ptr<cta::disk::ReadFile> rf(
      fileFactory.createReadFile(lh_src));
    std::unique_ptr<cta::disk::WriteFile> wf(
      fileFactory.createWriteFile(lh_dst));
    size_t res = 0;
    do {
      res = rf->read(data1, block_size);
      wf->write(data1, res);
    } while (res);
    wf->close();
  }
  std::unique_ptr<cta::disk::ReadFile> src(
      fileFactory.createReadFile(src_path));
  std::unique_ptr<cta::disk::ReadFile> dst(
      fileFactory.createReadFile(dst_path));
  size_t res1 = 0;
  size_t res2 = 0;
  do {
    res1 = src->read(data1, block_size);
    res2 = dst->read(data2, block_size);
    ASSERT_EQ(res1, res2);
    ASSERT_EQ(strncmp(data1, data2, res1), 0);
  } while (res1 || res2);
  delete[] data1;
  delete[] data2;
}

TEST(ctaDirectoryTests, directoryExist) {
  cta::disk::LocalDirectory dir("/tmp/");
  ASSERT_TRUE(dir.exist());

  cta::disk::LocalDirectory dirNotExist("/AZERTY/");
  ASSERT_FALSE(dirNotExist.exist());
}

TEST(ctaDirectoryTests, directoryCreate) {
  const char * dirTestPath = "/tmp/testDir";
  ::rmdir(dirTestPath);
  cta::disk::LocalDirectory dir(dirTestPath);
  ASSERT_NO_THROW(dir.mkdir());
  ::rmdir(dirTestPath);
}

TEST(ctaDirectoryTests, directoryFailCreate) {
  const char * dirTestPath = "//WRONG/PATH";
  cta::disk::LocalDirectory dir(dirTestPath);
  ASSERT_THROW(dir.mkdir(), cta::exception::Errnum);
}

TEST(ctaDirectoryTests, directoryGetFilesName) {
  std::string dirTestPath = "/tmp/directoryGetFilesNames";
  std::string rmCommand = "rm -rf "+dirTestPath;
  ::system(rmCommand.c_str());
  cta::disk::LocalDirectory dir(dirTestPath);
  ASSERT_NO_THROW(dir.mkdir());
  char filePath[] = "/tmp/directoryGetFilesNames/fileXXXXXX";
  int fileDirectory = ::mkstemp(filePath);
  cta::exception::Errnum::throwOnMinusOne(fileDirectory, "In directoryGetFilesName, fail mkstemp");
  ::close(fileDirectory);
  ASSERT_EQ(1, dir.getFilesName().size());
  ::unlink(filePath);
}

}  // namespace unitTests
