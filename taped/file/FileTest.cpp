/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FileReader.hpp"
#include "FileReaderFactory.hpp"
#include "FileWriter.hpp"
#include "LabelSession.hpp"
#include "ReadSession.hpp"
#include "ReadSessionFactory.hpp"
#include "WriteSession.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "disk/DiskFile.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/drive/FakeDrive.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/system/Wrapper.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class TestingRetrieveJob : public cta::RetrieveJob {
public:
  TestingRetrieveJob()
      : cta::RetrieveJob(nullptr,
                         cta::common::dataStructures::RetrieveRequest(),
                         cta::common::dataStructures::ArchiveFile(),
                         1,
                         cta::PositioningMethod::ByBlock) {}
};

class TestingArchiveJob : public cta::ArchiveJob {
public:
  TestingArchiveJob()
      : cta::ArchiveJob(nullptr,
                        *(static_cast<cta::catalogue::Catalogue*>(nullptr)),
                        cta::common::dataStructures::ArchiveFile(),
                        "",
                        cta::common::dataStructures::TapeFile()) {}
};

class ctaTapeFileTest : public ::testing::TestWithParam<cta::common::dataStructures::Label::Format> {
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
    cta::tape::tapeFile::LabelSession::label(&m_drive, m_label, false);
    cta::tape::tapeFile::LabelSession::label(&m_drive, m_label, true);
  }

  virtual void TearDown() {}

  cta::tape::drive::FakeDrive m_drive;
  uint32_t m_block_size;
  std::string m_label;
  TestingRetrieveJob m_fileToRecall;
  TestingArchiveJob m_fileToMigrate;
  cta::tape::daemon::VolumeInfo m_volInfo;
};

TEST_P(ctaTapeFileTest, throwsWhenReadingAnEmptyTape) {
  m_volInfo.labelFormat = GetParam();
  const auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  ASSERT_NE(readSession, nullptr);
  m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
  // cannot read a file on an empty tape
  ASSERT_THROW(cta::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall), cta::exception::Exception);
}

TEST_F(ctaTapeFileTest, throwsWhenUnexpectedLabelFormat) {
  m_volInfo.labelFormat = static_cast<cta::common::dataStructures::Label::Format>(0xFF);
  std::unique_ptr<cta::tape::tapeFile::ReadSession> readSession;
  ASSERT_THROW(readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false),
               cta::tape::tapeFile::TapeFormatError);
  ASSERT_EQ(readSession, nullptr);
}

TEST_P(ctaTapeFileTest, throwsWhenUsingSessionTwice) {
  m_volInfo.labelFormat = GetParam();
  const std::string testString("Hello World!");
  std::unique_ptr<cta::tape::tapeFile::WriteSession> writeSession;
  ASSERT_NO_THROW(writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(
                    cta::tape::tapeFile::WriteSession(m_drive, m_volInfo, 0, true, false)));
  ASSERT_EQ(writeSession->m_compressionEnabled, true);
  ASSERT_EQ(writeSession->m_vid.compare(m_label), 0);
  ASSERT_EQ(writeSession->isCorrupted(), false);
  {
    std::unique_ptr<cta::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer =
                      std::make_unique<cta::tape::tapeFile::FileWriter>(*writeSession, m_fileToMigrate, m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }
  const auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = cta::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
    // cannot have two FileReader's on the same session
    ASSERT_THROW(cta::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall),
                 cta::tape::tapeFile::SessionAlreadyInUse);
  }
}

TEST_F(ctaTapeFileTest, throwsWhenWritingAnEmptyFileOrSessionCorrupted) {
  const auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, false);
  ASSERT_EQ(writeSession->isCorrupted(), false);
  {
    std::unique_ptr<cta::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer =
                      std::make_unique<cta::tape::tapeFile::FileWriter>(*writeSession, m_fileToMigrate, m_block_size));
    ASSERT_THROW(writer->close(), cta::tape::tapeFile::ZeroFileWritten);
  }
  ASSERT_EQ(writeSession->isCorrupted(), true);
  {
    ASSERT_THROW(cta::tape::tapeFile::FileWriter writer(*writeSession, m_fileToMigrate, m_block_size),
                 cta::tape::tapeFile::SessionCorrupted);
  }
}

TEST_F(ctaTapeFileTest, throwsWhenClosingTwice) {
  const std::string testString("Hello World!");
  const auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, false);
  std::unique_ptr<cta::tape::tapeFile::FileWriter> writer;
  ASSERT_NO_THROW(writer =
                    std::make_unique<cta::tape::tapeFile::FileWriter>(*writeSession, m_fileToMigrate, m_block_size));
  writer->write(testString.c_str(), testString.size());
  writer->close();
  ASSERT_THROW(writer->close(), cta::tape::tapeFile::FileClosedTwice);
}

TEST_P(ctaTapeFileTest, throwsWhenWrongBlockSizeOrEOF) {
  m_volInfo.labelFormat = GetParam();
  const std::string testString("Hello World!");
  {
    const auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, false);
    std::unique_ptr<cta::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer =
                      std::make_unique<cta::tape::tapeFile::FileWriter>(*writeSession, m_fileToMigrate, m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }

  auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, true);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = cta::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
    size_t blockSize = reader->getBlockSize();
    char* data = new char[blockSize + 1];
    // block size needs to be the same provided by the headers
    ASSERT_THROW(reader->readNextDataBlock(data, 1), cta::tape::tapeFile::WrongBlockSize);
    // it is normal to reach end of file after a loop of reads
    ASSERT_THROW(while (true) { reader->readNextDataBlock(data, blockSize); }, cta::tape::tapeFile::EndOfFile);
    delete[] data;
  }
}

TEST_P(ctaTapeFileTest, canProperlyVerifyLabelWriteAndReadTape) {
  // Verify label
  m_volInfo.labelFormat = GetParam();
  {
    const auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, false);
    ASSERT_NE(readSession, nullptr);
    ASSERT_EQ(readSession->getCurrentFilePart(), cta::tape::tapeFile::PartOfFile::Header);
    ASSERT_EQ(readSession->getCurrentFseq(), static_cast<uint32_t>(1));
    ASSERT_EQ(readSession->isCorrupted(), false);
    ASSERT_EQ(readSession->m_vid.compare(m_label), 0);
  }

  // Write AULFile with Hello World
  const std::string testString("Hello World!");
  {
    const auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(m_drive, m_volInfo, 0, true, true);
    ASSERT_EQ(writeSession->m_compressionEnabled, true);
    ASSERT_EQ(writeSession->m_useLbp, true);
    ASSERT_EQ(writeSession->m_vid.compare(m_label), 0);
    ASSERT_EQ(writeSession->isCorrupted(), false);
    std::unique_ptr<cta::tape::tapeFile::FileWriter> writer;
    ASSERT_NO_THROW(writer =
                      std::make_unique<cta::tape::tapeFile::FileWriter>(*writeSession, m_fileToMigrate, m_block_size));
    writer->write(testString.c_str(), testString.size());
    writer->close();
  }

  // Read it back and compare
  const auto readSession = cta::tape::tapeFile::ReadSessionFactory::create(m_drive, m_volInfo, true);
  ASSERT_NE(readSession, nullptr);
  ASSERT_EQ(readSession->getCurrentFilePart(), cta::tape::tapeFile::PartOfFile::Header);
  ASSERT_EQ(readSession->getCurrentFseq(), static_cast<uint32_t>(1));
  ASSERT_EQ(readSession->isCorrupted(), false);
  ASSERT_EQ(readSession->m_vid.compare(m_label), 0);
  ASSERT_EQ(readSession->m_useLbp, true);
  {
    m_fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    const auto reader = cta::tape::tapeFile::FileReaderFactory::create(*readSession, m_fileToRecall);
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

TEST_F(ctaTapeFileTest, tapeSessionThrowsOnWrongSequence) {
  cta::tape::tapeFile::WriteSession writeSession(m_drive, m_volInfo, 0, true, false);
  EXPECT_NO_THROW(writeSession.validateNextFSeq(1));
  EXPECT_THROW(writeSession.reportWrittenFSeq(2), cta::exception::Exception);
  EXPECT_NO_THROW(writeSession.reportWrittenFSeq(1));
  EXPECT_NO_THROW(writeSession.validateNextFSeq(2));
  EXPECT_THROW(writeSession.validateNextFSeq(1), cta::exception::Exception);
}

INSTANTIATE_TEST_CASE_P(FormatLabelsParam,
                        ctaTapeFileTest,
                        ::testing::Values(cta::common::dataStructures::Label::Format::CTA
                                          // , cta::common::dataStructures::Label::Format::OSM
                                          ));

TEST(ctaTapeDiskFile, canWriteAndReadDisk) {
  const uint32_t block_size = 1024;
  char* data1 = new char[block_size];
  char* data2 = new char[block_size];
  cta::disk::DiskFileFactory fileFactory(0);
  TempFile sourceFile;
  sourceFile.randomFill(1000);
  TempFile destinationFile(sourceFile.path() + "_dst");
  // host part of file location
  std::string lh = "localhost:";
  {
    std::unique_ptr<cta::disk::ReadFile> rf(fileFactory.createReadFile(lh + sourceFile.path()));
    std::unique_ptr<cta::disk::WriteFile> wf(fileFactory.createWriteFile(lh + destinationFile.path()));
    size_t res = 0;
    do {
      res = rf->read(data1, block_size);
      wf->write(data1, res);
    } while (res);
    wf->close();
  }
  std::unique_ptr<cta::disk::ReadFile> src(fileFactory.createReadFile(sourceFile.path()));
  std::unique_ptr<cta::disk::ReadFile> dst(fileFactory.createReadFile(destinationFile.path()));
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
  const char* dirTestPath = "/tmp/testDir";
  ::rmdir(dirTestPath);
  cta::disk::LocalDirectory dir(dirTestPath);
  ASSERT_NO_THROW(dir.mkdir());
  ::rmdir(dirTestPath);
}

TEST(ctaDirectoryTests, directoryFailCreate) {
  const char* dirTestPath = "//WRONG/PATH";
  cta::disk::LocalDirectory dir(dirTestPath);
  ASSERT_THROW(dir.mkdir(), cta::exception::Errnum);
}

TEST(ctaDirectoryTests, directoryGetFilesName) {
  std::string dirTestPath = "/tmp/directoryGetFilesNames";
  std::string rmCommand = "rm -rf " + dirTestPath;
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
