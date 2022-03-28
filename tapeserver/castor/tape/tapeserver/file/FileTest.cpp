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

#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "disk/DiskFile.hpp"
#include "disk/RadosStriperPool.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "disk/DiskFileImplementations.hpp"

#include <gtest/gtest.h>
#include <memory>

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
        *((cta::catalogue::Catalogue *)nullptr), cta::common::dataStructures::ArchiveFile(),
        "", cta::common::dataStructures::TapeFile()) {
    }
  };
  
  class castorTapeFileTest : public ::testing::Test {
  protected:
    virtual void SetUp() {
      block_size = 262144;
      label = "K00001";
      fileToRecall.selectedCopyNb=1;
      cta::common::dataStructures::TapeFile tf;
      tf.blockId = 0;
      tf.fSeq = 1;
      tf.copyNb = 1;
      fileToRecall.archiveFile.tapeFiles.push_back(tf);
      fileToRecall.retrieveRequest.archiveFileID = 1;
      fileToMigrate.archiveFile.fileSize = 500;
      fileToMigrate.archiveFile.archiveFileID = 1;
      fileToMigrate.tapeFile.fSeq = 1;
      volInfo.vid= label;
      //Label
      castor::tape::tapeFile::LabelSession *lsWithoutLbp;
      lsWithoutLbp = new castor::tape::tapeFile::LabelSession(d, label, false);
      delete lsWithoutLbp;   
      castor::tape::tapeFile::LabelSession *lsWithout;
      lsWithout = new castor::tape::tapeFile::LabelSession(d, label, true);
      delete lsWithout;  
    }
    virtual void TearDown() {
      
    }   
    castor::tape::tapeserver::drive::FakeDrive d;
    uint32_t block_size;
    std::string label;
    TestingRetrieveJob fileToRecall;
    TestingArchiveJob fileToMigrate;
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
  };
  
  TEST_F(castorTapeFileTest, throwsWhenReadingAnEmptyTape) {    
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo, false);
    ASSERT_NE((long int)rs, 0);
    fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
    ASSERT_THROW({castor::tape::tapeFile::ReadFile rf1(rs, fileToRecall);}, cta::exception::Exception); //cannot read a file on an empty tape
    delete rs;
  }
   
  TEST_F(castorTapeFileTest, throwsWhenUsingSessionTwice) {
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ASSERT_NO_THROW(ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true, false));
    ASSERT_EQ(ws->m_compressionEnabled, true);
    ASSERT_EQ(ws->m_vid.compare(label), 0);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
      ASSERT_NO_THROW(wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size)));
      wf->write(testString.c_str(),testString.size());      
      wf->close();
    }
    delete ws;
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo, false);
    {
      fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
      castor::tape::tapeFile::ReadFile rf1(rs, fileToRecall);
      ASSERT_THROW({castor::tape::tapeFile::ReadFile rf2(rs, fileToRecall);},castor::tape::tapeFile::SessionAlreadyInUse); //cannot have two ReadFile's on the same session
    }
    delete rs;
  }
  
  TEST_F(castorTapeFileTest, throwsWhenWritingAnEmptyFileOrSessionCorrupted) {
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true, false);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
      ASSERT_NO_THROW(wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size)));
      ASSERT_THROW(wf->close(), castor::tape::tapeFile::ZeroFileWritten);
    }
    ASSERT_EQ(ws->isCorrupted(), true);
    {
      ASSERT_THROW(castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);, castor::tape::tapeFile::SessionCorrupted);
    }
    delete ws;
  }
  
  TEST_F(castorTapeFileTest, throwsWhenClosingTwice) {
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true, false);
    {
      std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
      ASSERT_NO_THROW(wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size)));
      wf->write(testString.c_str(),testString.size());
      wf->close();
      ASSERT_THROW(wf->close(), castor::tape::tapeFile::FileClosedTwice);
    }
    delete ws;
  }
  
  TEST_F(castorTapeFileTest, throwsWhenWrongBlockSizeOrEOF) {
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true, false);
    {
      std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
      ASSERT_NO_THROW(wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size)));
      wf->write(testString.c_str(),testString.size());
      wf->close();
    }
    delete ws;
    
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo, false);
    {
      fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
      castor::tape::tapeFile::ReadFile rf(rs, fileToRecall);
      size_t bs = rf.getBlockSize();
      char *data = new char[bs+1];
      ASSERT_THROW(rf.read(data, 1), castor::tape::tapeFile::WrongBlockSize); //block size needs to be the same provided by the headers
      ASSERT_THROW({while(true) {rf.read(data, bs);}}, castor::tape::tapeFile::EndOfFile); //it is normal to reach end of file after a loop of reads
      delete[] data;
    }
    delete rs;
  }
  
  TEST_F(castorTapeFileTest, canProperlyVerifyLabelWriteAndReadTape) {    
    //Verify label
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo, false);
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::tapeFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare(label), 0);
    delete rs;
    
    //Write AULFile with Hello World
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true, true);
    ASSERT_EQ(ws->m_compressionEnabled, true);
    ASSERT_EQ(ws->m_useLbp, true);
    ASSERT_EQ(ws->m_vid.compare(label), 0);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
      ASSERT_NO_THROW(wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size)));
      wf->write(testString.c_str(),testString.size());      
      wf->close();
    }
    delete ws;
    
    //Read it back and compare
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo, true);
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::tapeFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare(label), 0);
    ASSERT_EQ(rs->m_useLbp, true);
    {
      fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;
      castor::tape::tapeFile::ReadFile rf(rs, fileToRecall);
      size_t bs = rf.getBlockSize();
      ASSERT_EQ(bs, block_size);
      char *data = new char[bs+1];
      size_t bytes_read = rf.read(data, bs);
      data[bytes_read] = '\0';
      ASSERT_EQ(bytes_read, (size_t)testString.size());
      ASSERT_EQ(testString.compare(data),0);
      delete[] data;
    }
    delete rs;
  }
  
  TEST_F(castorTapeFileTest, tapeSessionThrowsOnWrongSequence) {
    castor::tape::tapeFile::WriteSession ws(d, volInfo, 0, true, false);
    EXPECT_NO_THROW(ws.validateNextFSeq(1));
    EXPECT_THROW(ws.reportWrittenFSeq(2),cta::exception::Exception);
    EXPECT_NO_THROW(ws.reportWrittenFSeq(1));
    EXPECT_NO_THROW(ws.validateNextFSeq(2));
    EXPECT_THROW(ws.validateNextFSeq(1), cta::exception::Exception);
  }
 
  // Class creating a temporary file of 1kB and deleting it 
  // automatically
  class TempFile {
  public:
    TempFile() {
      char path[]="/tmp/testCTA-XXXXXX";
      int fd=::mkstemp(path);
      cta::exception::Errnum::throwOnMinusOne(fd, "In TempFile::TempFile: failed to mkstemp: ");
      ::close(fd);
      m_path=path;
    }
    TempFile(const std::string& path): m_path(path) {}
    std::string path() { return m_path; }
    void randomFill(size_t size) {
      std::ofstream out(m_path,std::ios::out | std::ios::binary);
      std::ifstream in("/dev/urandom", std::ios::in|std::ios::binary);
      std::unique_ptr<char[]> buff(new char[size]);
      in.read(buff.get(),size);
      out.write(buff.get(),size);
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
    char data1[block_size];
    char data2[block_size];
    cta::disk::RadosStriperPool striperPool;
    cta::disk::DiskFileFactory fileFactory("", 0, striperPool);
    TempFile sourceFile;
    sourceFile.randomFill(1000);
    TempFile destinationFile(sourceFile.path()+"_dst");
    // host part of file location
    std::string lh = "localhost:";
    {
      std::unique_ptr<cta::disk::ReadFile> rf(
        fileFactory.createReadFile(lh + sourceFile.path()));
      std::unique_ptr<cta::disk::WriteFile> wf(
        fileFactory.createWriteFile(lh + destinationFile.path()));
      size_t res=0;
      do {
        res = rf->read(data1, block_size);
        wf->write(data1, res);
      } while(res);
      wf->close();
    }
    std::unique_ptr<cta::disk::ReadFile> src(
        fileFactory.createReadFile(sourceFile.path()));
    std::unique_ptr<cta::disk::ReadFile> dst(
        fileFactory.createReadFile(destinationFile.path()));
    size_t res1=0;
    size_t res2=0;
    do {
      res1 = src->read(data1, block_size);
      res2 = dst->read(data2, block_size);
      ASSERT_EQ(res1, res2);
      ASSERT_EQ(strncmp(data1, data2, res1), 0);
    } while(res1 || res2);
  }
  
  TEST(ctaDirectoryTests, directoryExist) {
    cta::disk::LocalDirectory dir("/tmp/");
    ASSERT_TRUE(dir.exist());
    
    cta::disk::LocalDirectory dirNotExist("/AZERTY/");
    ASSERT_FALSE(dirNotExist.exist());
  }
  
   TEST(ctaDirectoryTests, directoryCreate){
     const char * dirTestPath = "/tmp/testDir";
     ::rmdir(dirTestPath);
     cta::disk::LocalDirectory dir(dirTestPath);
     ASSERT_NO_THROW(dir.mkdir());
     ::rmdir(dirTestPath);
   }
   
   TEST(ctaDirectoryTests, directoryFailCreate){
     const char * dirTestPath = "//WRONG/PATH";
     cta::disk::LocalDirectory dir(dirTestPath);
     ASSERT_THROW(dir.mkdir(),cta::exception::Errnum);
   }
   
   TEST(ctaDirectoryTests, directoryGetFilesName){
     std::string dirTestPath = "/tmp/directoryGetFilesNames";
     std::string rmCommand = "rm -rf "+dirTestPath;
     ::system(rmCommand.c_str());
     cta::disk::LocalDirectory dir(dirTestPath);
     ASSERT_NO_THROW(dir.mkdir());
     char filePath[] = "/tmp/directoryGetFilesNames/fileXXXXXX";
     int fd = ::mkstemp(filePath);
     cta::exception::Errnum::throwOnMinusOne(fd,"In directoryGetFilesName, fail mkstemp");
     ::close(fd);
     ASSERT_EQ(1,dir.getFilesName().size());
     ::unlink(filePath);
   }
}
