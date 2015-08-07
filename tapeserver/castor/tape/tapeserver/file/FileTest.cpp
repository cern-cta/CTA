/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/file/DiskFile.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include <gtest/gtest.h>
#include <memory>

namespace UnitTests {
  
  class castorTapeFileTest : public ::testing::Test {
  protected:
    virtual void SetUp() {
      block_size = 262144;
      label = "K00001";
      fileToRecall.setBlockId0(0);
      fileToRecall.setBlockId1(0);
      fileToRecall.setBlockId2(0);
      fileToRecall.setBlockId3(0);
      fileToRecall.setFseq(1);
      fileToRecall.setFileid(1);
//      fileInfo.blockId=0;
//      fileInfo.checksum=43567;
//      fileInfo.fseq=1;
//      fileInfo.nsFileId=1;
//      fileInfo.size=500;
      fileToMigrate.setFileSize(500);
      fileToMigrate.setFileid(1);
      fileToMigrate.setFseq(1);
      volInfo.vid= label;
      //Label
      castor::tape::tapeFile::LabelSession *ls;
      ls = new castor::tape::tapeFile::LabelSession(d, label);
      delete ls;      
    }
    virtual void TearDown() {
      
    }   
    castor::tape::tapeserver::drive::FakeDrive d;
    uint32_t block_size;
    std::string label;
    castor::tape::tapegateway::FileToRecallStruct fileToRecall;
    castor::tape::tapegateway::FileToMigrateStruct fileToMigrate;
    castor::tape::tapeserver::client::ClientInterface::VolumeInfo volInfo;
  };
  
  TEST_F(castorTapeFileTest, throwsWhenReadingAnEmptyTape) {    
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo);    
    ASSERT_NE((long int)rs, 0);
    fileToRecall.setPositionCommandCode(castor::tape::tapegateway::TPPOSIT_BLKID);
    ASSERT_THROW({castor::tape::tapeFile::ReadFile rf1(rs, fileToRecall);}, castor::exception::Exception); //cannot read a file on an empty tape
    delete rs;
  }
   
  TEST_F(castorTapeFileTest, throwsWhenUsingSessionTwice) {
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true);
    ASSERT_EQ(ws->m_compressionEnabled, true);
    ASSERT_EQ(ws->m_vid.compare(label), 0);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);
      wf.write(testString.c_str(),testString.size());      
      wf.close();
    }
    delete ws;
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo);
    {
      fileToRecall.setPositionCommandCode(castor::tape::tapegateway::TPPOSIT_BLKID);
      castor::tape::tapeFile::ReadFile rf1(rs, fileToRecall);
      ASSERT_THROW({castor::tape::tapeFile::ReadFile rf2(rs, fileToRecall);},castor::tape::tapeFile::SessionAlreadyInUse); //cannot have two ReadFile's on the same session
    }
    delete rs;
  }
  
  TEST_F(castorTapeFileTest, throwsWhenWritingAnEmptyFileOrSessionCorrupted) {
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);
      ASSERT_THROW(wf.close(), castor::tape::tapeFile::ZeroFileWritten);
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
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true);
    {
      castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);
      wf.write(testString.c_str(),testString.size());
      wf.close();
      ASSERT_THROW(wf.close(), castor::tape::tapeFile::FileClosedTwice);
    }
    delete ws;
  }
  
  TEST_F(castorTapeFileTest, throwsWhenWrongBlockSizeOrEOF) {
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true);
    {
      castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);
      wf.write(testString.c_str(),testString.size());
      wf.close();
    }
    delete ws;
    
    castor::tape::tapeFile::ReadSession *rs;
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo);
    {
      fileToRecall.setPositionCommandCode(castor::tape::tapegateway::TPPOSIT_BLKID);
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
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo);
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::tapeFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare(label), 0);
    delete rs;
    
    //Write AULFile with Hello World
    const std::string testString("Hello World!");
    castor::tape::tapeFile::WriteSession *ws;
    ws = new castor::tape::tapeFile::WriteSession(d, volInfo, 0, true);
    ASSERT_EQ(ws->m_compressionEnabled, true);
    ASSERT_EQ(ws->m_vid.compare(label), 0);
    ASSERT_EQ(ws->isCorrupted(), false);
    {
      castor::tape::tapeFile::WriteFile wf(ws, fileToMigrate, block_size);
      wf.write(testString.c_str(),testString.size());      
      wf.close();
    }
    delete ws;
    
    //Read it back and compare
    rs = new castor::tape::tapeFile::ReadSession(d, volInfo);
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::tapeFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare(label), 0);
    {
      fileToRecall.setPositionCommandCode(castor::tape::tapegateway::TPPOSIT_BLKID);
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
    castor::tape::tapeFile::WriteSession ws(d, volInfo, 0, true);
    EXPECT_NO_THROW(ws.validateNextFSeq(1));
    EXPECT_THROW(ws.reportWrittenFSeq(2),castor::exception::Exception);
    EXPECT_NO_THROW(ws.reportWrittenFSeq(1));
    EXPECT_NO_THROW(ws.validateNextFSeq(2));
    EXPECT_THROW(ws.validateNextFSeq(1), castor::exception::Exception);
  }
    
  TEST(castorTapeDiskFile, canWriteAndReadDisk) {
    const uint32_t block_size = 1024;
    char data1[block_size];
    char data2[block_size];
    castor::tape::diskFile::DiskFileFactory fileFactory("RFIO","",0);
    {
      std::unique_ptr<castor::tape::diskFile::ReadFile> rf(
        fileFactory.createReadFile("localhost:/etc/fstab"));
      std::unique_ptr<castor::tape::diskFile::WriteFile> wf(
        fileFactory.createWriteFile("localhost:/tmp/fstab"));
      size_t res=0;
      do {
        res = rf->read(data1, block_size);
        wf->write(data1, res);
      } while(res);
      wf->close();
    }
    std::unique_ptr<castor::tape::diskFile::ReadFile> src(
        fileFactory.createReadFile("localhost:/tmp/fstab"));
    std::unique_ptr<castor::tape::diskFile::ReadFile> dst(
        fileFactory.createReadFile("localhost:/etc/fstab"));
    size_t res1=0;
    size_t res2=0;
    do {
      res1 = src->read(data1, block_size);
      res2 = dst->read(data2, block_size);
      ASSERT_EQ(res1, res2);
      ASSERT_EQ(strncmp(data1, data2, res1), 0);
    } while(res1 || res2);
  }
}
