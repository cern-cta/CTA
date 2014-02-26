/******************************************************************************
 *                      FileTest.cpp
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

#include <gtest/gtest.h>
#include "../system/Wrapper.hpp"
#include "../SCSI/Device.hpp"
#include "../drive/Drive.hpp"
#include "File.hpp"

namespace UnitTests {  
  TEST(castorTapeAULFile, canProperlyLabelWriteAndReadTape) {
    castor::tape::drives::FakeDrive d;
    const uint32_t block_size = 1024;
    castor::tape::AULFile::LabelSession *ls;
    ls = new castor::tape::AULFile::LabelSession(d, "K00001", true);
    ASSERT_NE((long int)ls, 0);
    delete ls;
    castor::tape::AULFile::ReadSession *rs;
    rs = new castor::tape::AULFile::ReadSession(d, "K00001");
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::AULFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare("K00001"), 0);
    delete rs;
    castor::tape::AULFile::WriteSession *ws;
    ws = new castor::tape::AULFile::WriteSession(d, "K00001", 0, true);
    ASSERT_EQ(ws->m_compressionEnabled, true);
    ASSERT_EQ(ws->m_vid.compare("K00001"), 0);
    ASSERT_EQ(ws->isCorrupted(), false);
    castor::tape::AULFile::FileInfo fileInfo;
    fileInfo.blockId=0;
    fileInfo.checksum=43567;
    fileInfo.fseq=1;
    fileInfo.nsFileId=1;
    fileInfo.size=500;
    {
      castor::tape::AULFile::WriteFile wf(ws, fileInfo, block_size);
      std::string data = "Hello World!";
      wf.write(data.c_str(),data.size());      
      wf.close();
    }
    delete ws;
    rs = new castor::tape::AULFile::ReadSession(d, "K00001");
    ASSERT_NE((long int)rs, 0);
    ASSERT_EQ(rs->getCurrentFilePart(), castor::tape::AULFile::Header);
    ASSERT_EQ(rs->getCurrentFseq(), (uint32_t)1);
    ASSERT_EQ(rs->isCorrupted(), false);
    ASSERT_EQ(rs->m_vid.compare("K00001"), 0);
    {      
      std::string hw = "Hello World!";
      castor::tape::AULFile::ReadFile rf(rs, fileInfo, castor::tape::AULFile::ByBlockId);
      size_t bs = rf.getBlockSize();
      ASSERT_EQ(bs, block_size);
      char *data = new char[bs+1];
      size_t bytes_read = rf.read(data, bs);
      data[bytes_read] = '\0';
      ASSERT_EQ(bytes_read, (size_t)hw.size());
      ASSERT_EQ(hw.compare(data),0);
      delete[] data;
    }
    delete rs;
  }
}
