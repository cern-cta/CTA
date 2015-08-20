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
#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/StringLogger.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <cmath>
#include <ext/stdio_filebuf.h>

namespace unitTests{
  using namespace castor::tape::tapeserver::daemon;

  class TestingArchiveJob: public cta::ArchiveJob {
  public:
    TestingArchiveJob() {
    }
  }; 

  class FakeTapeWriteTask : public  DataConsumer{
    castor::server::BlockingQueue<MemBlock*> fifo;
    unsigned long m_checksum;
  public:
    FakeTapeWriteTask():m_checksum(Payload::zeroAdler32()){
      
    }
    virtual MemBlock * getFreeBlock() {
      return fifo.pop();
    }
  
    virtual void pushDataBlock(MemBlock *mb) {
      m_checksum = mb->m_payload.adler32(m_checksum);
      fifo.push(mb);
    }
    unsigned long getChecksum() {
      return m_checksum;
    }
  };
 
  template <class T> unsigned long mycopy(T& out,int size){
    std::vector<char> v(size);
    unsigned long checksum = Payload::zeroAdler32();
    std::ifstream in("/dev/urandom", std::ios::in|std::ios::binary);
    in.read(&v[0],size);
    checksum = adler32(checksum,reinterpret_cast<unsigned char*>(&v[0]),size);
    out.write(&v[0],size);
//    out.seekg(0, std::ios::beg);
    return checksum;
  }

// This test was commented out in the CMakeLists.txt file that would have
// compiled it.  There was no comment explaining why it was commented out.
// The following commit message was associated with the commenting out of the
// CMakeLists.txt line:
//
//   commit 165117042951769b7d0023933cc6e705b04fad54
//   Author: David COME <david.come@cern.ch>
//   Date:   Tue Apr 22 11:35:11 2014 +0200
//
//      Commented out DiskReadTaskTest
//
// This test is commented and left here in the hope that somebody in the future
// will have the time to make this test compile again.
//
//TEST(castor_tape_tapeserver_daemon, DiskReadTaskTest){
//  char path[]="/tmp/testDRT-XXXXXX";
//  mkstemp(path);
//  std::ofstream out(path,std::ios::out | std::ios::binary);
//  castor::server::AtomicFlag flag;
//  castor::log::StringLogger log("castor_tape_tapeserver_daemon_DiskReadTaskTest");
//  castor::log::LogContext lc(log);
//  
//  const int blockSize=1500;    
//  const int fileSize(1024*2000);
//  
//  const unsigned long original_checksum = mycopy(out,fileSize);
//  castor::tape::tapeserver::daemon::MigrationMemoryManager mm(1,blockSize,lc);
//  
//  TestingArchiveJob file;
//  
//  file.archiveFile.lastKnownPath = path;
//  file.archiveFile.size = fileSize;
//  
//  const int blockNeeded=fileSize/mm.blockCapacity()+((fileSize%mm.blockCapacity()==0) ? 0 : 1);
//  int value=std::ceil(1024*2000./blockSize);
//  ASSERT_EQ(value,blockNeeded);
//  
//  FakeTapeWriteTask ftwt;
//  ftwt.pushDataBlock(new MemBlock(1,blockSize));
//  castor::tape::tapeserver::daemon::DiskReadTask drt(ftwt,&file,blockNeeded,flag);
//  drt.execute(lc);
//  
//  ASSERT_EQ(original_checksum,ftwt.getChecksum());
//  delete ftwt.getFreeBlock();
//}
}
