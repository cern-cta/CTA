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
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/StringLogger.hpp"
#include "scheduler/ArchiveMount.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <cmath>
#include <ext/stdio_filebuf.h>

namespace unitTests{
  
  class TestingArchiveMount: public cta::ArchiveMount {
  public:
    TestingArchiveMount(std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbrm): ArchiveMount(std::move(dbrm)) {
    }
  };
  
  class TestingArchiveJob: public cta::ArchiveJob {
  public:
    TestingArchiveJob() {
    }
  };
  
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::diskFile;
  
  struct MockMigrationReportPacker : public MigrationReportPacker {
    void reportCompletedJob(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob, u_int32_t checksum, u_int64_t size) {
      reportCompletedJob_(successfulArchiveJob, checksum, size);
    }
    
    void reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, const castor::exception::Exception &ex) {
      reportFailedJob_(failedArchiveJob, ex);
    }
    MOCK_METHOD3(reportCompletedJob_,void(std::unique_ptr<cta::ArchiveJob> &successfulArchiveJob, u_int32_t checksum, u_int64_t size));
    MOCK_METHOD2(reportFailedJob_, void(std::unique_ptr<cta::ArchiveJob> &failedArchiveJob, const castor::exception::Exception &ex));
    MOCK_METHOD0(reportEndOfSession, void());
    MOCK_METHOD2(reportEndOfSessionWithErrors, void(const std::string,int));
    MockMigrationReportPacker(cta::ArchiveMount *rm,castor::log::LogContext lc):
      MigrationReportPacker(rm,lc){}
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
    return checksum;
  }
  
  TEST(castor_tape_tapeserver_daemon, DiskReadTaskTest){
    char path[]="/tmp/testDRT-XXXXXX";
    mkstemp(path);
    std::ofstream out(path,std::ios::out | std::ios::binary);
    castor::server::AtomicFlag flag;
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_DiskReadTaskTest");
    castor::log::LogContext lc(log);

    const int blockSize=1500;    
    const int fileSize(1024*2000);

    const unsigned long original_checksum = mycopy(out,fileSize);
    castor::tape::tapeserver::daemon::MigrationMemoryManager mm(1,blockSize,lc);

    TestingArchiveJob file;

    file.archiveFile.path = path;
    file.archiveFile.size = fileSize;

    const int blockNeeded=fileSize/mm.blockCapacity()+((fileSize%mm.blockCapacity()==0) ? 0 : 1);
    int value=std::ceil(1024*2000./blockSize);
    ASSERT_EQ(value,blockNeeded);

    FakeTapeWriteTask ftwt;
    ftwt.pushDataBlock(new MemBlock(1,blockSize));
    castor::tape::tapeserver::daemon::DiskReadTask drt(ftwt,&file,blockNeeded,flag);
    DiskFileFactory fileFactory("RFIO","",0);
    drt.execute(lc,fileFactory,*((MigrationWatchDog*)NULL));

    ASSERT_EQ(original_checksum,ftwt.getChecksum());
    delete ftwt.getFreeBlock();
  }
}