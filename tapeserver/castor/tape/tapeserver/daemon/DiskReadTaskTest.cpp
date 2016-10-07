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
#include "castor/tape/tapeserver/file/RadosStriperPool.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "scheduler/ArchiveMount.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <cmath>
#include <ext/stdio_filebuf.h>

namespace unitTests{
  
  class TestingArchiveJob: public cta::ArchiveJob {
  public:
    TestingArchiveJob(): cta::ArchiveJob(*((cta::ArchiveMount *)NULL), 
        *((cta::catalogue::Catalogue *)NULL), cta::common::dataStructures::ArchiveFile(), 
        "", cta::common::dataStructures::TapeFile()) {
    }
  };
  
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::diskFile;
  
  struct MockMigrationReportPacker : public MigrationReportPacker {
    void reportCompletedJob(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob) {}
    void reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, const cta::exception::Exception& ex) {}
    void reportEndOfSession() {}
    void reportEndOfSessionWithErrors(const std::string msg, int error_code) {}
    void disableBulk() {}
    MockMigrationReportPacker(cta::ArchiveMount *rm,cta::log::LogContext lc):
      MigrationReportPacker(rm,lc) {}
  };
  
  class FakeTapeWriteTask : public  DataConsumer{
    cta::threading::BlockingQueue<MemBlock*> fifo;
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
  
  class MockMigrationWatchDog: public MigrationWatchDog {
  public:
    MockMigrationWatchDog(double periodToReport,double stuckPeriod,
    cta::tape::daemon::TapedProxy& initialProcess,
    const std::string & driveUnitName,
    cta::log::LogContext lc, double pollPeriod = 0.1): 
      MigrationWatchDog(periodToReport, stuckPeriod, initialProcess, 
          driveUnitName, lc, pollPeriod) {}
  private:

    virtual void logStuckFile() {}

    virtual void run() {}

  };
  
  TEST(castor_tape_tapeserver_daemon, DiskReadTaskTest){
    char path[]="/tmp/testDRT-XXXXXX";
    ::close(::mkstemp(path));
    std::string url("file://");
    url += path;
    std::ofstream out(path,std::ios::out | std::ios::binary);
    cta::threading::AtomicFlag flag;
    cta::log::StringLogger log("castor_tape_tapeserver_daemon_DiskReadTaskTest",cta::log::DEBUG);
    cta::log::LogContext lc(log);

    const int blockSize=1500;    
    const int fileSize(1024*2000);

    const unsigned long original_checksum = mycopy(out,fileSize);
    castor::tape::tapeserver::daemon::MigrationMemoryManager mm(1,blockSize,lc);

    TestingArchiveJob file;

    file.archiveFile.fileSize = fileSize;
    file.srcURL = url;

    const int blockNeeded=fileSize/mm.blockCapacity()+((fileSize%mm.blockCapacity()==0) ? 0 : 1);
    int value=std::ceil(1024*2000./blockSize);
    ASSERT_EQ(value,blockNeeded);

    FakeTapeWriteTask ftwt;
    ftwt.pushDataBlock(new MemBlock(1,blockSize));
    castor::tape::tapeserver::daemon::DiskReadTask drt(ftwt,&file,blockNeeded,flag);
    castor::tape::file::RadosStriperPool striperPool;
    DiskFileFactory fileFactory("RFIO", "", 0, striperPool);

    castor::messages::TapeserverProxyDummy tspd;
    MockMigrationWatchDog mmwd(1.0, 1.0, tspd, "", lc);
    drt.execute(lc,fileFactory,mmwd, 0);

    ASSERT_EQ(original_checksum,ftwt.getChecksum());
    delete ftwt.getFreeBlock();
  }
}
