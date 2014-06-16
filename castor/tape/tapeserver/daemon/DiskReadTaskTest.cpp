#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/StringLogger.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <cmath>
#include <ext/stdio_filebuf.h>

namespace unitTests{
  using namespace castor::tape::tapeserver::daemon;
  class FakeTapeWriteTask : public  DataConsumer{
    castor::tape::threading::BlockingQueue<MemBlock*> fifo;
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
  TEST(castor_tape_tapeserver_daemon, DiskReadTaskTest){
    char path[]="/tmp/testDRT-XXXXXX";
    mkstemp(path);
    std::ofstream out(path,std::ios::out | std::ios::binary);
    castor::tape::threading::AtomicFlag flag;
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_DiskReadTaskTest");
    castor::log::LogContext lc(log);
    
    const int blockSize=1500;    
    const int fileSize(1024*2000);
    
    const unsigned long original_checksum = mycopy(out,fileSize);
    castor::tape::tapeserver::daemon::MigrationMemoryManager mm(1,blockSize,lc);
    
    castor::tape::tapegateway::FileToMigrateStruct* file=new castor::tape::tapegateway::FileToMigrateStruct();
    
    file->setPath(path);
    file->setFileSize(fileSize);
    
    const int blockNeeded=fileSize/mm.blockCapacity()+((fileSize%mm.blockCapacity()==0) ? 0 : 1);
    int value=std::ceil(1024*2000./blockSize);
    ASSERT_EQ(value,blockNeeded);
    
    FakeTapeWriteTask ftwt;
    ftwt.pushDataBlock(new MemBlock(1,blockSize));
    castor::tape::tapeserver::daemon::DiskReadTask drt(ftwt,file,blockNeeded,flag);
    drt.execute(lc);
    
    ASSERT_EQ(original_checksum,ftwt.getChecksum());
    delete ftwt.getFreeBlock();
  }
}
