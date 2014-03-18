#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/ClientInterface.hpp"
#include "castor/tape/tapeserver/daemon/DiskThreadPoolInterface.hpp"
#include "castor/log/StringLogger.hpp"
using namespace castor::tape;
namespace
{
  
class FakeClient : public tapeserver::daemon::ClientInterface
{
  virtual tapegateway::FilesToRecallList * getFilesToRecall(uint64_t files,
  uint64_t bytes, RequestReport &report)
  throw (castor::tape::Exception) 
  {
    std::auto_ptr<tapegateway::FilesToRecallList> ptr(new tapegateway::FilesToRecallList());
    ptr->filesToRecall().push_back(new tapegateway::FileToRecallStruct);
    ptr->filesToRecall().back()->setFileid(213);
    ptr->filesToRecall().back()->setBlockId0(1);
    ptr->filesToRecall().back()->setBlockId1(2);
    ptr->filesToRecall().back()->setBlockId2(3);
    ptr->filesToRecall().back()->setBlockId3(4);
    ptr->filesToRecall().back()->setFseq(255);
    ptr->filesToRecall().back()->setPath("rfio:///root/bidule");
    ptr->filesToRecall().back()->setFileTransactionId(666);
    ptr->filesToRecall().back()->setFilesToRecallList(ptr.get());
    
    report.transactionId=666;
    report.connectDuration=42;
    report.sendRecvDuration=21;
    
    return ptr.release();
  }
};

class FakeDiskWriteThreadPool : public castor::tape::tapeserver::daemon::DiskThreadPoolInterface<DiskWriteTask>
{
  virtual void push(DiskWriteTask* t){
  
  }
};
     
class FakeSingleTapeReadThread : public TapeSingleThreadInterface<TapeReadTask>
{
  virtual void push(TapeReadTask* t){
  
  }
};

TEST(castor_tape_tapeserver_daemon, RecallTaskInjectorTest) {
  MemoryManager mm(50U,50U);
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
  castor::log::LogContext lc(log);
  
  FakeClient client;
  FakeDiskWriteThreadPool diskWrite;
  FakeSingleTapeReadThread tapeRead;
          
  tapeserver::daemon::RecallTaskInjector rti(mm,tapeRead,diskWrite,client,lc);
}

}