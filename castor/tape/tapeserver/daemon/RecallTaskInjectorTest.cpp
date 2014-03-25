#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/ClientInterface.hpp"
#include "castor/tape/tapeserver/daemon/DiskThreadPoolInterface.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include <gtest/gtest.h>

namespace
{
using namespace castor::tape::tapeserver::daemon;
using namespace castor::tape;
const unsigned int nbFile=5;
const int blockSize=4096;

class FakeClient : public ClientInterface
{
  
public:
  FakeClient(int nbCalls):m_current(0){
    for(int n=0;n<nbCalls;++n) {
      std::auto_ptr<tapegateway::FilesToRecallList> ptr(new tapegateway::FilesToRecallList());
      for(unsigned int i=0;i<nbFile;++i)
      {
        ptr->filesToRecall().push_back(new tapegateway::FileToRecallStruct);
        ptr->filesToRecall().back()->setFileid(i);
        ptr->filesToRecall().back()->setBlockId0(1*i);
        ptr->filesToRecall().back()->setBlockId1(2*i);
        ptr->filesToRecall().back()->setBlockId2(3*i);
        ptr->filesToRecall().back()->setBlockId3(4*i);
        ptr->filesToRecall().back()->setFseq(255+i);
        ptr->filesToRecall().back()->setPath("rfio:///root/bidule");
        ptr->filesToRecall().back()->setFileTransactionId(666+i);
        ptr->filesToRecall().back()->setFilesToRecallList(ptr.get());
    }
    lists.push_back(ptr.release());
    }
    lists.push_back(NULL);
  }
  virtual void reportMigrationResults(tapegateway::FileMigrationReportList & migrationReport,
  RequestReport& report) throw (castor::tape::Exception){}
  
  virtual void reportRecallResults(tapegateway::FileRecallReportList & migrationReport,
  RequestReport& report) throw (castor::tape::Exception){}
  
  //workaround to use assertion inside a function returning something else than void
  //see http://code.google.com/p/googletest/wiki/AdvancedGuide#Assertion_Placement
  void assertion(){ASSERT_EQ(true,static_cast<unsigned int>(m_current)<lists.size());}
  
  virtual tapegateway::FilesToRecallList * getFilesToRecall(uint64_t files,
  uint64_t bytes, RequestReport &report) throw (castor::tape::Exception) 
  {
    
    report.transactionId=666;
    report.connectDuration=42;
    report.sendRecvDuration=21;
    assertion();
    return lists[m_current++];
 
  }
  
  virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
  RequestReport &transactionReport) throw (castor::tape::Exception) {};
 
  virtual void reportEndOfSession(RequestReport &report) throw (Exception) {}
private:
  std::vector<tapegateway::FilesToRecallList*> lists;
  int m_current;
};

class FakeDiskWriteThreadPool : public DiskThreadPoolInterface<DiskWriteTask>
{
public:
  using tapeserver::daemon::DiskThreadPoolInterface<DiskWriteTask>::m_tasks;
  
  virtual void finish() 
  {
    m_tasks.push(new endOfSession);
  }
  virtual void push(DiskWriteTask* t){
      m_tasks.push(t);
  }
  ~FakeDiskWriteThreadPool(){
    const unsigned int size= m_tasks.size();
    for(unsigned int i=0;i<size;++i){
      delete m_tasks.pop();
    }
  }
};
     
class FakeSingleTapeReadThread : public TapeSingleThreadInterface<TapeReadTask>
{
public:
  using TapeSingleThreadInterface<TapeReadTask>::m_tasks;
  
  FakeSingleTapeReadThread(castor::tape::drives::DriveInterface& drive):
  TapeSingleThreadInterface<TapeReadTask>(drive){}
  
  ~FakeSingleTapeReadThread(){
    const unsigned int size= m_tasks.size();
    for(unsigned int i=0;i<size;++i){
      delete m_tasks.pop();
    }
  }
   virtual void run () 
  {
     m_tasks.push(new endOfSession);
  }
  virtual void push(TapeReadTask* t){
    m_tasks.push(t);
  }
};

TEST(castor_tape_tapeserver_daemon, RecallTaskInjectorNominal) {
  MemoryManager mm(50U,50U);
  const int nbCalls=2;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
  castor::log::LogContext lc(log);
  
  castor::tape::drives::FakeDrive drive;
  FakeClient client(nbCalls);
  FakeDiskWriteThreadPool diskWrite;
  FakeSingleTapeReadThread tapeRead(drive);
          
  tapeserver::daemon::RecallTaskInjector rti(mm,tapeRead,diskWrite,client,lc);
  
  ASSERT_EQ(true,rti.synchronousInjection(6,blockSize));
  ASSERT_EQ(nbFile,diskWrite.m_tasks.size());
  ASSERT_EQ(nbFile,tapeRead.m_tasks.size());
  
  rti.startThreads();
  rti.requestInjection(6,blockSize,false);
  rti.requestInjection(6,blockSize,true);
  rti.waitThreads();
  
  //pushed nbFile*2 files + 1 end of Work task
  ASSERT_EQ(nbFile*nbCalls+1,diskWrite.m_tasks.size());
  ASSERT_EQ(nbFile*nbCalls+1,tapeRead.m_tasks.size());
  
  for(unsigned int i=0;i<nbFile*nbCalls;++i)
  {
    delete diskWrite.m_tasks.pop();
    delete tapeRead.m_tasks.pop();
  }
  
  for(int i=0;i<1;++i)
  {
    DiskWriteTask* diskWriteTask=diskWrite.m_tasks.pop();
    TapeReadTask* tapeReadTask=tapeRead.m_tasks.pop();
    
    ASSERT_EQ(diskWriteTask->endOfWork(),true);
    ASSERT_EQ(tapeReadTask->endOfWork(),true);
    
    delete diskWriteTask;
    delete tapeReadTask;
  }
}
TEST(castor_tape_tapeserver_daemon, RecallTaskInjectorNoFiles) {
  MemoryManager mm(50U,50U);
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
  castor::log::LogContext lc(log);
  
  castor::tape::drives::FakeDrive drive;
  FakeClient client(0);
  FakeDiskWriteThreadPool diskWrite;
  FakeSingleTapeReadThread tapeRead(drive);
          
  tapeserver::daemon::RecallTaskInjector rti(mm,tapeRead,diskWrite,client,lc);
  
  ASSERT_EQ(false,rti.synchronousInjection(6,blockSize));
  ASSERT_EQ(0U,diskWrite.m_tasks.size());
  ASSERT_EQ(0U,tapeRead.m_tasks.size());
}
}
