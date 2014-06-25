#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/legacymsg/RmcProxyDummy.hpp"
#include "castor/legacymsg/VmgrProxyDummy.hpp"
#include "castor/legacymsg/VdqmProxyDummy.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"
#include "castor/utils/ProcessCapDummy.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests
{
using namespace castor::tape::tapeserver::daemon;
using namespace castor::tape;
const int blockSize=4096;

class FakeDiskWriteThreadPool: public DiskWriteThreadPool
{
public:
  using DiskWriteThreadPool::m_tasks;
  FakeDiskWriteThreadPool(castor::log::LogContext & lc):
    DiskWriteThreadPool(1,*((RecallReportPacker*)NULL), lc){}
  virtual ~FakeDiskWriteThreadPool() {};
};
     
class FakeSingleTapeReadThread : public TapeSingleThreadInterface<TapeReadTask>
{
public:
  using TapeSingleThreadInterface<TapeReadTask>::m_tasks;
  
  FakeSingleTapeReadThread(castor::tape::drives::DriveInterface& drive,
    castor::legacymsg::RmcProxy & rmc,
    castor::tape::tapeserver::daemon::TapeServerReporter & tsr,
    const castor::tape::tapeserver::client::ClientInterface::VolumeInfo& volInfo, 
     castor::utils::ProcessCap& cap,
    castor::log::LogContext & lc):
  TapeSingleThreadInterface<TapeReadTask>(drive, rmc, tsr, volInfo,cap, lc){}
  
  ~FakeSingleTapeReadThread(){
    const unsigned int size= m_tasks.size();
    for(unsigned int i=0;i<size;++i){
      delete m_tasks.pop();
    }
  }
   virtual void run () 
  {
     m_tasks.push(NULL);
  }
  virtual void push(TapeReadTask* t){
    m_tasks.push(t);
  }
};

TEST(castor_tape_tapeserver_daemon, RecallTaskInjectorNominal) {
  const int nbCalls=2;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
  castor::log::LogContext lc(log);
  RecallMemoryManager mm(50U,50U,lc);
  castor::tape::drives::FakeDrive drive;
  FakeClient client(nbCalls);
  FakeDiskWriteThreadPool diskWrite(lc);
  castor::legacymsg::RmcProxyDummy rmc;
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::tape::tapeserver::client::ClientInterface::VolumeInfo volume;
  volume.clientType=castor::tape::tapegateway::READ_TP;
  volume.density="8000GC";
  volume.labelObsolete="AUL";
  volume.vid="V12345";
  volume.volumeMode=castor::tape::tapegateway::READ;
  castor::tape::tapeserver::daemon::TapeServerReporter gsr(initialProcess,
  utils::DriveConfig(),"0.0.0.0",volume,lc);
  castor::utils::ProcessCapDummy cap;
  FakeSingleTapeReadThread tapeRead(drive, rmc, gsr, volume, cap,lc);

  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  tapeserver::daemon::RecallTaskInjector rti(mm,tapeRead,diskWrite,client,6,blockSize,lc);
  
  ASSERT_EQ(true,rti.synchronousInjection());
  ASSERT_EQ(nbFile,diskWrite.m_tasks.size());
  ASSERT_EQ(nbFile,tapeRead.m_tasks.size());
  
  rti.startThreads();
  rti.requestInjection(false);
  rti.requestInjection(true);
  rti.finish();
  rti.waitThreads();
  
  //pushed nbFile*2 files + 1 end of work
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
    
    //static_cast is needed otherwise compilation fails on SL5 with a raw NULL
    ASSERT_EQ(static_cast<DiskWriteTask*>(NULL),diskWriteTask);
    ASSERT_EQ(static_cast<TapeReadTask*>(NULL),tapeReadTask);
    delete diskWriteTask;
    delete tapeReadTask;
  }
}
TEST(castor_tape_tapeserver_daemon, RecallTaskInjectorNoFiles) {
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
  castor::log::LogContext lc(log);
  RecallMemoryManager mm(50U,50U,lc);
  castor::tape::drives::FakeDrive drive;
  FakeClient client(0);
  FakeDiskWriteThreadPool diskWrite(lc);
  castor::legacymsg::RmcProxyDummy rmc;
  castor::legacymsg::VmgrProxyDummy vmgr;
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::messages::TapeserverProxyDummy initialProcess;  
  castor::tape::tapeserver::client::ClientInterface::VolumeInfo volume;
  volume.clientType=castor::tape::tapegateway::READ_TP;
  volume.density="8000GC";
  volume.labelObsolete="AUL";
  volume.vid="V12345";
  volume.volumeMode=castor::tape::tapegateway::READ;
  castor::utils::ProcessCapDummy cap;
  castor::tape::tapeserver::daemon::TapeServerReporter tsr(initialProcess,  
  utils::DriveConfig(),"0.0.0.0",volume,lc);  
  FakeSingleTapeReadThread tapeRead(drive, rmc, tsr, volume,cap, lc);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  tapeserver::daemon::RecallTaskInjector rti(mm,tapeRead,diskWrite,client,6,blockSize,lc);
  
  ASSERT_EQ(false,rti.synchronousInjection());
  ASSERT_EQ(0U,diskWrite.m_tasks.size());
  ASSERT_EQ(0U,tapeRead.m_tasks.size());
}
}
