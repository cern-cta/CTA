#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include <gtest/gtest.h>

namespace unitTests{
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::tapeserver::client;
  struct MockRecallReportPacker : public ReportPackerInterface<detail::Recall>{
    MOCK_METHOD2(reportCompletedJob,void(const FileStruct&,unsigned long));
    MOCK_METHOD3(reportFailedJob, void(const FileStruct& ,const std::string&,int));
    MOCK_METHOD0(reportEndOfSession, void());
    MOCK_METHOD2(reportEndOfSessionWithErrors, void(const std::string,int));
    MockRecallReportPacker(ClientInterface& client,castor::log::LogContext lc):
      ReportPackerInterface<detail::Recall>(client,lc){}
  };
  
  TEST(castor_tape_tapeserver_daemon, DiskWriteTaskFailledBlock){
    using ::testing::_;
    
    MockClient client;
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_DiskWriteTaskFailledBlock");
    castor::log::LogContext lc(log);
    
    MockRecallReportPacker report(client,lc);
    EXPECT_CALL(report,reportFailedJob(_,_,_));
    RecallMemoryManager mm(10,100,lc);
        
    castor::tape::tapegateway::FileToRecallStruct file;
    file.setPath("/dev/null");
    file.setFileid(0);
    DiskWriteTask t(dynamic_cast<tapegateway::FileToRecallStruct*>(file.clone()),mm);
    for(int i=0;i<6;++i){
      MemBlock* mb=mm.getFreeBlock();
      mb->m_fileid=0;
      mb->m_fileBlock=i;
      mb->m_failed = (i==5) ? true : false;
      t.pushDataBlock(mb);
    }    
    MemBlock* mb=mm.getFreeBlock();

    t.pushDataBlock(mb);
    t.pushDataBlock(NULL);
    t.execute(report,lc);
  }
}

