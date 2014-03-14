#include "RecallTaskInjector.hpp"
#include "castor/log/LogContext.hpp"
#include <stdint.h>

using castor::log::LogContext;
using castor::log::Param;

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(MemoryManager & mm, 
        TapeReadSingleThread & tapeReader,DiskWriteThreadPool & diskWriter,
        ClientInterface& client,castor::log::LogContext& lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_client(client),m_lc(lc) 
{}

void RecallTaskInjector::requestInjection(int maxFiles, int byteSizeThreshold, bool lastCall) {
  
  LogContext::ScopedParam sp01(m_lc, Param("maxFiles", maxFiles));
  LogContext::ScopedParam sp02(m_lc, Param("byteSizeThreshold",byteSizeThreshold));
  LogContext::ScopedParam sp03(m_lc, Param("lastCall", lastCall));

  m_lc.log(LOG_INFO,"Request more jobs");
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
          m_queue.push(Request(maxFiles, byteSizeThreshold, lastCall));
}

void RecallTaskInjector::waitThreads() {
  m_thread.wait();
}

void RecallTaskInjector::startThreads() {
  m_thread.start();
}

void RecallTaskInjector::injectBulkRecalls(const std::list<RecallJob>& jobs) {
  for (std::list<RecallJob>::const_iterator it = jobs.begin(); it != jobs.end(); ++it) {
    
    LogContext::ScopedParam sp(m_lc, Param("NSFILEID", it->fileId));
    LogContext::ScopedParam sp2(m_lc, Param("NSFILESEQNUMBER", it->fSeq));
    m_lc.log(LOG_INFO, "Logged file to recall");
    
    DiskWriteFileTask * dwt = new DiskWriteFileTask(it->fileId, it->blockCount, m_memManager);
    TapeReadFileTask * trt = new TapeReadFileTask(*dwt, it->fSeq, it->blockCount);
    
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
  }
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", jobs.size()));
}

bool RecallTaskInjector::synchronousInjection(uint64_t maxFiles, uint64_t byteSizeThreshold)
{
  ClientInterface::RequestReport reqReport;  
  //@TODO interface TBD   
  LogContext::ScopedParam sp(m_lc, Param("maxFiles", maxFiles));
  LogContext::ScopedParam sp0(m_lc, Param("byteSizeThreshold",byteSizeThreshold));

  std::list<RecallJob> jobs ;/*=*/ m_client.getFilesToRecall(maxFiles, byteSizeThreshold,reqReport);

  LogContext::ScopedParam sp01(m_lc, Param("transactionId", reqReport.transactionId));
  LogContext::ScopedParam sp02(m_lc, Param("connectDuration", reqReport.connectDuration));
  LogContext::ScopedParam sp03(m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration));

  if(jobs.empty()) {
    m_lc.log(LOG_ERR, "Get called but no files to retrieve");
    return false;
  }
  else {
    injectBulkRecalls(jobs);
    return true;
  }
}

//--------------------------------------
void RecallTaskInjector::WorkerThread::run()
{
  using castor::log::LogContext;
  
      while (1) {
        Request req = _this.m_queue.pop();
	printf("RecallJobInjector:run: about to call client interface\n");
        ClientInterface::RequestReport reqReport;
        std::list<RecallJob> jobs ; /*=*/ _this.m_client.getFilesToRecall(req.nbMaxFiles, req.byteSizeThreshold,reqReport);
        
        LogContext::ScopedParam sp01(_this.m_lc, Param("transactionId", reqReport.transactionId));
        LogContext::ScopedParam sp02(_this.m_lc, Param("connectDuration", reqReport.connectDuration));
        LogContext::ScopedParam sp03(_this.m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
  
        if (jobs.empty()) {
          if (req.lastCall) {
	    _this.m_lc.log(LOG_INFO,"No more file to recall: triggering the end of session.\n");
            _this.m_tapeReader.finish();
            _this.m_diskWriter.finish();
            break;
          } else {
	    printf("In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.\n");
	  }
        } else {
          _this.injectBulkRecalls(jobs);
        }
      } // end of while(1)
      //-------------
      try {
        while(1) {
          Request req = _this.m_queue.tryPop();
          printf("In RecallJobInjector::WorkerThread::run(): popping extra request (lastCall=%d)\n", req.lastCall);
        }
      } catch (castor::tape::threading::noMore) {
        printf("In RecallJobInjector::WorkerThread::run(): Drained the request queue. We're now empty. Finishing.\n");
      }
}

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
