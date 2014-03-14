#include "RecallTaskInjector.hpp"
#include <stdint.h>

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

void RecallTaskInjector::requestInjection(int maxFiles, int maxBlocks, bool lastCall) {
  printf("RecallJobInjector::requestInjection()\n");
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
          m_queue.push(Request(maxFiles, maxBlocks, lastCall));
}

void RecallTaskInjector::waitThreads() {
  m_thread.wait();
}

void RecallTaskInjector::startThreads() {
  m_thread.start();
}

void RecallTaskInjector::injectBulkRecalls(const std::list<RecallJob>& jobs) {
  for (std::list<RecallJob>::const_iterator i = jobs.begin(); i != jobs.end(); ++i) {
    DiskWriteFileTask * dwt = new DiskWriteFileTask(i->fileId, i->blockCount, m_memManager);
    TapeReadFileTask * trt = new TapeReadFileTask(*dwt, i->fSeq, i->blockCount);
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
  }
}

bool RecallTaskInjector::synchronousInjection(uint64_t maxFiles, uint64_t maxSize)
{
  ClientInterface::RequestReport requestReporter;
  std::list<RecallJob> jobs ; m_client.getFilesToRecall(maxFiles, maxSize,requestReporter);
  
  if(jobs.empty()) {
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
      while (1) {
        Request req = _this.m_queue.pop();
	printf("RecallJobInjector:run: about to call client interface\n");
        ClientInterface::RequestReport requestReporter;
        std::list<RecallJob> jobs ; _this.m_client.getFilesToRecall(req.maxFiles, req.maxBlocks,requestReporter);
        if (jobs.empty()) {
          if (req.lastCall) {
	    printf("In RecallJobInjector::WorkerThread::run(): last call turned up empty: triggering the end of session.\n");
            _this.m_tapeReader.finish();
            _this.m_diskWriter.finish();
            break;
          } else {
	    printf("In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.\n");
	  }
        } else {
	  printf("In RecallJobInjector::WorkerThread::run(): got %zd files.\n", jobs.size());
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
