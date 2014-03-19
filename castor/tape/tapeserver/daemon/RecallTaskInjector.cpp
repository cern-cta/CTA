#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include <stdint.h>

using castor::log::LogContext;
using castor::log::Param;

namespace{
  uint32_t blockID(const castor::tape::tapegateway::FileToRecallStruct& ftr)
  {
    return (ftr.blockId0() << 24) | (ftr.blockId1() << 16) |  (ftr.blockId2() << 8) | ftr.blockId3();
  }
}

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(MemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskThreadPoolInterface<DiskWriteTask> & diskWriter,
        ClientInterface& client,castor::log::LogContext& lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_client(client),m_lc(lc) 
{}

void RecallTaskInjector::requestInjection(int maxFiles, int byteSizeThreshold, bool lastCall) {
  //@TODO where shall we  acquire the lock ? There of just before the push ?
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
  
  LogContext::ScopedParam sp01(m_lc, Param("maxFiles", maxFiles));
  LogContext::ScopedParam sp02(m_lc, Param("byteSizeThreshold",byteSizeThreshold));
  LogContext::ScopedParam sp03(m_lc, Param("lastCall", lastCall));

  m_lc.log(LOG_INFO,"Request more jobs");
  m_queue.push(Request(maxFiles, byteSizeThreshold, lastCall));
}

void RecallTaskInjector::waitThreads() {
  m_thread.wait();
}

void RecallTaskInjector::startThreads() {
  m_thread.start();
}

void RecallTaskInjector::injectBulkRecalls(const std::vector<castor::tape::tapegateway::FileToRecallStruct*>& jobs) {
  for (std::vector<tapegateway::FileToRecallStruct*>::const_iterator it = jobs.begin(); it != jobs.end(); ++it) {

    LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->id())),
      LogContext::ScopedParam(m_lc, Param("NSFILEID2", (*it)->fileid())),
      LogContext::ScopedParam(m_lc, Param("NSFILESEQNUMBER", (*it)->fseq())),
      LogContext::ScopedParam(m_lc, Param("NSFILEBLOCKID", blockID(**it))),
      LogContext::ScopedParam(m_lc, Param("NSFILENSHOST", (*it)->nshost())),
      LogContext::ScopedParam(m_lc, Param("NSFILEPATH", (*it)->path()))
    };
    suppresUnusedVariable(sp[0]);
    
    m_lc.log(LOG_INFO, "Logged file to recall");
    
    DiskWriteFileTask * dwt = new DiskWriteFileTask((*it)->id(), blockID(**it), m_memManager);
    TapeReadFileTask * trt = new TapeReadFileTask(*dwt, (*it)->fseq(), blockID(**it));
    
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
  }
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", jobs.size()));
  m_lc.log(LOG_INFO, "Tasks for recalling injected");
}

bool RecallTaskInjector::synchronousInjection(uint64_t maxFiles, uint64_t byteSizeThreshold)
{
  ClientProxy::RequestReport reqReport;  

  std::auto_ptr<castor::tape::tapegateway::FilesToRecallList> filesToRecallList(m_client.getFilesToRecall(maxFiles,byteSizeThreshold,reqReport));
  LogContext::ScopedParam sp[]={
    LogContext::ScopedParam(m_lc, Param("maxFiles", maxFiles)),
    LogContext::ScopedParam(m_lc, Param("byteSizeThreshold",byteSizeThreshold)),
    LogContext::ScopedParam(m_lc, Param("transactionId", reqReport.transactionId)),
    LogContext::ScopedParam(m_lc, Param("connectDuration", reqReport.connectDuration)),
    LogContext::ScopedParam(m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration))
  };
  suppresUnusedVariable(sp);
  
  if(NULL==filesToRecallList.get()) { 
    m_lc.log(LOG_ERR, "Get called but no files to retrieve");
    return false;
  }
  else {
    std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
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
        ClientProxy::RequestReport reqReport;
        std::auto_ptr<tapegateway::FilesToRecallList> filesToRecallList(_this.m_client.getFilesToRecall(req.nbMaxFiles, req.byteSizeThreshold,reqReport));
        
        LogContext::ScopedParam sp01(_this.m_lc, Param("transactionId", reqReport.transactionId));
        LogContext::ScopedParam sp02(_this.m_lc, Param("connectDuration", reqReport.connectDuration));
        LogContext::ScopedParam sp03(_this.m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
  
        if (NULL == filesToRecallList.get()) {
          if (req.lastCall) {
	    _this.m_lc.log(LOG_INFO,"No more file to recall: triggering the end of session.\n");
            _this.m_tapeReader.finish();
            _this.m_diskWriter.finish();
            break;
          } else {
	    printf("In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.\n");
	  }
        } else {
          std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
          _this.injectBulkRecalls(jobs);
        }
      } // end of while(1)
      //-------------
  
     /* We want to finish at the first lastCall we encounter.
      * But even after sending finish() to m_diskWriter and to m_tapeReader,
        m_diskWriter might still want some more task (the threshold could be crossed),
      *  so we discard everything that might still be in the queue
      */
      try {
        while(1) {
          Request req = _this.m_queue.tryPop();
          printf("In RecallJobInjector::WorkerThread::run(): popping extra request (lastCall=%d)\n", req.lastCall);
        }
      } catch (castor::tape::threading::noMore) {
        printf("In RecallJobInjector::WorkerThread::run(): Drained the request queue. We're now empty. Finishing.\n");
      }
}

/*RecallTaskInjector::WorkerThread::WorkerThread(RecallTaskInjector & rji): _this(rji) 
{}*/

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
