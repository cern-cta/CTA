#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "log.h"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include <stdint.h>

using castor::log::LogContext;
using castor::log::Param;

namespace{
  /**
   *  function to set a NULL the owning FilesToMigrateList  of a FileToMigrateStruct
   *   Indeed, a clone of a structure will only do a shallow copy (sic).
   *   Otherwise at the second destruction the object will try to remove itself 
   *   from the owning list and then boom !
   * @param ptr a pointer to an object to change
   * @return the parameter ptr 
   */
  castor::tape::tapegateway::FileToRecallStruct* removeOwningList(castor::tape::tapegateway::FileToRecallStruct* ptr){
    ptr->setFilesToRecallList(NULL);
    return ptr;
  }
}

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskWriteThreadPool & diskWriter,
        client::ClientInterface& client,
        uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_client(client),m_lc(lc),m_maxFiles(maxFiles),m_byteSizeThreshold(byteSizeThreshold)
{}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
RecallTaskInjector::~RecallTaskInjector(){
}
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------

void RecallTaskInjector::finish(){
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request());
}
//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void RecallTaskInjector::requestInjection(bool lastCall) {
  //@TODO where shall we  acquire the lock ? There of just before the push ?
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request(m_maxFiles, m_byteSizeThreshold, lastCall));
}
//------------------------------------------------------------------------------
//waitThreads
//------------------------------------------------------------------------------
void RecallTaskInjector::waitThreads() {
  m_thread.wait();
}
//------------------------------------------------------------------------------
//startThreads
//------------------------------------------------------------------------------
void RecallTaskInjector::startThreads() {
  m_thread.start();
}
//------------------------------------------------------------------------------
//injectBulkRecalls
//------------------------------------------------------------------------------
void RecallTaskInjector::injectBulkRecalls(const std::vector<castor::tape::tapegateway::FileToRecallStruct*>& jobs) {
  for (std::vector<tapegateway::FileToRecallStruct*>::const_iterator it = jobs.begin(); it != jobs.end(); ++it) {

    LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("NSHOSTNAME", (*it)->nshost())),
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->fileid())),
      LogContext::ScopedParam(m_lc, Param("fSeq", (*it)->fseq())),
      LogContext::ScopedParam(m_lc, Param("blockID", blockID(**it))),
      LogContext::ScopedParam(m_lc, Param("path", (*it)->path()))
    };
    tape::utils::suppresUnusedVariable(sp);
    
    m_lc.log(LOG_INFO, "Logged file to recall");
    
    DiskWriteTask * dwt = 
      new DiskWriteTask(
        removeOwningList(dynamic_cast<tape::tapegateway::FileToRecallStruct*>((*it)->clone())), 
        m_memManager);
    TapeReadTask * trt = 
      new TapeReadTask(
        removeOwningList(
          dynamic_cast<tape::tapegateway::FileToRecallStruct*>((*it)->clone())), 
          *dwt,
          m_memManager);
    
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
  }
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", jobs.size()));
  m_lc.log(LOG_INFO, "Tasks for recalling injected");
}
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousInjection()
{
  client::ClientProxy::RequestReport reqReport;  

  std::auto_ptr<castor::tape::tapegateway::FilesToRecallList> 
    filesToRecallList(m_client.getFilesToRecall(m_maxFiles,m_byteSizeThreshold,reqReport));
  LogContext::ScopedParam sp[]={
    LogContext::ScopedParam(m_lc, Param("maxFiles", m_maxFiles)),
    LogContext::ScopedParam(m_lc, Param("byteSizeThreshold",m_byteSizeThreshold)),
    LogContext::ScopedParam(m_lc, Param("transactionId", reqReport.transactionId)),
    LogContext::ScopedParam(m_lc, Param("connectDuration", reqReport.connectDuration)),
    LogContext::ScopedParam(m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration))
  };
  tape::utils::suppresUnusedVariable(sp);
  
  if(NULL==filesToRecallList.get()) { 
    m_lc.log(LOG_ERR, "No files to recall: empty mount");
    return false;
  }
  else {
    std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
    injectBulkRecalls(jobs);
    return true;
  }
}
//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void RecallTaskInjector::WorkerThread::run()
{
  using castor::log::LogContext;
  _this.m_lc.pushOrReplace(Param("thread", "recallTaskInjector"));
  _this.m_lc.log(LOG_DEBUG, "Starting RecallTaskInjector thread");
  
  while (1) {
    Request req = _this.m_queue.pop();
    _this.m_lc.log(LOG_INFO,"RecallJobInjector:run: about to call client interface\n");
    client::ClientProxy::RequestReport reqReport;
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
        _this.m_lc.log(LOG_INFO,"In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.\n");
      }
    } else {
      std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
      _this.injectBulkRecalls(jobs);
    }
  } // end of while(1)
  //-------------
  _this.m_lc.log(LOG_DEBUG, "Finishing RecallTaskInjector thread");
  /* We want to finish at the first lastCall we encounter.
   * But even after sending finish() to m_diskWriter and to m_tapeReader,
   * m_diskWriter might still want some more task (the threshold could be crossed),
   * so we discard everything that might still be in the queue
   */
  bool stillReading =true;
  while(stillReading) {
    Request req = _this.m_queue.pop();
    if (req.end) stillReading = false;
    LogContext::ScopedParam sp(_this.m_lc, Param("lastCall", req.lastCall));
    _this.m_lc.log(LOG_INFO,"In RecallJobInjector::WorkerThread::run(): popping extra request");
  }
}

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
