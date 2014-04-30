#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "log.h"
#include <memory>
#include <sstream>
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {


  DiskWriteThreadPool::DiskWriteThreadPool(int nbThread, int maxFilesReq, int maxBlocksReq,
          ReportPackerInterface<detail::Recall>& report,castor::log::LogContext lc):
          m_maxFilesReq(maxFilesReq), m_maxBytesReq(maxBlocksReq),
          m_reporter(report),m_lc(lc)
  {
    m_lc.pushOrReplace(castor::log::Param("threadCount", nbThread));
    m_lc.log(LOG_INFO, "Creating threads in DiskWriteThreadPool::DiskWriteThreadPool");
    for(int i=0; i<nbThread; i++) {
      DiskWriteWorkerThread * thr = new DiskWriteWorkerThread(*this);
      m_threads.push_back(thr);
    }
  }
  DiskWriteThreadPool::~DiskWriteThreadPool() {
    while (m_threads.size()) {
      delete m_threads.back();
      m_threads.pop_back();
    }
  }
  void DiskWriteThreadPool::startThreads() {
    m_lc.log(LOG_INFO, "Starting threads in DiskWriteThreadPool::DiskWriteThreadPool");
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->start();
    }
  }
  
  void DiskWriteThreadPool::waitThreads() {
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->wait();
    }
  }
   void DiskWriteThreadPool::push(DiskWriteTaskInterface *t) { 
     {//begin of critical section
       castor::tape::threading::MutexLocker ml(&m_counterProtection);
       if(NULL==t){
         throw castor::tape::Exception("NULL task should not been directly pushed into DiskWriteThreadPool");
       }
     }
      m_tasks.push(t);
  }
  void DiskWriteThreadPool::finish() {
    castor::tape::threading::MutexLocker ml(&m_counterProtection);
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }

  bool DiskWriteThreadPool::belowMidFilesAfterPop(int filesPopped) const {
    return m_tasks.size() -filesPopped < m_maxFilesReq/2;
  }

  bool DiskWriteThreadPool::crossingDownFileThreshod(int filesPopped) const {
    return (m_tasks.size() >= m_maxFilesReq/2) && (m_tasks.size() - filesPopped < m_maxFilesReq/2);
  }
  
  void DiskWriteThreadPool::DiskWriteWorkerThread::run() {
    m_lc.pushOrReplace(log::Param("thread", "diskWrite"));
    m_lc.log(LOG_INFO, "Starting DiskWriteWorkerThread");
    std::auto_ptr<DiskWriteTaskInterface>  task;
    while(1) {
      task.reset(m_parentThreadPool. m_tasks.pop());
      if (NULL!=task.get()) {
        if(false==task->execute(m_parentThreadPool.m_reporter,m_lc)) {
          ++m_parentThreadPool.m_failedWriteCount; 
        }
      } //end of task!=NULL
      else {
        log::LogContext::ScopedParam param(m_lc, log::Param("threadID", m_threadID));
        m_lc.log(LOG_INFO,"Disk write thread finishing");
        break;
      }
    } //enf of while(1)
    
    if(0 == --m_parentThreadPool.m_nbActiveThread){
      //Im the last Thread alive, report end of session
      if(m_parentThreadPool.m_failedWriteCount==0){
        m_parentThreadPool.m_reporter.reportEndOfSession();
      }
      else{
        m_parentThreadPool.m_reporter.reportEndOfSessionWithErrors("A thread failed to write a file",SEINTERNAL);
      }
    }
    m_lc.log(LOG_INFO, "Finishing DiskWriteWorkerThread");
  }
}}}}

