#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include <memory>
#include <sstream>
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {


  DiskWriteThreadPool::DiskWriteThreadPool(int nbThread, int maxFilesReq, int maxBlocksReq,
          RecallReportPacker& report,castor::log::LogContext lc):
          m_jobInjector(NULL), m_blocksQueued(0), 
          m_maxFilesReq(maxFilesReq), m_maxBytesReq(maxBlocksReq),m_reporter(report),m_lc(lc)
   {
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
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->startThreads();
    }
  }
  void DiskWriteThreadPool::waitThreads() {
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->waitThreads();
    }
  }
   void DiskWriteThreadPool::push(DiskWriteTaskInterface *t) { 
    {
      castor::tape::threading::MutexLocker ml(&m_counterProtection);
      //m_tasks.size() += t->files();
      m_blocksQueued += t->blocks();
    }
    m_tasks.push(t);
  }
  void DiskWriteThreadPool::finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }
  void DiskWriteThreadPool::setJobInjector(TaskInjector * ji){
    m_jobInjector = ji;
  }

  bool DiskWriteThreadPool::belowMidBlocksAfterPop(int blocksPopped) const {
    return m_blocksQueued - blocksPopped < m_maxBytesReq/2;
  }
  bool DiskWriteThreadPool::belowMidFilesAfterPop(int filesPopped) const {
    return m_tasks.size() -filesPopped < m_maxFilesReq/2;
  }
   bool DiskWriteThreadPool::crossingDownBlockThreshod(int blockPopped) const {
    return (m_blocksQueued >= m_maxBytesReq/2) && (m_blocksQueued - blockPopped < m_maxBytesReq/2);
  }
  bool DiskWriteThreadPool::crossingDownFileThreshod(int filesPopped) const {
    return (m_tasks.size() >= m_maxFilesReq/2) && (m_tasks.size() - filesPopped < m_maxFilesReq/2);
  }
  DiskWriteTaskInterface * DiskWriteThreadPool::popAndRequestMoreJobs() {
    using castor::log::LogContext;
    using castor::log::Param;
    
    DiskWriteTaskInterface * ret = m_tasks.pop();
    {
      castor::tape::threading::MutexLocker ml(&m_counterProtection);
      // We are about to go to empty: request a last call job injection 
      if(m_tasks.size() == 1 && ret->files()) {
        
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("files",m_tasks.size())),
          LogContext::ScopedParam(m_lc, Param("blocks", m_blocksQueued)),
          LogContext::ScopedParam(m_lc, Param("ret->files", ret->files())),
          LogContext::ScopedParam(m_lc, Param("ret->blocks", ret->blocks())),
          LogContext::ScopedParam(m_lc, Param("maxFiles", m_maxFilesReq)),
          LogContext::ScopedParam(m_lc, Param("maxBlocks", m_maxBytesReq))
        };
        tape::utils::suppresUnusedVariable(sp);
    
        m_lc.log(LOG_INFO, "In DiskWriteTaskInterface::popAndRequestMoreJobs(), requesting last call");
    
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBytesReq, true);
        //if we are below mid on both block and files and we are crossing a threshold 
        //on either files of blocks, then request more jobs
      } else if (belowMidBlocksAfterPop(ret->blocks()) && belowMidFilesAfterPop(ret->files()) 
	      && (crossingDownBlockThreshod(ret->blocks()) || crossingDownFileThreshod(ret->files()))) {
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("files",m_tasks.size())),
          LogContext::ScopedParam(m_lc, Param("blocks", m_blocksQueued)),
          LogContext::ScopedParam(m_lc, Param("ret->files", ret->files())),
          LogContext::ScopedParam(m_lc, Param("ret->blocks", ret->blocks())),
          LogContext::ScopedParam(m_lc, Param("maxFiles", m_maxFilesReq)),
          LogContext::ScopedParam(m_lc, Param("maxBlocks", m_maxBytesReq))
        };
        tape::utils::suppresUnusedVariable(sp);
    
        m_lc.log(LOG_INFO, "In DiskWriteTaskInterface::popAndRequestMoreJobs(), requesting: files");
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBytesReq, false);
      }      
      m_blocksQueued-=ret->blocks();
    }
    return ret;
  }
  void DiskWriteThreadPool::DiskWriteWorkerThread::run() {
    std::auto_ptr<DiskWriteTaskInterface>  task;
    castor::log::LogContext lc = _this.m_lc;
    while(1) {
      task.reset(_this.popAndRequestMoreJobs());
      if (NULL!=task.get()) {
        if(false==task->execute(_this.m_reporter,lc)) {
          ++failledWritting; 
        }
        else {
          log::LogContext::ScopedParam(_this.m_lc, log::Param("threadID", threadID));
          lc.log(LOG_INFO,"Disk write thread finishing");
          break;
        }
      } //end of task!=NULL
    } //enf of while(1))
    
    if(0 == --m_nbActiveThread){
      //Im the last Thread alive, report end of session
      if(failledWritting==0){
        _this.m_reporter.reportEndOfSession();
      }
      else{
        std::ostringstream ss("threadID=");
        ss<<threadID<<" failed to write a file";
        _this.m_reporter.reportEndOfSessionWithErrors(ss.str(),SEINTERNAL);
      }
    }
  }
 
  tape::threading::AtomicCounter<int> DiskWriteThreadPool::DiskWriteWorkerThread::m_nbActiveThread(0);
  tape::threading::AtomicCounter<int> DiskWriteThreadPool::DiskWriteWorkerThread::failledWritting(0);
}}}}

