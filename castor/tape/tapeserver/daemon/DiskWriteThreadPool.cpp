#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include <memory>
#include <sstream>
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {


  DiskWriteThreadPool::DiskWriteThreadPool(int nbThread, int maxFilesReq, int maxBlocksReq,
          ReportPackerInterface<detail::Recall>& report,castor::log::LogContext lc):
          m_jobInjector(NULL),m_maxFilesReq(maxFilesReq), m_maxBytesReq(maxBlocksReq),
          m_reporter(report),m_lc(lc)
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
     {//begin of critical section
       castor::tape::threading::MutexLocker ml(&m_counterProtection);
       if(NULL==t){
         throw castor::tape::Exception("NULL task should not been directly pushed into DiskWriteThreadPool");
       }
     }
      m_tasks.push(t);
      std::cout<<"size tasks "<<m_tasks.size()<<std::endl;
  }
  void DiskWriteThreadPool::finish() {
    castor::tape::threading::MutexLocker ml(&m_counterProtection);
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }
  void DiskWriteThreadPool::setJobInjector(TaskInjector * ji){
    m_jobInjector = ji;
  }

  bool DiskWriteThreadPool::belowMidFilesAfterPop(int filesPopped) const {
    return m_tasks.size() -filesPopped < m_maxFilesReq/2;
  }

  bool DiskWriteThreadPool::crossingDownFileThreshod(int filesPopped) const {
    return (m_tasks.size() >= m_maxFilesReq/2) && (m_tasks.size() - filesPopped < m_maxFilesReq/2);
  }
  DiskWriteTaskInterface * DiskWriteThreadPool::popAndRequestMoreJobs() {
    using castor::log::LogContext;
    using castor::log::Param;
    
    DiskWriteTaskInterface * ret = m_tasks.pop();
    if(ret)
    {
      castor::tape::threading::MutexLocker ml(&m_counterProtection);
      std::cout<<"pop "<<m_tasks.size()<<std::endl;
      // We are about to go to empty: request a last call job injection 
      if(m_tasks.size() == 1) {
        
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("files",m_tasks.size())),
          LogContext::ScopedParam(m_lc, Param("ret->files", 1)),
          LogContext::ScopedParam(m_lc, Param("maxFiles", m_maxFilesReq)),
          LogContext::ScopedParam(m_lc, Param("maxBlocks", m_maxBytesReq))
        };
        tape::utils::suppresUnusedVariable(sp);
    
        m_lc.log(LOG_INFO, "In DiskWriteTaskInterface::popAndRequestMoreJobs(), requesting last call");
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBytesReq, true);
        //if we are below mid on both block and files and we are crossing a threshold 
        //on either files of blocks, then request more jobs
      } else if ( belowMidFilesAfterPop(1) && crossingDownFileThreshod(1)) {
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("files",m_tasks.size())),
          LogContext::ScopedParam(m_lc, Param("ret->files", 1)),
          LogContext::ScopedParam(m_lc, Param("maxFiles", m_maxFilesReq)),
          LogContext::ScopedParam(m_lc, Param("maxBlocks", m_maxBytesReq))
        };
        tape::utils::suppresUnusedVariable(sp);
        m_lc.log(LOG_INFO, "In DiskWriteTaskInterface::popAndRequestMoreJobs(), requesting: files");
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBytesReq, false);
      }      
    }
    return ret;
  }
  void DiskWriteThreadPool::DiskWriteWorkerThread::run() {
    std::auto_ptr<DiskWriteTaskInterface>  task;
    
    while(1) {
      task.reset(_this. m_tasks.pop());
      if (NULL!=task.get()) {
        if(false==task->execute(_this.m_reporter,lc)) {
          ++failledWritting; 
        }
      } //end of task!=NULL
      else {
        log::LogContext::ScopedParam param(lc, log::Param("threadID", threadID));
        lc.log(LOG_INFO,"Disk write thread finishing");
        break;
      }
    } //enf of while(1)
    
    if(0 == --m_nbActiveThread){
      //Im the last Thread alive, report end of session
      if(failledWritting==0){
        _this.m_reporter.reportEndOfSession();
      }
      else{
        _this.m_reporter.reportEndOfSessionWithErrors("A thread failed to write a file",SEINTERNAL);
      }
    }
  }
 
  tape::threading::AtomicCounter<int> DiskWriteThreadPool::DiskWriteWorkerThread::m_nbActiveThread(0);
  tape::threading::AtomicCounter<int> DiskWriteThreadPool::DiskWriteWorkerThread::failledWritting(0);
}}}}

