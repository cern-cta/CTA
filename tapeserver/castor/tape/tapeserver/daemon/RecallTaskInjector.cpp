/****************************************************************************** 
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
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
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "log.h"
#include "scheduler/RetrieveJob.hpp"

#include <stdint.h>

using castor::log::LogContext;
using castor::log::Param;

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskWriteThreadPool & diskWriter,
        cta::RetrieveMount &retrieveMount,
        uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_retrieveMount(retrieveMount),m_lc(lc),m_maxFiles(maxFiles),m_maxBytes(byteSizeThreshold)
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
  castor::server::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request());
}
//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void RecallTaskInjector::requestInjection(bool lastCall) {
  //@TODO where shall we  acquire the lock ? There of just before the push ?
  castor::server::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));
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
void RecallTaskInjector::injectBulkRecalls(const std::vector<cta::RetrieveJob *>& jobs) {
  for (auto it = jobs.begin(); it != jobs.end(); ++it) {
    
    (*it)->positioningMethod=cta::RetrieveJob::PositioningMethod::ByBlock;

    LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("NSHOSTNAME", (*it)->tapeCopyLocation.nsHostName)),
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->tapeCopyLocation.fileId)),
      LogContext::ScopedParam(m_lc, Param("fSeq", (*it)->tapeCopyLocation.fseq)),
      LogContext::ScopedParam(m_lc, Param("blockID", (*it)->tapeCopyLocation.blockId)),
      LogContext::ScopedParam(m_lc, Param("path", (*it)->tapeCopyLocation.filePath))
    };
    tape::utils::suppresUnusedVariable(sp);
    
    m_lc.log(LOG_INFO, "Recall task created");
    
    DiskWriteTask * dwt = new DiskWriteTask(*it, m_memManager);
    TapeReadTask * trt = new TapeReadTask(*it, *dwt, m_memManager);
    
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
    m_lc.log(LOG_INFO, "Created tasks for recalling a file");
  }
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", jobs.size()));
  m_lc.log(LOG_INFO, "Finished processing batch of recall tasks from client");
}
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousInjection()
{
  std::vector<cta::RetrieveJob *> jobs;
  
  try {
    uint64_t files=0;
    uint64_t bytes=0;
    while(files<=m_maxFiles && bytes<=m_maxBytes) {
      std::unique_ptr<cta::RetrieveJob> job=m_retrieveMount.getNextJob();
      if(!job.get()) break;
      jobs.push_back(job.release());
      files++;
      bytes+=job->m_fileSize;
    }
  } catch (castor::exception::Exception & ex) {
    castor::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_retrieveMount.getMountTransactionId())
          .add("byteSizeThreshold",m_maxBytes)
          .add("maxFiles", m_maxFiles)
          .add("message", ex.getMessageValue());
    m_lc.log(LOG_ERR, "Failed to getFilesToRecall");
    return false;
  }
  castor::log::ScopedParamContainer scoped(m_lc); 
  scoped.add("byteSizeThreshold",m_maxBytes)
        .add("maxFiles", m_maxFiles);
  if(jobs.empty()) { 
    m_lc.log(LOG_ERR, "No files to recall: empty mount");
    return false;
  }
  else {
    injectBulkRecalls(jobs);
    return true;
  }
}
//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------
  void RecallTaskInjector::signalEndDataMovement(){        
    //first send the end signal to the threads
    m_tapeReader.finish();
    m_diskWriter.finish();
  }
//------------------------------------------------------------------------------
//deleteAllTasks
//------------------------------------------------------------------------------
  void RecallTaskInjector::deleteAllTasks(){
    //discard all the tasks !!
    while(m_queue.size()>0){
      m_queue.pop();
    }
  }
  
//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void RecallTaskInjector::WorkerThread::run()
{
  using castor::log::LogContext;
  m_parent.m_lc.pushOrReplace(Param("thread", "RecallTaskInjector"));
  m_parent.m_lc.log(LOG_DEBUG, "Starting RecallTaskInjector thread");
  
  try{
    while (1) {
      Request req = m_parent.m_queue.pop();
      if (req.end) {
        m_parent.m_lc.log(LOG_INFO,"Received a end notification from tape thread: triggering the end of session.");
        m_parent.signalEndDataMovement();
        break;
      }
      m_parent.m_lc.log(LOG_DEBUG,"RecallJobInjector:run: about to call client interface");
      std::vector<cta::RetrieveJob *> jobs;
      
      uint64_t files=0;
      uint64_t bytes=0;
      while(files<=req.filesRequested && bytes<=req.bytesRequested) {
        std::unique_ptr<cta::RetrieveJob> job=m_parent.m_retrieveMount.getNextJob();
        if(!job.get()) break;
        jobs.push_back(job.release());
        files++;
        bytes+=job->m_fileSize;
      }
      
      LogContext::ScopedParam sp01(m_parent.m_lc, Param("transactionId", m_parent.m_retrieveMount.getMountTransactionId()));
      
      if (jobs.empty()) {
        if (req.lastCall) {
          m_parent.m_lc.log(LOG_INFO,"No more file to recall: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        } else {
          m_parent.m_lc.log(LOG_DEBUG,"In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.");
        }
      } else {
        m_parent.injectBulkRecalls(jobs);
      }
    } // end of while(1)
  } //end of try
  catch(const castor::exception::Exception& ex){
      //we end up there because we could not talk to the client
      log::ScopedParamContainer container( m_parent.m_lc);
      container.add("exception code",ex.code())
               .add("exception message",ex.getMessageValue());
      m_parent.m_lc.logBacktrace(LOG_ERR,ex.backtrace());
      m_parent.m_lc.log(LOG_ERR,"In RecallJobInjector::WorkerThread::run(): "
      "could not retrieve a list of file to recall. End of session");
      
      m_parent.signalEndDataMovement();
      m_parent.deleteAllTasks();
    }
  //-------------
  m_parent.m_lc.log(LOG_DEBUG, "Finishing RecallTaskInjector thread");
  /* We want to finish at the first lastCall we encounter.
   * But even after sending finish() to m_diskWriter and to m_tapeReader,
   * m_diskWriter might still want some more task (the threshold could be crossed),
   * so we discard everything that might still be in the queue
   */
  if(m_parent.m_queue.size()>0) {
    bool stillReading =true;
    while(stillReading) {
      Request req = m_parent.m_queue.pop();
      if (req.end){
        stillReading = false;
      }
      LogContext::ScopedParam sp(m_parent.m_lc, Param("lastCall", req.lastCall));
      m_parent.m_lc.log(LOG_DEBUG,"In RecallJobInjector::WorkerThread::run(): popping extra request");
    }
  }
}

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
