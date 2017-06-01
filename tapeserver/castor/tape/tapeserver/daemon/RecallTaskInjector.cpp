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
#include "common/log/LogContext.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/SCSI/Structures.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "scheduler/RetrieveJob.hpp"

#include <stdint.h>

using cta::log::LogContext;
using cta::log::Param;

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskWriteThreadPool & diskWriter,
        cta::RetrieveMount &retrieveMount,
        uint64_t maxFiles, uint64_t byteSizeThreshold,cta::log::LogContext lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_retrieveMount(retrieveMount),m_lc(lc),m_maxFiles(maxFiles),m_maxBytes(byteSizeThreshold),
        m_useRAO(false) {}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
RecallTaskInjector::~RecallTaskInjector(){
}
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------

void RecallTaskInjector::finish(){
  cta::threading::MutexLocker ml(m_producerProtection);
  m_queue.push(Request());
  /* Since this is the ending request, the RecallTaskInjector does not need
   * to wait to have access to the drive
   */
  if (m_useRAO)
    setPromise();
}
//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void RecallTaskInjector::requestInjection(bool lastCall) {
  //@TODO where shall we  acquire the lock ? There of just before the push ?
  cta::threading::MutexLocker ml(m_producerProtection);
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
//setDriveInterface
//------------------------------------------------------------------------------
void RecallTaskInjector::setDriveInterface(castor::tape::tapeserver::drive::DriveInterface *di) {
  m_drive = di;
}
//------------------------------------------------------------------------------
//initRAO
//------------------------------------------------------------------------------
void RecallTaskInjector::initRAO() {
  m_useRAO = true;
  m_raoFuture = m_raoPromise.get_future();
  m_raoLimits = m_drive->getLimitUDS();
}
//------------------------------------------------------------------------------
//waitForPromise
//------------------------------------------------------------------------------
bool RecallTaskInjector::waitForPromise() {
  std::chrono::milliseconds duration (1000);
   std::future_status status = m_raoFuture.wait_for(duration);
   if (status == std::future_status::ready)
     return true;
   return false;
}
//------------------------------------------------------------------------------
//setPromise
//------------------------------------------------------------------------------
void RecallTaskInjector::setPromise() {
  try {
    m_raoPromise.set_value();
  } catch (const std::exception &exc) {}
}
//------------------------------------------------------------------------------
//injectBulkRecalls
//------------------------------------------------------------------------------
void RecallTaskInjector::injectBulkRecalls() {

  uint32_t block_size = 262144;
  uint32_t njobs = m_jobs.size();
  std::vector<uint32_t> raoOrder;

  if (m_useRAO) {
    std::list<castor::tape::SCSI::Structures::RAO::blockLims> files;

    for (uint32_t i = 0; i < njobs; i++) {
      cta::RetrieveJob *job = m_jobs.at(i).get();

      castor::tape::SCSI::Structures::RAO::blockLims lims;
      strncpy((char*)lims.fseq, std::to_string(i).c_str(), sizeof(i));
      lims.begin = job->selectedTapeFile().blockId;
      lims.end = job->selectedTapeFile().blockId + 8 +
                 /* ceiling the number of blocks */
                 ((job->archiveFile.fileSize + block_size - 1) / block_size);

      files.push_back(lims);

      if (files.size() == m_raoLimits.maxSupported ||
              ((i == njobs - 1) && (files.size() > 1))) {
        /* We do a RAO query if:
         *  1. the maximum number of files supported by the drive
         *     for RAO query has been reached
         *  2. the end of the jobs list has been reached and there are at least
         *     2 unordered files
         */
        m_drive->queryRAO(files, m_raoLimits.maxSupported);

        /* Add the RAO sorted files to the new list*/
        for (auto fit = files.begin(); fit != files.end(); fit++) {
          uint64_t id = atoi((char*)fit->fseq);
          raoOrder.push_back(id);
        }
        files.clear();
      }
    }
    
    /* Copy the rest of the files in the new ordered list */
    for (auto fit = files.begin(); fit != files.end(); fit++) {
      uint64_t id = atoi((char*)fit->fseq);
      raoOrder.push_back(id);
    }
    files.clear();
  }

  std::string queryOrderLog = "Query fseq order:";
  for (uint32_t i = 0; i < njobs; i++) {
    uint32_t index = m_useRAO ? raoOrder.at(i) : i;

    cta::RetrieveJob *job = m_jobs.at(index).release();
    queryOrderLog += std::to_string(job->selectedTapeFile().fSeq) + " ";

    job->positioningMethod=cta::PositioningMethod::ByBlock;

    LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("fileId", job->retrieveRequest.archiveFileID)),
      LogContext::ScopedParam(m_lc, Param("fSeq", job->selectedTapeFile().fSeq)),
      LogContext::ScopedParam(m_lc, Param("blockID", job->selectedTapeFile().blockId)),
      LogContext::ScopedParam(m_lc, Param("dstURL", job->retrieveRequest.dstURL))
    };
    tape::utils::suppresUnusedVariable(sp);
    
    m_lc.log(cta::log::INFO, "Recall task created");
    
    DiskWriteTask * dwt = new DiskWriteTask(job, m_memManager);
    TapeReadTask * trt = new TapeReadTask(job, *dwt, m_memManager);

    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
    m_lc.log(cta::log::INFO, "Created tasks for recalling a file");
  }
  m_lc.log(cta::log::INFO, queryOrderLog);
  m_jobs.clear();
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", m_jobs.size()));
  m_lc.log(cta::log::INFO, "Finished processing batch of recall tasks from client");
}
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousFetch()
{
  try {
    uint64_t files=0;
    uint64_t bytes=0;
    while(files<=m_maxFiles && bytes<=m_maxBytes) {
      std::unique_ptr<cta::RetrieveJob> job=m_retrieveMount.getNextJob();
      if(!job.get()) break;
      files++;
      bytes+=job->archiveFile.fileSize;
      m_jobs.emplace_back(job.release());
    }
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_retrieveMount.getMountTransactionId())
          .add("byteSizeThreshold",m_maxBytes)
          .add("maxFiles", m_maxFiles)
          .add("message", ex.getMessageValue());
    m_lc.log(cta::log::ERR, "Failed to getFilesToRecall");
    return false;
  }
  cta::log::ScopedParamContainer scoped(m_lc); 
  scoped.add("byteSizeThreshold",m_maxBytes)
        .add("maxFiles", m_maxFiles);
  if(m_jobs.empty()) {
    m_lc.log(cta::log::ERR, "No files to recall: empty mount");
    return false;
  }
  else {
    if (! m_useRAO)
      injectBulkRecalls();
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
  using cta::log::LogContext;
  m_parent.m_lc.pushOrReplace(Param("thread", "RecallTaskInjector"));
  m_parent.m_lc.log(cta::log::DEBUG, "Starting RecallTaskInjector thread");
  if (m_parent.m_useRAO) {
    bool moreJobs = true;
    /* RecallTaskInjector is waiting to have access to the drive in order
     * to perform the RAO query; while waiting, it is fetching more jobs
     */
    while (true) {
      if (m_parent.waitForPromise()) break;
      if (moreJobs) {
        /* Fetching while there are still jobs to fetch
         * Otherwise, we are just waiting for the promise
         */
        moreJobs =  m_parent.synchronousFetch();
      }
    }
    m_parent.injectBulkRecalls();
    m_parent.m_useRAO = false;
  }
  try{
    while (1) {
      Request req = m_parent.m_queue.pop();
      if (req.end) {
        m_parent.m_lc.log(cta::log::INFO,"Received a end notification from tape thread: triggering the end of session.");
        
        m_parent.signalEndDataMovement();
        break;
      }
      m_parent.m_lc.log(cta::log::DEBUG,"RecallJobInjector:run: about to call client interface");
      uint64_t files=0;
      uint64_t bytes=0;
      while(files<=req.filesRequested && bytes<=req.bytesRequested) {
        std::unique_ptr<cta::RetrieveJob> job=m_parent.m_retrieveMount.getNextJob();
        if(!job.get()) break;
        files++;
        bytes+=job->archiveFile.fileSize;
        m_parent.m_jobs.emplace_back(job.release());
      }
      
      LogContext::ScopedParam sp01(m_parent.m_lc, Param("transactionId", m_parent.m_retrieveMount.getMountTransactionId()));
      
      if (m_parent.m_jobs.empty()) {
        if (req.lastCall) {
          m_parent.m_lc.log(cta::log::INFO,"No more file to recall: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        } else {
          m_parent.m_lc.log(cta::log::DEBUG,"In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.");
        }
      } else {
        m_parent.injectBulkRecalls();
      }
    } // end of while(1)
  } //end of try
  catch(const cta::exception::Exception& ex){
      //we end up there because we could not talk to the client
      cta::log::ScopedParamContainer container( m_parent.m_lc);
      container.add("exception message",ex.getMessageValue());
      m_parent.m_lc.logBacktrace(cta::log::ERR,ex.backtrace());
      m_parent.m_lc.log(cta::log::ERR,"In RecallJobInjector::WorkerThread::run(): "
      "could not retrieve a list of file to recall. End of session");
      m_parent.signalEndDataMovement();
      m_parent.deleteAllTasks();
    }
  //-------------
  m_parent.m_lc.log(cta::log::DEBUG, "Finishing RecallTaskInjector thread");
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
      m_parent.m_lc.log(cta::log::DEBUG,"In RecallJobInjector::WorkerThread::run(): popping extra request");
    }
  }
}

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor
