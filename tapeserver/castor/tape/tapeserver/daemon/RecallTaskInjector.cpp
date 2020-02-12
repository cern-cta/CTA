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
        m_useRAO(false),
        m_firstTasksInjectedFuture(m_firstTasksInjectedPromise.get_future()){}
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
}
//------------------------------------------------------------------------------
//waitForPromise
//------------------------------------------------------------------------------
void RecallTaskInjector::waitForPromise() {
  m_raoFuture.wait();
}
//------------------------------------------------------------------------------
//setPromise
//------------------------------------------------------------------------------
void RecallTaskInjector::setPromise() {
  try {
    m_raoPromise.set_value();
  } catch (const std::exception &exc) {
    throw cta::exception::Exception(std::string("In RecallTaskInjector::setPromise() got std::exception: ") + exc.what());
  }
}

/**
 * Idempotently tell the TapeReadSingleThread
 * that the first TapeRead tasks have been injected.
 * If we do not do that, as the RecallTaskInjector has to do some
 * RAO query, the TapeReadSingleThread would start without any tasks
 * and will tell the RecallTaskInjector to stop
 */
void RecallTaskInjector::setFirstTasksInjectedPromise() {
  if(!m_promiseFirstTaskInjectedSet){
    m_firstTasksInjectedPromise.set_value();
    m_promiseFirstTaskInjectedSet = true;
  }
}

/**
 * This method is used by the TapeReadSingleThread to wait
 * the first injection of TapeRead tasks
 */
void RecallTaskInjector::waitForFirstTasksInjectedPromise(){
  m_firstTasksInjectedFuture.wait();
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
    m_lc.log(cta::log::INFO, "Performing RAO reordering");

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

  std::ostringstream recallOrderLog;
  if(m_useRAO) {
    recallOrderLog << "Recall order of FSEQs using RAO:";
  } else {
    recallOrderLog << "Recall order of FSEQs:";
  }

  for (uint32_t i = 0; i < njobs; i++) {
    uint32_t index = m_useRAO ? raoOrder.at(i) : i;

    cta::RetrieveJob *job = m_jobs.at(index).release();
    recallOrderLog << " " << job->selectedTapeFile().fSeq;

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
  if(njobs > 0){
    //At least one task has been created, we tell the TapeReadSingleThread that
    //it can start its infinite loop
    setFirstTasksInjectedPromise();
  }
  m_lc.log(cta::log::INFO, recallOrderLog.str());
  m_jobs.clear();
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", njobs));
  m_lc.log(cta::log::INFO, "Finished processing batch of recall tasks from client");
}
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousFetch()
{
  uint64_t reqFiles = (m_useRAO && m_hasUDS) ? m_raoLimits.maxSupported - m_fetched : m_maxFiles;
  /* If RAO is enabled, we must ask for files up to 1PB.
   * We are limiting to 1PB because the size will be passed as
   * oracle::occi::Number which is limited to ~56 bits precision
   */
  uint64_t reqSize = (m_useRAO && m_hasUDS) ? 1024L * 1024 * 1024 * 1024 * 1024 : m_maxBytes;
  try {
    auto jobsList = m_retrieveMount.getNextJobBatch(reqFiles, reqSize, m_lc);
    for (auto & j: jobsList)
      m_jobs.emplace_back(j.release());
    m_fetched = jobsList.size();
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_retrieveMount.getMountTransactionId())
          .add("byteSizeThreshold",reqSize)
          .add("requestedFiles", reqFiles)
          .add("message", ex.getMessageValue());
    m_lc.log(cta::log::ERR, "Failed to getFilesToRecall");
    return false;
  }
  cta::log::ScopedParamContainer scoped(m_lc); 
  scoped.add("byteSizeThreshold",reqSize)
        .add("requestedFiles", reqFiles);
  if(m_jobs.empty()) {
    m_lc.log(cta::log::ERR, "No files to recall: empty mount");
    return false;
  }
  else {
    if (! m_useRAO)
      injectBulkRecalls();
    else {
      cta::log::ScopedParamContainer scoped(m_lc);
      scoped.add("fetchedFiles", m_fetched);
      m_lc.log(cta::log::INFO,"Fetched files to recall");
    }
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
    /* RecallTaskInjector is waiting to have access to the drive in order
     * to perform the RAO query;
     * This waitForPromise() call means that the drive is mounted 
     */
    m_parent.waitForPromise();
    try {
      m_parent.m_raoLimits = m_parent.m_drive->getLimitUDS();
      m_parent.m_hasUDS = true;
      LogContext::ScopedParam sp(m_parent.m_lc, Param("maxSupportedUDS", m_parent.m_raoLimits.maxSupported));
      m_parent.m_lc.log(cta::log::INFO,"Query getLimitUDS for RAO completed");
      if (m_parent.m_fetched < m_parent.m_raoLimits.maxSupported) {
        /* Fetching until we reach maxSupported for the tape drive RAO */
        m_parent.synchronousFetch();
      }
      m_parent.injectBulkRecalls();
      m_parent.m_useRAO = false;
    }
    catch (castor::tape::SCSI::Exception& e) {
      m_parent.m_lc.log(cta::log::WARNING, "The drive does not support RAO: disabled");
      m_parent.m_useRAO = false;
      m_parent.injectBulkRecalls();
    }
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
      auto jobsList = m_parent.m_retrieveMount.getNextJobBatch(req.filesRequested, req.bytesRequested, m_parent.m_lc);
      for (auto & j: jobsList)
        m_parent.m_jobs.emplace_back(j.release());
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
