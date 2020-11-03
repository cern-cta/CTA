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
#include "castor/tape/tapeserver/drive/DriveGeneric.hpp"

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
void RecallTaskInjector::initRAO(const castor::tape::tapeserver::rao::RAOParams & dataConfig, cta::catalogue::Catalogue * catalogue) {
  m_raoManager = castor::tape::tapeserver::rao::RAOManager(dataConfig,m_drive,catalogue);
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
  uint32_t njobs = m_jobs.size();
  std::vector<uint64_t> raoOrder;

  bool useRAO = m_raoManager.useRAO();
  if (useRAO) {
    m_lc.log(cta::log::INFO, "Performing RAO reordering");
    
    raoOrder = m_raoManager.queryRAO(m_jobs,m_lc);
  }

  std::ostringstream recallOrderLog;
  if(useRAO) {
    recallOrderLog << "Recall order of FSEQs using RAO:";
  } else {
    recallOrderLog << "Recall order of FSEQs:";
  }

  for (uint32_t i = 0; i < njobs; i++) {
    uint64_t index = useRAO ? raoOrder.at(i) : i;

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
bool RecallTaskInjector::synchronousFetch(bool & noFilesToRecall)
{
  noFilesToRecall = false;
  uint64_t reqFiles = (m_raoManager.useRAO() && m_raoManager.getMaxFilesSupported()) ? m_raoManager.getMaxFilesSupported().value() - m_fetched : m_maxFiles;
  /* If RAO is enabled, we must ask for files up to 1PB.
   * We are limiting to 1PB because the size will be passed as
   * oracle::occi::Number which is limited to ~56 bits precision
   */
  uint64_t reqSize = (m_raoManager.useRAO() && m_raoManager.hasUDS()) ? 1024L * 1024 * 1024 * 1024 * 1024 : m_maxBytes;
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
    m_lc.log(cta::log::WARNING, "No files to recall: empty mount");
    noFilesToRecall = true;
    return false;
  }
  else {
    if (! m_raoManager.useRAO())
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
  if (m_parent.m_raoManager.useRAO()) {
    /* RecallTaskInjector is waiting to have access to the drive in order
     * to perform the RAO query;
     * This waitForPromise() call means that the drive is mounted 
     */
    m_parent.waitForPromise();
    try {
      m_parent.m_raoManager.setEnterpriseRAOUdsLimits(m_parent.m_drive->getLimitUDS());
      LogContext::ScopedParam sp(m_parent.m_lc, Param("maxSupportedUDS", m_parent.m_raoManager.getMaxFilesSupported().value()));
      m_parent.m_lc.log(cta::log::INFO,"Query getLimitUDS for RAO Enterprise completed");
    } catch (castor::tape::SCSI::Exception& e) {
      cta::log::ScopedParamContainer spc(m_parent.m_lc);
      spc.add("exceptionMessage",e.getMessageValue());
      m_parent.m_lc.log(cta::log::INFO, "Error while fetching the limitUDS for RAO enterprise drive. Will run a CTA RAO.");
    } catch(const castor::tape::tapeserver::drive::DriveDoesNotSupportRAOException &ex){
      m_parent.m_lc.log(cta::log::INFO, "The drive does not support RAO Enterprise, will run a CTA RAO.");
    }
    cta::optional<uint64_t> maxFilesSupportedByRAO = m_parent.m_raoManager.getMaxFilesSupported();
    if (maxFilesSupportedByRAO && m_parent.m_fetched < maxFilesSupportedByRAO.value()) {
      /* Fetching until we reach maxSupported for the tape drive RAO */
      //unused boolean here but need to be kept to respect the synchronousFetch signature
      bool noFilesToRecall;
      m_parent.synchronousFetch(noFilesToRecall);
    }
    m_parent.injectBulkRecalls();
    /**
     * Commenting this line as we want the RAO to be executed on
     * all the batchs and not only one.
     */
    //m_parent.m_raoManager.disableRAO();
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
