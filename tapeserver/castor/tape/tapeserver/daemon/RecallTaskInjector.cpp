/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "common/log/LogContext.hpp"
#include "common/make_unique.hpp"
#include "common/optional.hpp"
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
        m_retrieveMount(retrieveMount),
        m_lc(lc),m_maxBatchFiles(maxFiles),m_maxBatchBytes(byteSizeThreshold),
        m_files(0), m_bytes(0),
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
  m_queue.push(Request(m_maxBatchFiles, m_maxBatchBytes, lastCall));
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
//reserveSpaceForNextJobBatch
//------------------------------------------------------------------------------
bool RecallTaskInjector::reserveSpaceForNextJobBatch() {
  bool useRAO = m_raoManager.useRAO();
  auto nextJobBatch = previewGetNextJobBatch(useRAO);

  cta::DiskSpaceReservationRequest necessaryReservedSpace;

  for (auto job: nextJobBatch) {
    auto diskSystemName = job->diskSystemName();
    if (diskSystemName) {
      necessaryReservedSpace.addRequest(diskSystemName.value(), job->archiveFile.fileSize);
    } 
  }

  // See if the free disk space reserved by the injector is enough to fit the new batch
  bool needReservation = false;
  cta::DiskSpaceReservationRequest spaceToReserve;
  for(const auto &reservation: necessaryReservedSpace) {
    cta::log::ScopedParamContainer spc(m_lc);
    auto currentFreeSpace = m_reservedFreeSpace[reservation.first];
    if (currentFreeSpace < reservation.second) {
      spaceToReserve.addRequest(reservation.first, reservation.second - currentFreeSpace);
      needReservation = true;
      spc.add("bytesToReserve", reservation.second - currentFreeSpace);
    }
    spc.add("diskSystemName", reservation.first);
    spc.add("currentReservedBytes", currentFreeSpace);
    spc.add("necessaryReservedBytes", reservation.second);
    m_lc.log(cta::log::DEBUG, "In RecallTaskInjector::reserveSpaceForNextJobBatch(): Current disk space reservation");
  }

  if (!needReservation) {
    m_lc.log(cta::log::INFO, "In RecallTaskInjector::reserveSpaceForNextJobBatch(): Reservation is not necessary for next job batch");
    return true;
  }

  for (const auto &reservation: spaceToReserve) {
    cta::log::ScopedParamContainer spc(m_lc);
    spc.add("diskSystemName", reservation.first);
    spc.add("bytes", reservation.second);
    m_lc.log(cta::log::DEBUG, "Disk space reservation necessary for next job batch");
  }
  bool ret = true;
  try {
    ret = m_retrieveMount.reserveDiskSpace(spaceToReserve, m_lc);
  } catch (std::out_of_range&) {
    //#1076 If the disk system for this mount was removed, process the jobs as if they had no disk system
    // (assuming only one disk system per mount)
    for (auto job: nextJobBatch) {
      job->diskSystemName() = cta::nullopt_t();
    }
    m_lc.log(cta::log::WARNING, 
    "In RecallTaskInjector::reserveSpaceForNextJobBatch(): Disk sapce reservation failed "
    "because disk system configuration has been removed, processing job batch as if it had no disk system");
    return true;
  }
  
  if(!ret) {
    m_retrieveMount.requeueJobBatch(m_jobs, m_lc);
    m_files = 0;
    m_bytes = 0;
    m_lc.log(cta::log::ERR, "In RecallTaskInjector::reserveSpaceForNextJobBatch(): Disk space reservation failed, requeued all pending jobs");
  }
  else {
    m_lc.log(cta::log::INFO, "In RecallTaskInjector::reserveSpaceForNextJobBatch(): Disk space reservation for next job batch succeeded");
    for (const auto &reservation: spaceToReserve) {
      m_reservedFreeSpace.addRequest(reservation.first, reservation.second);
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
//previewGetNextJobBatch
//------------------------------------------------------------------------------
std::list<cta::RetrieveJob*> RecallTaskInjector::previewGetNextJobBatch(bool useRAO) {
  uint32_t njobs = m_jobs.size();
  std::vector<uint64_t> raoOrder;
  if (useRAO) {
    raoOrder = m_raoManager.queryRAO(m_jobs,m_lc);
  }
  uint64_t nFiles = 0;
  uint64_t nBytes = 0;
  std::list<cta::RetrieveJob*> jobBatch;
  for (uint32_t i = 0; i < njobs && nFiles < m_maxBatchFiles && nBytes < m_maxBatchBytes; i++) {
    uint64_t index = useRAO ? raoOrder.at(i) : i;
    auto job = m_jobs.at(index).get();
    jobBatch.push_back(job);
    nFiles++;
    nBytes += job->archiveFile.fileSize;
  }
  return jobBatch;
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
  uint64_t nFiles = 0;
  uint64_t nBytes = 0;
  std::list<std::unique_ptr<cta::RetrieveJob>> retrieveJobsBatch;
  /*
  Select a batch of tasks, request disk space for the batch, process the successfull files and
  store the rest for reinjecting in the queue.
  */
  for (uint32_t i = 0; i < njobs && nFiles < m_maxBatchFiles && nBytes < m_maxBatchBytes; i++) {
    uint64_t index = useRAO ? raoOrder.at(i) : i;
    cta::RetrieveJob *job = m_jobs.at(index).release();
    job->positioningMethod=cta::PositioningMethod::ByBlock;
    retrieveJobsBatch.push_back(std::unique_ptr<cta::RetrieveJob>(job));
    nFiles++;
    nBytes += job->archiveFile.fileSize;
    m_files--;
    m_bytes -= job->archiveFile.fileSize;
  }

  bool setPromise = (retrieveJobsBatch.size() != 0);
  for(auto &job_ptr: retrieveJobsBatch) {
    cta::RetrieveJob *job = job_ptr.release();
    DiskWriteTask * dwt = new DiskWriteTask(job, m_memManager);
    TapeReadTask * trt = new TapeReadTask(job, *dwt, m_memManager);
    recallOrderLog << " " << job->selectedTapeFile().fSeq;
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
    if (job->diskSystemName()) {
      m_reservedFreeSpace.removeRequest(job->diskSystemName().value(), job->archiveFile.fileSize);
    }
    m_lc.log(cta::log::INFO, "Created tasks for recalling a file");
  }
  if(setPromise){
    //At least one task has been created, we tell the TapeReadSingleThread that
    //it can start its infinite loop
    setFirstTasksInjectedPromise();
  }
  m_lc.log(cta::log::INFO, recallOrderLog.str());
  // keep the rest for later injection
  m_jobs.erase(std::remove_if(m_jobs.begin(), m_jobs.end(), [](const std::unique_ptr<cta::RetrieveJob> &jobptr) {
    return jobptr.get() == nullptr;
  }), m_jobs.end());
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", njobs));
  m_lc.log(cta::log::INFO, "Finished processing batch of recall tasks from client");

}

//------------------------------------------------------------------------------
//synchronousFetch
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousFetch(bool & noFilesToRecall)
{
  noFilesToRecall = true;
  /* If RAO is enabled, we must ask for files up to 1PB.
   * We are limiting to 1PB because the size will be passed as
   * oracle::occi::Number which is limited to ~56 bits precision
   */

  uint64_t reqFiles = (m_raoManager.useRAO() && m_raoManager.getMaxFilesSupported()) ? m_raoManager.getMaxFilesSupported().value() : m_maxBatchFiles;
  if (reqFiles <= m_files) {
    return true; //No need to pop from the queue, injector already  holds enough files, but we return there is still work to be done
  }
  reqFiles -= m_files;
  
  uint64_t reqSize = (m_raoManager.useRAO() && m_raoManager.hasUDS()) ? 1024L * 1024 * 1024 * 1024 * 1024 : m_maxBatchBytes;
  if (reqSize <= m_bytes) {
     return true; //No need to pop from the queue, injector already holds enough bytes, but we return there is still work to be done
  }
  reqSize -= m_bytes;
  try {
    auto jobsList = m_retrieveMount.getNextJobBatch(reqFiles, reqSize, m_lc);
    for (auto & j: jobsList) {
      m_files++;
      m_bytes += j->archiveFile.fileSize;
      m_jobs.emplace_back(j.release());
    }
    m_fetched = jobsList.size();
    noFilesToRecall = !jobsList.size();
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_retrieveMount.getMountTransactionId())
          .add("requestedBytes",reqSize)
          .add("requestedFiles", reqFiles)
          .add("message", ex.getMessageValue());
    m_lc.log(cta::log::ERR, "Failed to getFilesToRecall");
    return false;
  }
  if(m_jobs.empty()) {
    m_lc.log(cta::log::WARNING, "No files left to recall on the queue or in the injector");
    return false;
  }
    cta::log::ScopedParamContainer scoped(m_lc); 
  scoped.add("requestedBytes",reqSize)
        .add("requestedFiles", reqFiles)
        .add("fetchedFiles", m_fetched);
  m_lc.log(cta::log::INFO,"Fetched files to recall");  
  return true;
}
//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------
void RecallTaskInjector::signalEndDataMovement(){        
  //send the end signal to the threads
  //(do this only one)
  if (!m_sessionEndSignaled) {
    m_tapeReader.finish();
    m_diskWriter.finish();
    m_sessionEndSignaled = true;
  }
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
  }
  bool reservationResult = m_parent.reserveSpaceForNextJobBatch();

  /* this should never happen, since we check the reservation space before starting the session*/
  if (!reservationResult) {
    m_parent.signalEndDataMovement();
    m_parent.setFirstTasksInjectedPromise();
    goto end_injection;
  }

  m_parent.injectBulkRecalls(); //do an initial injection before entering loop
  try{
    while (1) {
      Request req = m_parent.m_queue.pop();
      if (req.end) {
        m_parent.m_lc.log(cta::log::INFO,"Received a end notification from tape thread: triggering the end of session.");
        m_parent.signalEndDataMovement();
        break;
      }
      m_parent.m_lc.log(cta::log::DEBUG,"RecallJobInjector:run: about to call client interface");
      LogContext::ScopedParam sp01(m_parent.m_lc, Param("transactionId", m_parent.m_retrieveMount.getMountTransactionId()));
      
      /*When disk space reservation fails, we will no longer fetch more files, 
      just wait for the session to finish naturally
      */
      if (reservationResult) {
        bool noFilesToRecall;
        m_parent.synchronousFetch(noFilesToRecall);
        
        reservationResult = m_parent.reserveSpaceForNextJobBatch();
        if (!reservationResult) {
          m_parent.m_lc.log(cta::log::INFO, "Disk space reservation failed: will inject no more files");
        }        
      }
      
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

end_injection:
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
