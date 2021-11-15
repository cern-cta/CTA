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

#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include <memory>
#include <sstream>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// DiskReadThreadPool constructor
//------------------------------------------------------------------------------
DiskReadThreadPool::DiskReadThreadPool(int nbThread, uint64_t maxFilesReq,uint64_t maxBytesReq,
    castor::tape::tapeserver::daemon::MigrationWatchDog & migrationWatchDog,
    cta::log::LogContext lc, const std::string & xrootPrivateKeyPath, uint16_t xrootTimeout) : 
    m_xrootPrivateKeyPath(xrootPrivateKeyPath),
    m_xrootTimeout(xrootTimeout),
    m_watchdog(migrationWatchDog),
    m_lc(lc),m_maxFilesReq(maxFilesReq),
    m_maxBytesReq(maxBytesReq), m_nbActiveThread(0) {
  for(int i=0; i<nbThread; i++) {
    DiskReadWorkerThread * thr = new DiskReadWorkerThread(*this);
    m_threads.push_back(thr);
    m_lc.pushOrReplace(cta::log::Param("threadID",i));
    m_lc.log(cta::log::DEBUG, "DiskReadWorkerThread created");
  }
}

//------------------------------------------------------------------------------
// DiskReadThreadPool destructor
//------------------------------------------------------------------------------
DiskReadThreadPool::~DiskReadThreadPool() { 
  while (m_threads.size()) {
    delete m_threads.back();
    m_threads.pop_back();
  }
  m_lc.log(cta::log::DEBUG, "Deleted threads in DiskReadThreadPool::~DiskReadThreadPool");
}

//------------------------------------------------------------------------------
// DiskReadThreadPool::startThreads
//------------------------------------------------------------------------------
void DiskReadThreadPool::startThreads() {
  for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); ++i) {
    (*i)->start();
  }
  m_lc.log(cta::log::INFO, "All the DiskReadWorkerThreads are started");
}

//------------------------------------------------------------------------------
// DiskReadThreadPool::waitThreads
//------------------------------------------------------------------------------
void DiskReadThreadPool::waitThreads() {
  for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); ++i) {
    (*i)->wait();
  }
}

//------------------------------------------------------------------------------
// DiskReadThreadPool::push
//------------------------------------------------------------------------------
void DiskReadThreadPool::push(DiskReadTask *t) { 
  m_tasks.push(t); 
  m_lc.log(cta::log::INFO, "Push a task into the DiskReadThreadPool");
}

//------------------------------------------------------------------------------
// DiskReadThreadPool::finish
//------------------------------------------------------------------------------
void DiskReadThreadPool::finish() {
  /* Insert one endOfSession per thread */
  for (size_t i=0; i<m_threads.size(); i++) {
    m_tasks.push(NULL);
  }
}

//------------------------------------------------------------------------------
// DiskReadThreadPool::popAndRequestMore
//------------------------------------------------------------------------------
DiskReadTask* DiskReadThreadPool::popAndRequestMore(cta::log::LogContext &lc){
  cta::threading::BlockingQueue<DiskReadTask*>::valueRemainingPair 
  vrp = m_tasks.popGetSize();
  cta::log::LogContext::ScopedParam sp(lc, cta::log::Param("m_maxFilesReq", m_maxFilesReq));
  cta::log::LogContext::ScopedParam sp0(lc, cta::log::Param("m_maxBytesReq", m_maxBytesReq));

  if(0==vrp.remaining){
    m_injector->requestInjection(true);
    lc.log(cta::log::DEBUG, "Requested injection from MigrationTaskInjector (with last call)");
  }else if(vrp.remaining + 1 ==  m_maxFilesReq/2){
    m_injector->requestInjection(false);
    lc.log(cta::log::DEBUG, "Requested injection from MigrationTaskInjector (without last call)");
  }
  return vrp.value;
}
//------------------------------------------------------------------------------
//addThreadStats
//------------------------------------------------------------------------------
void DiskReadThreadPool::addThreadStats(const DiskStats& other){
  cta::threading::MutexLocker lock(m_statAddingProtection);
  m_pooldStat+=other;
}
//------------------------------------------------------------------------------
//logWithStat
//------------------------------------------------------------------------------
void DiskReadThreadPool::logWithStat(int level, const std::string& message){
  m_pooldStat.totalTime = m_totalTime.secs();
  cta::log::ScopedParamContainer params(m_lc);
  params.add("poolReadWriteTime", m_pooldStat.readWriteTime)
        .add("poolWaitFreeMemoryTime",m_pooldStat.waitFreeMemoryTime)
        .add("poolCheckingErrorTime",m_pooldStat.checkingErrorTime)
        .add("poolOpeningTime",m_pooldStat.openingTime)
        .add("poolTransferTime", m_pooldStat.transferTime)
        .add("poolRealTime",m_pooldStat.totalTime)
        .add("poolFileCount",m_pooldStat.filesCount)
        .add("poolDataVolume", m_pooldStat.dataVolume)
        .add("poolGlobalPayloadTransferSpeedMBps",
           m_pooldStat.totalTime?1.0*m_pooldStat.dataVolume/1000/1000/m_pooldStat.totalTime:0)
        .add("poolAverageDiskPerformanceMBps",
           m_pooldStat.transferTime?1.0*m_pooldStat.dataVolume/1000/1000/m_pooldStat.transferTime:0.0)
        .add("poolOpenRWCloseToTransferTimeRatio",
           m_pooldStat.transferTime?(m_pooldStat.openingTime+m_pooldStat.readWriteTime+m_pooldStat.closingTime)/m_pooldStat.transferTime:0.0);
  m_lc.log(level,message);
}
//------------------------------------------------------------------------------
// DiskReadWorkerThread::run
//------------------------------------------------------------------------------
void DiskReadThreadPool::DiskReadWorkerThread::run() {
  cta::log::ScopedParamContainer logParams(m_lc);
  logParams.add("thread", "DiskRead")
           .add("threadID", m_threadID);
  m_lc.log(cta::log::DEBUG, "Starting DiskReadWorkerThread");
  
  std::unique_ptr<DiskReadTask> task;
  cta::utils::Timer localTime;
  cta::utils::Timer totalTime;
  
  while(1) {
    task.reset( m_parent.popAndRequestMore(m_lc));
    m_threadStat.waitInstructionsTime += localTime.secs(cta::utils::Timer::resetCounter);
    if (NULL!=task.get()) {
      task->execute(m_lc, m_diskFileFactory,m_parent.m_watchdog, m_threadID);
      m_threadStat += task->getTaskStats();
    }
    else {
      break;
    }
  } //end of while(1)
  m_threadStat.totalTime = totalTime.secs();
  m_parent.addThreadStats(m_threadStat);
  logWithStat(cta::log::INFO, "Finishing of DiskReadWorkerThread");
  // We now acknowledge to the task injector that read reached the end. There
  // will hence be no more requests for more. (last thread turns off the light)
  int remainingThreads = --m_parent.m_nbActiveThread;
  if (!remainingThreads) {
    m_parent.m_injector->finish();
    m_lc.log(cta::log::INFO, "Signalled to task injector the end of disk read threads");
    m_parent.logWithStat(cta::log::INFO, "All the DiskReadWorkerThreads have completed");
  } else {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("remainingThreads", remainingThreads);
    m_lc.log(cta::log::DEBUG, "Will not signal the end to task injector yet");
  }
}

//------------------------------------------------------------------------------
// DiskReadWorkerThread::logWithStat
//------------------------------------------------------------------------------
void DiskReadThreadPool::DiskReadWorkerThread::
logWithStat(int level, const std::string& message){
  cta::log::ScopedParamContainer params(m_lc);
     params.add("threadReadWriteTime", m_threadStat.readWriteTime)
           .add("threadWaitFreeMemoryTime",m_threadStat.waitFreeMemoryTime)
           .add("threadCheckingErrorTime",m_threadStat.checkingErrorTime)
           .add("threadOpeningTime",m_threadStat.openingTime)
           .add("threadTransferTime",m_threadStat.transferTime)
           .add("threadTotalTime",m_threadStat.totalTime)
           .add("threadDataVolume",m_threadStat.dataVolume)
           .add("threadFileCount",m_threadStat.filesCount)
           .add("threadGlobalPayloadTransferSpeedMBps",
              m_threadStat.totalTime?1.0*m_threadStat.dataVolume/1000/1000/m_threadStat.totalTime:0)
           .add("threadAverageDiskPerformanceMBps",
              m_threadStat.transferTime?1.0*m_threadStat.dataVolume/1000/1000/m_threadStat.transferTime:0.0)
           .add("threadOpenRWCloseToTransferTimeRatio",
              m_threadStat.transferTime?(m_threadStat.openingTime+m_threadStat.readWriteTime+m_threadStat.closingTime)/m_threadStat.transferTime:0.0);
    m_lc.log(level,message);
}
}}}}

