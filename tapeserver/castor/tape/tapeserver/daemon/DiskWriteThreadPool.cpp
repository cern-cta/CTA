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

#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "common/Timer.hpp"
#include "common/log/LogContext.hpp"

#include <memory>
#include <sstream>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskWriteThreadPool::DiskWriteThreadPool(int nbThread,
  RecallReportPacker& report,
  RecallWatchDog& recallWatchDog,
  cta::log::LogContext lc,
  const std::string & xrootPrivateKeyPath,
  uint16_t xrootTimeout):
  m_xrootPrivateKeyPath(xrootPrivateKeyPath),
  m_xrootTimeout(xrootTimeout),
  m_reporter(report),m_watchdog(recallWatchDog),m_lc(lc)
{
  m_lc.pushOrReplace(cta::log::Param("threadCount", nbThread));
  for(int i=0; i<nbThread; i++) {
    DiskWriteWorkerThread * thr = new DiskWriteWorkerThread(*this);
    m_threads.push_back(thr);
  }
  m_lc.log(cta::log::DEBUG, "Created threads in DiskWriteThreadPool::DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::~DiskWriteThreadPool
//------------------------------------------------------------------------------
DiskWriteThreadPool::~DiskWriteThreadPool() {
  // A barrier preventing destruction of the object if a poster has still not
  // returned yet from the push or finish function.
  cta::threading::MutexLocker ml(m_pusherProtection);
  while (m_threads.size()) {
    delete m_threads.back();
    m_threads.pop_back();
  }
  m_lc.log(cta::log::DEBUG, "Deleted threads in DiskWriteThreadPool::~DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::startThreads
//------------------------------------------------------------------------------
void DiskWriteThreadPool::startThreads() {
  for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); i++) {
    (*i)->start();
  }
  m_lc.log(cta::log::INFO, "Starting threads in DiskWriteThreadPool::DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::waitThreads
//------------------------------------------------------------------------------
void DiskWriteThreadPool::waitThreads() {
  for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); i++) {
    (*i)->wait();
  }
  m_lc.log(cta::log::INFO, "All DiskWriteThreadPool threads are now complete");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::push
//------------------------------------------------------------------------------
void DiskWriteThreadPool::push(DiskWriteTask *t) { 
  {
    if(NULL==t){
      throw cta::exception::Exception("NULL task should not been directly pushed into DiskWriteThreadPool");
    }
  }
  cta::threading::MutexLocker ml(m_pusherProtection);
  m_tasks.push(t);
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::finish
//------------------------------------------------------------------------------
void DiskWriteThreadPool::finish() {
  cta::threading::MutexLocker ml(m_pusherProtection);
  for (size_t i=0; i<m_threads.size(); i++) {
    m_tasks.push(NULL);
  }
}
//------------------------------------------------------------------------------
//addThreadStats
//------------------------------------------------------------------------------
void DiskWriteThreadPool::addThreadStats(const DiskStats& other){
  cta::threading::MutexLocker lock(m_statAddingProtection);
  m_pooldStat+=other;
}
//------------------------------------------------------------------------------
//logWithStat
//------------------------------------------------------------------------------
void DiskWriteThreadPool::logWithStat(int level, const std::string& message){
  m_pooldStat.totalTime = m_totalTime.secs();
  cta::log::ScopedParamContainer params(m_lc);
  params.add("poolReadWriteTime", m_pooldStat.readWriteTime)
        .add("poolChecksumingTime",m_pooldStat.checksumingTime)
        .add("poolWaitDataTime",m_pooldStat.waitDataTime)
        .add("poolWaitReportingTime",m_pooldStat.waitReportingTime)
        .add("poolCheckingErrorTime",m_pooldStat.checkingErrorTime)
        .add("poolOpeningTime",m_pooldStat.openingTime)
        .add("poolClosingTime", m_pooldStat.closingTime)
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
// DiskWriteWorkerThread::run
//------------------------------------------------------------------------------
void DiskWriteThreadPool::DiskWriteWorkerThread::run() {
  cta::log::ScopedParamContainer logParams(m_lc);
  logParams.add("thread", "DiskWrite")
           .add("threadID", m_threadID);
  m_lc.log(cta::log::INFO, "Starting DiskWriteWorkerThread");
  
  std::unique_ptr<DiskWriteTask>  task;
  cta::utils::Timer localTime;
  cta::utils::Timer totalTime(localTime);
  
  while(1) {
    task.reset(m_parentThreadPool.m_tasks.pop());
    m_threadStat.waitInstructionsTime+=localTime.secs(cta::utils::Timer::resetCounter);
    if (NULL!=task.get()) {
      if(false==task->execute(m_parentThreadPool.m_reporter,m_lc, 
          m_diskFileFactory, m_parentThreadPool.m_watchdog, m_threadID)) {
        ++m_parentThreadPool.m_failedWriteCount;
        cta::log::ScopedParamContainer logParams(m_lc);
        logParams.add("errorCount", m_parentThreadPool.m_failedWriteCount);
        m_lc.log(cta::log::ERR, "Task failed: counting another error for this session");
      }
      m_threadStat+=task->getTaskStats();
    } //end of task!=NULL
    else {
      m_lc.log(cta::log::DEBUG,"DiskWriteWorkerThread exiting: no more work");
      break;
    }
  } //enf of while(1)
  m_threadStat.totalTime = totalTime.secs();
  logWithStat(cta::log::INFO, "Finishing DiskWriteWorkerThread");
  m_parentThreadPool.addThreadStats(m_threadStat);
  if(0 == --m_parentThreadPool.m_nbActiveThread){

    //Im the last Thread alive, report end of session
    if(m_parentThreadPool.m_failedWriteCount==0){
      m_parentThreadPool.m_reporter.reportEndOfSession();
      m_parentThreadPool.logWithStat(cta::log::INFO, "As last exiting DiskWriteWorkerThread, reported a successful end of session");
    }
    else{
      m_parentThreadPool.m_reporter.reportEndOfSessionWithErrors("End of recall session with error(s)",666);
      cta::log::ScopedParamContainer logParams(m_lc);
      logParams.add("errorCount", m_parentThreadPool.m_failedWriteCount);
      m_parentThreadPool.logWithStat(cta::log::INFO, "As last exiting DiskWriteWorkerThread, reported an end of session with errors");
    }
    const double deliveryTime = m_parentThreadPool.m_totalTime.secs();
    m_parentThreadPool.m_watchdog.updateStatsDeliveryTime(deliveryTime);
  }
}

//------------------------------------------------------------------------------
// DiskWriteWorkerThread::logWithStat
//------------------------------------------------------------------------------
void DiskWriteThreadPool::DiskWriteWorkerThread::
logWithStat(int level, const std::string& msg) {
  cta::log::ScopedParamContainer params(m_lc);
     params.add("threadReadWriteTime", m_threadStat.readWriteTime)
           .add("threadChecksumingTime",m_threadStat.checksumingTime)
           .add("threadWaitDataTime",m_threadStat.waitDataTime)
           .add("threadWaitReportingTime",m_threadStat.waitReportingTime)
           .add("threadCheckingErrorTime",m_threadStat.checkingErrorTime)
           .add("threadOpeningTime",m_threadStat.openingTime)
           .add("threadClosingTime", m_threadStat.closingTime)
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
    m_lc.log(level,msg);
}
}}}}

