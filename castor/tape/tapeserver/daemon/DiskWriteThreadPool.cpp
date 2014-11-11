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

#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/common/CastorConfiguration.hpp"
#include "castor/utils/Timer.hpp"
#include "h/log.h"

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
  RecallReportPacker& report,castor::log::LogContext lc,
  const std::string & remoteFileProtocol,
  const std::string & xrootPrivateKeyPath):
  m_diskFileFactory(remoteFileProtocol,xrootPrivateKeyPath),
  m_reporter(report),m_lc(lc)
{
  m_lc.pushOrReplace(castor::log::Param("threadCount", nbThread));
  for(int i=0; i<nbThread; i++) {
    DiskWriteWorkerThread * thr = new DiskWriteWorkerThread(*this);
    m_threads.push_back(thr);
  }
  m_lc.log(LOG_DEBUG, "Created threads in DiskWriteThreadPool::DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::~DiskWriteThreadPool
//------------------------------------------------------------------------------
DiskWriteThreadPool::~DiskWriteThreadPool() {
  // A barrier preventing destruction of the object if a poster has still not
  // returned yet from the push or finish function.
  castor::server::MutexLocker ml(&m_pusherProtection);
  while (m_threads.size()) {
    delete m_threads.back();
    m_threads.pop_back();
  }
  m_lc.log(LOG_DEBUG, "Deleted threads in DiskWriteThreadPool::~DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::startThreads
//------------------------------------------------------------------------------
void DiskWriteThreadPool::startThreads() {
  for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); i++) {
    (*i)->start();
  }
  m_lc.log(LOG_INFO, "Starting threads in DiskWriteThreadPool::DiskWriteThreadPool");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::waitThreads
//------------------------------------------------------------------------------
void DiskWriteThreadPool::waitThreads() {
  for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
          i != m_threads.end(); i++) {
    (*i)->wait();
  }
  m_lc.log(LOG_INFO, "All DiskWriteThreadPool threads are now complete");
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::push
//------------------------------------------------------------------------------
void DiskWriteThreadPool::push(DiskWriteTask *t) { 
  {
    if(NULL==t){
      throw castor::exception::Exception("NULL task should not been directly pushed into DiskWriteThreadPool");
    }
  }
  castor::server::MutexLocker ml(&m_pusherProtection);
  m_tasks.push(t);
}

//------------------------------------------------------------------------------
// DiskWriteThreadPool::finish
//------------------------------------------------------------------------------
void DiskWriteThreadPool::finish() {
  castor::server::MutexLocker ml(&m_pusherProtection);
  for (size_t i=0; i<m_threads.size(); i++) {
    m_tasks.push(NULL);
  }
}
//------------------------------------------------------------------------------
//addThreadStats
//------------------------------------------------------------------------------
void DiskWriteThreadPool::addThreadStats(const DiskStats& other){
  castor::server::MutexLocker lock(&m_statAddingProtection);
  m_pooldStat+=other;
}
//------------------------------------------------------------------------------
//logWithStat
//------------------------------------------------------------------------------
void DiskWriteThreadPool::logWithStat(int level, const std::string& message){
  m_pooldStat.totalTime = m_totalTime.secs();
  log::ScopedParamContainer params(m_lc);
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
  typedef castor::log::LogContext::ScopedParam ScopedParam;
  using castor::log::Param;
  m_lc.pushOrReplace(log::Param("thread", "diskWrite"));
  m_lc.pushOrReplace(log::Param("threadID", m_threadID));
  m_lc.log(LOG_INFO, "Starting DiskWriteWorkerThread");
  
  std::auto_ptr<DiskWriteTask>  task;
  castor::utils::Timer localTime;
  castor::utils::Timer totalTime(localTime);
  
  while(1) {
    task.reset(m_parentThreadPool.m_tasks.pop());
    m_threadStat.waitInstructionsTime+=localTime.secs(castor::utils::Timer::resetCounter);
    if (NULL!=task.get()) {
      if(false==task->execute(m_parentThreadPool.m_reporter,m_lc, 
          m_parentThreadPool.m_diskFileFactory)) {
        ++m_parentThreadPool.m_failedWriteCount;
        ScopedParam sp(m_lc, Param("errorCount", m_parentThreadPool.m_failedWriteCount));
        m_lc.log(LOG_ERR, "Task failed: counting another error for this session");
      }
      m_threadStat+=task->getTaskStats();
    } //end of task!=NULL
    else {
      m_lc.log(LOG_DEBUG,"DiskWriteWorkerThread exiting: no more work");
      break;
    }
  } //enf of while(1)
  m_threadStat.totalTime = totalTime.secs();
  logWithStat(LOG_INFO, "Finishing DiskWriteWorkerThread");
  m_parentThreadPool.addThreadStats(m_threadStat);
  if(0 == --m_parentThreadPool.m_nbActiveThread){

    //Im the last Thread alive, report end of session
    if(m_parentThreadPool.m_failedWriteCount==0){
      m_parentThreadPool.m_reporter.reportEndOfSession();
      m_parentThreadPool.logWithStat(LOG_INFO, "As last exiting DiskWriteWorkerThread, reported a successful end of session");
    }
    else{
      m_parentThreadPool.m_reporter.reportEndOfSessionWithErrors("End of recall session with error(s)",SEINTERNAL);
      ScopedParam sp(m_lc, Param("errorCount", m_parentThreadPool.m_failedWriteCount));
      m_parentThreadPool.logWithStat(LOG_INFO, "As last exiting DiskWriteWorkerThread, reported an end of session with errors");
    }
  }
}

//------------------------------------------------------------------------------
// DiskWriteWorkerThread::logWithStat
//------------------------------------------------------------------------------
void DiskWriteThreadPool::DiskWriteWorkerThread::
logWithStat(int level, const std::string& msg) {
  log::ScopedParamContainer params(m_lc);
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

