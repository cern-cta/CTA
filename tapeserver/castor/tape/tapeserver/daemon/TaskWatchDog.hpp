/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once 


#include "common/threading/AtomicFlag.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/log/LogContext.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/Timer.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "scheduler/TapeMount.hpp"
#include "DataTransferSession.hpp"

#include <map>
#include <unistd.h>

namespace castor::tape::tapeserver::daemon {

/**
 * Virtual class for watching tape read or write operation (mostly complete)
 */
class TaskWatchDog : private cta::threading::Thread{
  friend class DataTransferSession;
protected:
  /**
   * The mutex protecting updates and the worker thread from each other
   */
  cta::threading::Mutex m_mutex;
  
  /**
   *  Number of blocks we moved since the last update. Has to be atomic because it is 
   *  updated from the outside 
   */
  uint64_t m_TapeBytesMovedMoved = 0;
  
  /**
   * Statistics of the current session
   */
  TapeSessionStats m_stats;
  
  /**
   * Did we set the stats at least once?
   */
  bool m_statsSet = false;
  
  /**
   * A timer allowing estimation of the total timer for crashing sessions
   */
  cta::utils::Timer m_tapeThreadTimer;
  
  /**
   *  Timer for regular heartbeat reports to parent process
   */  
  cta::utils::Timer m_reportTimer;
  
  /*
   *  How long since we last logged a warning?
   */
  cta::utils::Timer m_blockMovementReportTimer;
  
  /**
   * how long since the last block movement?
   */
  cta::utils::Timer m_blockMovementTimer;
  
  /**
   * How fast should we tick?
   */
  const double m_pollPeriod;
  
  /**
   *  How often to we send heartbeat notifications (in second)
   */
  const double m_reportPeriod;
  
  /**
   *  How long to we have to wait before saying we are stuck (in second)
   */
  const double m_stuckPeriod;
  
  /*
   *  Atomic flag to stop the thread's loop
   */
  cta::threading::AtomicFlag m_stopFlag;
  
  /*
   *  The proxy that will receive or heartbeat notifications
   */
  cta::tape::daemon::TapedProxy& m_initialProcess;
  
  /**
   * Reference to the current mount. Used to report performance statistics to object
   * store.
   */
  cta::TapeMount & m_mount;

  /**
   * The drive unit name to report
   */
  const std::string m_driveUnitName;
  /*
   *  Is the system at _this very moment_ reading or writing on tape
   */
  bool m_fileBeingMoved = false;
  /*
   * Logging system  
   */
  cta::log::LogContext m_lc;
  
  /*
   * Member function actually logging the file.
   */
  virtual void logStuckFile() = 0;
  
  /**
   * One offs parameters to be sent to the initial process
   */
  cta::threading::BlockingQueue<cta::log::Param> m_toAddParamsQueue;

  /**
   * One offs parameters to be deleted from the initial process
   */
  cta::threading::BlockingQueue<std::string> m_toDeleteParamsQueue;
  
  /**
   * Map of all error counts
   */
  std::map<std::string, uint32_t> m_errorCounts;

  /**
   * Send the statistics to the initial process. m_mutex should be taken before
   * calling.
   */
  void reportStats() {
    // Shortcut definitions
    typedef cta::log::Param Param;
    if (m_statsSet) {
      // Build the statistics to be logged
      std::list<Param> paramList;
      // for delivery time we estimate it ourselves before getting the
      // final stats at the end of the last disk thread. This allows proper
      // estimation of statistics for data transfer sessions that get killed
      // before completion.
      const double deliveryTime = 
        m_stats.deliveryTime?m_stats.deliveryTime:m_tapeThreadTimer.secs();
      // total time is a special case. We will estimate it ourselves before
      // getting the final stats (at the end of the thread). This will allow
      // proper estimation of statistics for tape sessions that get killed
      // before completion.
      const double totalTime = m_stats.totalTime?m_stats.totalTime:m_tapeThreadTimer.secs();
      /** Time beetwen fineshed tape thread and finished disk threads */
      const double drainingTime = 
        deliveryTime > totalTime?deliveryTime-totalTime: 0.0;
      bool wasTapeMounted = true;
      if(m_stats.mountTime == 0.0){
        //Tape was not mounted, we add a message to tell that no physical mount has been
        //triggered
        wasTapeMounted = false;
      }
      paramList.push_back(Param("wasTapeMounted",wasTapeMounted));
      paramList.push_back(Param("mountTime", m_stats.mountTime));
      paramList.push_back(Param("positionTime", m_stats.positionTime));
      paramList.push_back(Param("waitInstructionsTime", m_stats.waitInstructionsTime));
      paramList.push_back(Param("waitFreeMemoryTime", m_stats.waitFreeMemoryTime));
      paramList.push_back(Param("waitDataTime", m_stats.waitDataTime));
      paramList.push_back(Param("waitReportingTime", m_stats.waitReportingTime));
      paramList.push_back(Param("checksumingTime", m_stats.checksumingTime));
      paramList.push_back(Param("readWriteTime", m_stats.readWriteTime));
      paramList.push_back(Param("flushTime", m_stats.flushTime));
      paramList.push_back(Param("unloadTime", m_stats.unloadTime));
      paramList.push_back(Param("unmountTime", m_stats.unmountTime));
      paramList.push_back(Param("encryptionControlTime", m_stats.encryptionControlTime));
      paramList.push_back(Param("transferTime", m_stats.transferTime()));
      paramList.push_back(Param("totalTime", totalTime));
      paramList.push_back(Param("deliveryTime", deliveryTime));
      paramList.push_back(Param("drainingTime", drainingTime));
      paramList.push_back(Param("dataVolume", m_stats.dataVolume));
      paramList.push_back(Param("filesCount", m_stats.filesCount));
      paramList.push_back(Param("headerVolume", m_stats.headerVolume));
      // Compute performance
      paramList.push_back(Param("payloadTransferSpeedMBps", totalTime?1.0*m_stats.dataVolume
                /1000/1000/totalTime:0.0));
      paramList.push_back(Param("driveTransferSpeedMBps", totalTime?1.0*(m_stats.dataVolume+m_stats.headerVolume)
                /1000/1000/totalTime:0.0));
      if(m_mount.getMountType() == cta::common::dataStructures::MountType::Retrieve){
	paramList.push_back(Param("repackFilesCount",m_stats.repackFilesCount));
	paramList.push_back(Param("userFilesCount",m_stats.userFilesCount));
	paramList.push_back(Param("verifiedFilesCount",m_stats.verifiedFilesCount));
	paramList.push_back(Param("repackBytesCount",m_stats.repackBytesCount));
	paramList.push_back(Param("userBytesCount",m_stats.userBytesCount));
	paramList.push_back(Param("verifiedBytesCount",m_stats.verifiedBytesCount));
      }
      // Ship the logs to the initial process
      m_initialProcess.addLogParams(m_driveUnitName, paramList);
    }
  }
  
  /**
   * Thread's loop
   */
  void run(){
    // Shortcut definitions
    typedef cta::log::Param Param;
    // reset timers as we don't know how long it took before the thread started
    m_reportTimer.reset();
    m_blockMovementReportTimer.reset();
    m_blockMovementTimer.reset();
    while(!m_stopFlag) {
      
      // Critical section block for internal watchdog
      {
        cta::threading::MutexLocker locker(m_mutex);
        //useful if we are stuck on a particular file
        if(m_fileBeingMoved && 
           m_blockMovementTimer.secs()>m_stuckPeriod &&
           m_blockMovementReportTimer.secs()>m_stuckPeriod){
          // We are stuck while moving a file. We will just log that.
          logStuckFile();
          m_blockMovementReportTimer.reset();
        }
      }
      
      // Send any one-off parameter
      {
        std::list<Param> params;
        // This is thread safe because we are the only consumer:
        // a non-zero size guarantees we will find something.
        while (m_toAddParamsQueue.size())
          params.push_back(m_toAddParamsQueue.pop());
        if (params.size()) {
          m_initialProcess.addLogParams(m_driveUnitName, params);
        }
      }

      // Send any one-off parameter to delete
      {
        std::list<std::string> params;
        // This is thread safe because we are the only consumer:
        // a non-zero size guarantees we will find something.
        while (m_toDeleteParamsQueue.size())
          params.push_back(m_toDeleteParamsQueue.pop());
        if (params.size()) {
          m_initialProcess.deleteLogParams(m_driveUnitName, params);
        }
      }

      //heartbeat to notify activity to the mother
      // and transmit statistics
      if(m_reportTimer.secs() > m_reportPeriod){
        cta::threading::MutexLocker locker(m_mutex);
        m_lc.log(cta::log::DEBUG,"going to report");
        m_reportTimer.reset();
        m_initialProcess.reportHeartbeat(m_TapeBytesMovedMoved, 0);
        reportStats(); 
        try {
          m_mount.setTapeSessionStats(m_stats);
        } catch (cta::exception::Exception & ex) {
          cta::log::ScopedParamContainer params (m_lc);
          params.add("exceptionMessage", ex.getMessageValue());
          m_lc.log(cta::log::WARNING, "In TaskWatchDog::run(): failed to set tape session stats in sched. DB. Skipping.");
        }

      } 
      else{
        usleep(m_pollPeriod*1000*1000);
      }
    }
    // At the end of the run, make sure we push the last stats to the initial
    // process and that we process any remaining parameter.
    {
      cta::threading::MutexLocker locker(m_mutex);
      reportStats();
      try {
        m_mount.setTapeSessionStats(m_stats);
      } catch (cta::exception::Exception & ex) {
        cta::log::ScopedParamContainer params (m_lc);
        params.add("exceptionMessage", ex.getMessageValue());
        m_lc.log(cta::log::WARNING, "In TaskWatchDog::run(): failed to set tape session stats in sched. DB. Skipping.");
      }
      // Flush the one-of parameters one last time.
      std::list<Param> params;
      while (m_toAddParamsQueue.size())
        params.push_back(m_toAddParamsQueue.pop());
      if (params.size()) {
        m_initialProcess.addLogParams(m_driveUnitName, params);
      }

      std::list<std::string> paramsToDelete;
      // Flush the one-of parameters one last time.
      while (m_toDeleteParamsQueue.size())
        paramsToDelete.push_back(m_toDeleteParamsQueue.pop());
      if (params.size()) {
        m_initialProcess.deleteLogParams(m_driveUnitName, paramsToDelete);
      }
    }
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    // To be sure we wait the full half second, we use a timed loop.
    cta::utils::Timer exitTimer;
    while (exitTimer.secs() < 0.5)
      usleep(100*1000);
  }
  
public:
  /**
   * Constructor
   *
   * @param periodToReport How often should we report to the mother (in seconds)
   * @param stuckPeriod  How long do we wait before we say we are stuck on file (in seconds)
   * @param initialProcess The proxy we use for sending heartbeat 
   * @param reportPacker 
   * @param lc To log the events
   */
  TaskWatchDog(double reportPeriod, double stuckPeriod, cta::tape::daemon::TapedProxy& initialProcess,
    cta::TapeMount& mount, std::string_view driveUnitName, const cta::log::LogContext& lc, double pollPeriod = 0.1) :
    m_pollPeriod(pollPeriod), m_reportPeriod(reportPeriod), m_stuckPeriod(stuckPeriod), m_initialProcess(initialProcess),
    m_mount(mount), m_driveUnitName(driveUnitName), m_lc(lc) {
    m_lc.pushOrReplace(cta::log::Param("thread","Watchdog"));
  }
  
  /**
   * notify the watchdog a mem block has been moved
   */
  void notify(uint64_t movedBytes){
    cta::threading::MutexLocker locker(m_mutex);
    m_blockMovementTimer.reset();
    m_TapeBytesMovedMoved+=movedBytes;
  }
 
  /**
   * update the watchdog's statistics for the session delivery time
   * @param a new deliveryTime
   */
  void updateStatsDeliveryTime (const double deliveryTime) {
    cta::threading::MutexLocker locker(m_mutex);
    m_stats.deliveryTime = deliveryTime;
  }
  
  /**
   * update the watchdog's statistics for the session
   * @param stats the stats counters collection to push
   */
  void updateStatsWithoutDeliveryTime (const TapeSessionStats & stats) {
    cta::threading::MutexLocker locker(m_mutex);
    const double savedDeliveryTime = m_stats.deliveryTime;
    m_stats = stats;
    m_stats.deliveryTime = savedDeliveryTime;
    m_statsSet = true;
  }

  /**
   * update the watchdog's statistics for the session
   * @param stats the stats counters collection to push
   */
  void updateStats (const TapeSessionStats & stats) {
    cta::threading::MutexLocker locker(m_mutex);
    m_stats = stats;
    m_statsSet = true;
  }
  
  /**
   * update the tapeThreadTimer, allowing logging a "best estimate" total time
   * in case of crash. If the provided stats in updateStats has a non-zero total
   * time, we will not use this value.
   * @param tapeThreadTimer:  the start time of the tape thread
   */
  void updateThreadTimer(const cta::utils::Timer & timer) {
    cta::threading::MutexLocker locker(m_mutex);
    m_tapeThreadTimer = timer;
  }
  
  /**
   * Queue new parameter to be sent asynchronously to the main thread.
   */
  void addParameter (const cta::log::Param & param) {
    m_toAddParamsQueue.push(param);
  }

 /**
   * Queue the parameter to be deleted asynchronously in the main thread.
   */
  void deleteParameter (const std::string & param) {
    m_toDeleteParamsQueue.push(param);
  }
  
  /**
   * Add error by name. We will count the errors by name in the watchdog.
   */
  void addToErrorCount (const std::string & errorName) {
    uint32_t count;
    {
      cta::threading::MutexLocker locker(m_mutex);
      // There is no default constructor for uint32_t (auto set to zero),
      // so we need to consider 2 cases.
      if(m_errorCounts.end() != m_errorCounts.find(errorName)) {
        count = ++m_errorCounts[errorName];
      } else {
        count = m_errorCounts[errorName] = 1;
      }
    }
    // We ship the new value ASAP to the main thread.
    addParameter(cta::log::Param(errorName, count));
  }
  
  /**
   * Set error count. This is used for once per session events that could
   * be detected several times.
   */
  void setErrorCount (const std::string & errorName, uint32_t value) {
    {
      cta::threading::MutexLocker locker(m_mutex);
      m_errorCounts[errorName] = value;
    }
    // We ship the new value ASAP to the main thread.
    addParameter(cta::log::Param(errorName, value));
  }
  
  /**
   * Test whether an error happened
   */
  bool errorHappened() {
    cta::threading::MutexLocker locker(m_mutex);
    return m_errorCounts.size();
  }
  
  /**
   * Start the thread
   */
  void startThread(){
    start();
  }
  
  /**
   * Ask to stop the watchdog thread and join it
   */
  void stopAndWaitThread(){
    m_stopFlag.set();
    wait();
  }
  
};

/**
 * Implementation of TaskWatchDog for recalls 
 */
class RecallWatchDog: public TaskWatchDog {
  
private:
  
  /** The file we are working on */
  uint64_t m_fileId;
  uint64_t m_fSeq;
  
  void logStuckFile() override {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("TimeSinceLastBlockMove", m_blockMovementTimer.secs())
          .add("TimeSinceLastBlockMoveReport", m_blockMovementReportTimer.secs())
          .add("NoBlockMoveMaxSecs", m_stuckPeriod)
          .add("fileId", m_fileId)
          .add("fSeq", m_fSeq);
    m_lc.log(cta::log::WARNING, "No tape block movement for too long during recalling");
  }
  
public:
  
  /** Pass through constructor */
  RecallWatchDog(double periodToReport,double stuckPeriod,
    cta::tape::daemon::TapedProxy& initialProcess,
    cta::TapeMount & mount,
    const std::string & driveUnitName,
    const cta::log::LogContext& lc, double pollPeriod = 0.1): 
  TaskWatchDog(periodToReport, stuckPeriod, initialProcess, mount, driveUnitName, lc, 
    pollPeriod) {}
  
  /**
   * Notify the watchdog which file we are operating
   * @param file
   */
  void notifyBeginNewJob(const uint64_t fileId, uint64_t fSeq) {
    cta::threading::MutexLocker locker(m_mutex);
    m_fileId=fileId;
    m_fSeq=fSeq;
    m_fileBeingMoved=true;
  }
  
  /**
   * Notify the watchdog  we have finished operating on the current file
   */
  void fileFinished(){
    cta::threading::MutexLocker locker(m_mutex);
    m_fileBeingMoved=false;
    m_fileId=0;
    m_fSeq=0;
  }
};

/**
 * Implementation of TaskWatchDog for migrations 
 */
class MigrationWatchDog: public TaskWatchDog {

private:
  
  /** The file we are working on */
  uint64_t m_fileId;
  uint64_t m_fSeq;
 
  void logStuckFile() override {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("TimeSinceLastBlockMove", m_blockMovementTimer.secs())
          .add("TimeSinceLastBlockMoveReport", m_blockMovementReportTimer.secs())
          .add("NoBlockMoveMaxSecs", m_stuckPeriod)
          .add("fileId", m_fileId)
          .add("fSeq", m_fSeq);
    m_lc.log(cta::log::WARNING, "No tape block movement for too long during archiving");
  }
  
public:
  
  /** Pass through constructor */
  MigrationWatchDog(double periodToReport,double stuckPeriod,
    cta::tape::daemon::TapedProxy& initialProcess,
    cta::TapeMount & mount,
    const std::string & driveUnitName,
    const cta::log::LogContext& lc, double pollPeriod = 0.1): 
  TaskWatchDog(periodToReport, stuckPeriod, initialProcess, mount, driveUnitName, lc, 
    pollPeriod) {}
  
  /**
   * Notify the watchdog which file we are operating
   * @param file
   */
  void notifyBeginNewJob(const uint64_t fileId, uint64_t fSeq){
    cta::threading::MutexLocker locker(m_mutex);
    m_fileId=fileId;
    m_fSeq=fSeq;
    m_fileBeingMoved=true;
  }
  
  /**
   * Notify the watchdog  we have finished operating on the current file
   */
  void fileFinished(){
    cta::threading::MutexLocker locker(m_mutex);
    m_fileBeingMoved=false;
    m_fileId=0;
    m_fSeq=0;
  }
};

} // namespace castor::tape::tapeserver::daemon
