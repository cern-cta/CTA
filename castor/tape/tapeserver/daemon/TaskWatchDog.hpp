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

#pragma once 


#include "castor/server/AtomicFlag.hpp"
#include "castor/server/BlockingQueue.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/utils/Timer.hpp"
#include "castor/utils/utils.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"

namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {
/**
 * Virtual class for watching tape read or write operation (mostly complete)
 */
class TaskWatchDog : private castor::server::Thread{
protected:
  /**
   * The mutex protecting updates and the worker thread from each other
   */
  castor::server::Mutex m_mutex;
  
  /**
   *  Number of blocks we moved since the last update. Has to be atomic because it is 
   *  updated from the outside 
   */
  uint64_t m_nbOfMemblocksMoved;
  
  /**
   * Statistics of the current session
   */
  TapeSessionStats m_stats;
  
  /**
   * Did we set the stats at least once?
   */
  bool m_statsSet;
  
  /**
   * A timer allowing estimation of the total timer for crashing sessions
   */
  castor::utils::Timer m_tapeThreadTimer;
  
  /**
   *  Timer for regular heartbeat reports to parent process
   */  
  castor::utils::Timer m_reportTimer;
  
  /*
   *  How long since we last logged a warning?
   */
  castor::utils::Timer m_blockMovementReportTimer;
  
  /**
   * how long since the last block movement?
   */
  castor::utils::Timer m_blockMovementTimer;
  
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
  castor::server::AtomicFlag m_stopFlag;
  
  /*
   *  The proxy that will receive or heartbeat notifications
   */
  messages::TapeserverProxy& m_initialProcess;
  
  /**
   * The drive unit name to report
   */
  const std::string m_driveUnitName;
  /*
   *  Is the system at _this very moment_ reading or writing on tape
   */
  bool m_fileBeingMoved;
  /*
   * Logging system  
   */
  log::LogContext m_lc;
  
  /*
   * Member function actually logging the file.
   */
  virtual void logStuckFile() = 0;
  
  /**
   * One offs parameters to be sent to the initial process
   */
  castor::server::BlockingQueue<castor::log::Param> m_paramsQueue;

  /**
   * Send the statistics to the initial process. m_mutex should be taken before
   * calling.
   */
  void reportStats() {
    // Shortcut definitions
    typedef castor::log::ParamDoubleSnprintf ParamDoubleSnprintf;
    typedef castor::log::Param Param;
    if (m_statsSet) {
      // Build the statistics to be logged
      std::list<Param> paramList;
      // total time is a special case. We will estimate it ourselves before
      // getting the final stats (at the end of the thread). This will allow
      // proper estimation of statistics for tape sessions that get killed
      // before completion.
      double totalTime = m_stats.totalTime?m_stats.totalTime:m_tapeThreadTimer.secs();
      paramList.push_back(Param("mountTime", m_stats.mountTime));
      paramList.push_back(Param("positionTime", m_stats.positionTime));
      paramList.push_back(Param("waitInstructionsTime", m_stats.waitInstructionsTime));
      paramList.push_back(Param("waitFreeMemoryTime", m_stats.waitFreeMemoryTime));
      paramList.push_back(Param("waitDataTime", m_stats.waitDataTime));
      paramList.push_back(Param("waitReportingTime", m_stats.waitReportingTime));
      paramList.push_back(Param("checksumingTime", m_stats.checksumingTime));
      paramList.push_back(Param("transferTime", m_stats.transferTime));
      paramList.push_back(Param("flushTime", m_stats.flushTime));
      paramList.push_back(Param("unloadTime", m_stats.unloadTime));
      paramList.push_back(Param("unmountTime", m_stats.unmountTime));
      paramList.push_back(Param("totalTime", totalTime));
      paramList.push_back(Param("dataVolume", m_stats.dataVolume));
      paramList.push_back(Param("filesCount", m_stats.filesCount));
      paramList.push_back(Param("headerVolume", m_stats.headerVolume));
      // Compute performance
      paramList.push_back(Param("payloadTransferSpeedMBps", totalTime?1.0*m_stats.dataVolume
                /1000/1000/totalTime:0.0));
      paramList.push_back(Param("driveTransferSpeedMBps", totalTime?1.0*(m_stats.dataVolume+m_stats.headerVolume)
                /1000/1000/totalTime:0.0));
      // Ship the logs to the initial process
      m_initialProcess.addLogParams(m_driveUnitName, paramList);
    }
  }
  
  /**
   * Thread's loop
   */
  void run(){
    // Shortcut definitions
    typedef castor::log::ParamDoubleSnprintf ParamDoubleSnprintf;
    typedef castor::log::Param Param;
    // reset timers as we don't know how long it took before the thread started
    m_reportTimer.reset();
    m_blockMovementReportTimer.reset();
    m_blockMovementTimer.reset();
    while(!m_stopFlag) {
      
      // Critical section block for internal watchdog
      {
        castor::server::MutexLocker locker(&m_mutex);
        //useful if we are stuck on a particular file
        if(m_fileBeingMoved && 
           m_blockMovementTimer.secs()>m_stuckPeriod &&
           m_blockMovementReportTimer.secs()>m_stuckPeriod){
          // We are stuck while moving a file. We will log that.
          logStuckFile();
          m_blockMovementReportTimer.reset();
          break;
        }
      }
      
      // Send any one-off parameter
      {
        std::list<Param> params;
        // This is thread safe because we are the only consumer:
        // a non-zero size guarantees we will find something.
        while (m_paramsQueue.size())
          params.push_back(m_paramsQueue.pop());
        if (params.size()) {
          m_initialProcess.addLogParams(m_driveUnitName, params);
        }
      }

      //heartbeat to notify activity to the mother
      // and transmit statistics
      if(m_reportTimer.secs() > m_reportPeriod){
        castor::server::MutexLocker locker(&m_mutex);
        m_lc.log(LOG_DEBUG,"going to report");
        m_reportTimer.reset();
        m_initialProcess.notifyHeartbeat(m_driveUnitName, m_nbOfMemblocksMoved);
        reportStats();
        m_nbOfMemblocksMoved=0;
      } 
      else{
        usleep(m_pollPeriod*1000*1000);
      }
    }
    // At the end of the run, make sure we push the last stats to the initial
    // process
    {
      castor::server::MutexLocker locker(&m_mutex);
      reportStats();
    }
  }
  
  public:
    
  /**
   * Constructor
   * @param periodToReport How often should we report to the mother (in seconds)
   * @param stuckPeriod  How long do we wait before we say we are stuck on file (in seconds)
   * @param initialProcess The proxy we use for sending heartbeat 
   * @param reportPacker 
   * @param lc To log the events
   */
  TaskWatchDog(double reportPeriod,double stuckPeriod,
         messages::TapeserverProxy& initialProcess,
          const std::string & driveUnitName,
         log::LogContext lc, double pollPeriod = 0.1):
  m_nbOfMemblocksMoved(0), m_statsSet(false), m_pollPeriod(pollPeriod),
  m_reportPeriod(reportPeriod), m_stuckPeriod(stuckPeriod), 
  m_initialProcess(initialProcess), m_driveUnitName(driveUnitName),
  m_fileBeingMoved(false), m_lc(lc) {
    m_lc.pushOrReplace(log::Param("thread","Watchdog"));
  }
  
  /**
   * notify the watchdog a mem block has been moved
   */
  void notify(){
    castor::server::MutexLocker locker(&m_mutex);
    m_blockMovementTimer.reset();
    m_nbOfMemblocksMoved++;
  }
  
  /**
   * update the watchdog's statistics for the session
   * @param stats the stats counters collection to push
   */
  void updateStats (const TapeSessionStats & stats) {
    castor::server::MutexLocker locker(&m_mutex);
    m_stats = stats;
    m_statsSet = true;
  }
  
  /**
   * update the tapeThreadTimer, allowing logging a "best estimate" total time
   * in case of crash. If the provided stats in updateStats has a non-zero total
   * time, we will not use this value.
   * @param tapeThreadTimer:  the start time of the tape thread
   */
  void updateThreadTimer(const castor::utils::Timer & timer) {
    castor::server::MutexLocker locker(&m_mutex);
    m_tapeThreadTimer = timer;
  }
  
  /**
   * 
   */
  void addParameter (const log::Param & param) {
    m_paramsQueue.push(param);
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
  /** Our file type */
  typedef castor::tape::tapegateway::FileToRecallStruct FileStruct;
  /** The file we are working on */
  FileStruct m_file;
  
  virtual void logStuckFile() {
// This code is commented out pending the implementation of a more complete
// watchdog involving disk side too (CASTOR-4773 tapeserverd's internal watchdog 
// improvement: track disk transfers as well)
// 
//    castor::log::ScopedParamContainer params(m_lc);
//    params.addTiming("TimeSinceLastBlockMove", m_blockMovementTimer.usecs())
//          .add("Path",m_file.path())
//          .add("FILEID",m_file.fileid())
//          .add("fSeq",m_file.fseq());
//    m_lc.log(LOG_WARNING, "No tape block movement for too long");
  }
public:
  /** Pass through constructor */
  RecallWatchDog(double periodToReport,double stuckPeriod,
    messages::TapeserverProxy& initialProcess,
    const std::string & driveUnitName,
    log::LogContext lc, double pollPeriod = 0.1): 
  TaskWatchDog(periodToReport, stuckPeriod, initialProcess, driveUnitName, lc, 
    pollPeriod) {}
  /**
   * Notify the watchdog which file we are operating
   * @param file
   */
  void notifyBeginNewJob(const FileStruct& file){
    castor::server::MutexLocker locker(&m_mutex);
    m_file=file;
    m_fileBeingMoved=true;
  }
  
  /**
   * Notify the watchdog  we have finished operating on the current file
   */
  void fileFinished(){
    castor::server::MutexLocker locker(&m_mutex);
    m_fileBeingMoved=false;
    m_file=FileStruct();
  }
};

/**
 * Implementation of TaskWatchDog for migrations 
 */
class MigrationWatchDog: public TaskWatchDog {
private:
  /** Our file type */
  typedef castor::tape::tapegateway::FileToMigrateStruct FileStruct;
  /** The file we are working on */
  FileStruct m_file;
  
  virtual void logStuckFile() {
    castor::log::ScopedParamContainer params(m_lc);
    params.addSnprintfDouble("TimeSinceLastBlockMove", m_blockMovementTimer.usecs())
          .add("Path",m_file.path())
          .add("FILEID",m_file.fileid())
          .add("fSeq",m_file.fseq());
    m_lc.log(LOG_WARNING, "No tape block movement for too long");
  }
public:
  /** Pass through constructor */
  MigrationWatchDog(double periodToReport,double stuckPeriod,
    messages::TapeserverProxy& initialProcess,
    const std::string & driveUnitName,
    log::LogContext lc, double pollPeriod = 0.1): 
  TaskWatchDog(periodToReport, stuckPeriod, initialProcess, driveUnitName, lc, 
    pollPeriod) {}
  /**
   * Notify the watchdog which file we are operating
   * @param file
   */
  void notifyBeginNewJob(const FileStruct& file){
    castor::server::MutexLocker locker(&m_mutex);
    m_file=file;
    m_fileBeingMoved=true;
  }
  
  /**
   * Notify the watchdog  we have finished operating on the current file
   */
  void fileFinished(){
    castor::server::MutexLocker locker(&m_mutex);
    m_fileBeingMoved=false;
    m_file=FileStruct();
  }
};

}}}}
