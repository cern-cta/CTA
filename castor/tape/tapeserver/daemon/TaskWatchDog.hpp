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
#include "castor/log/LogContext.hpp"
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/utils/utils.hpp"

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
   *  Timer for regular heartbeat reports to parent process
   */  
  castor::tape::utils::Timer m_reportTimer;
  
  /*
   *  How long since we last logged a warning?
   */
  castor::tape::utils::Timer m_blockMovementReportTimer;
  
  /**
   * how long since the last block movement?
   */
  castor::tape::utils::Timer m_blockMovementTimer;
  
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
   * Thread's loop
   */
  void run(){
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

      //heartbeat to notify activity to the mother
      if(m_reportTimer.secs() > m_reportPeriod){
        castor::server::MutexLocker locker(&m_mutex);
        m_lc.log(LOG_DEBUG,"going to report");
        m_reportTimer.reset();
        m_initialProcess.notifyHeartbeat(m_driveUnitName, m_nbOfMemblocksMoved);
        m_nbOfMemblocksMoved=0;
      } 
      else{
        usleep(m_pollPeriod*1000*1000);
      }
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
  m_nbOfMemblocksMoved(0), m_pollPeriod(pollPeriod),
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
    params.addTiming("TimeSinceLastBlockMove", m_blockMovementTimer.usecs())
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
