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


#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/utils/utils.hpp"

namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {
/**
 * Templated class for watching tape read or write operation
 */
template <class placeHolder> class TaskWatchDog : private castor::tape::threading::Thread{
  tape::threading::Mutex m_mutex;
  typedef typename ReportPackerInterface<placeHolder>::FileStruct FileStruct;
  
  /*
   *  Number of blocks we moved since the last update. Has to be atomic because it is 
   *  updated from the outside 
   */
  uint64_t m_nbOfMemblocksMoved;
  
  /*
   *  Utility member to send heartbeat notifications at a given period.
   */  
  timeval m_previousReportTime;
  
  /*
   *  When was the last time we have been notified by the tape working thread
   */
  timeval m_previousNotifiedTime;
  
  /*
   * How often to we send heartbeat notifications (in second)
   */
  const double m_periodToReport; 
  
  /*
   * How long to we have to wait before saying we are stuck (in second)
   */
  const double m_stuckPeriod; 
  
  /*
   *  Atomic flag to stop the thread's loop
   */
  castor::tape::threading::AtomicFlag m_stopFlag;
  
  /*
   *  The proxy that will receive or heartbeat notifications
   */
  messages::TapeserverProxy& m_initialProcess;
  
  /*
   *  An interace on the report packer for sending  a message to the client if we 
   *  were to be stuck
   */
  ReportPackerInterface<placeHolder>& m_reportPacker;
  
  /*
   *  The file we are operating 
   */
  FileStruct m_file;
  
  /*
   *  Is the system at _this very moment_ reading or writing on tape
   */
  bool m_fileBeingMoved;
  /*
   * Logging system  
   */
  log::LogContext m_lc;

  /**
   * Thread;s loop
   */
  void run(){
    timeval currentTime;
    while(!m_stopFlag) {
      
      //CRITICAL SECTION
      {
        tape::threading::MutexLocker locker(&m_mutex);
        castor::utils::getTimeOfDay(&currentTime);
        
        timeval diffTimeStuck = castor::utils::timevalAbsDiff(currentTime,m_previousNotifiedTime);
        double diffTimeStuckd = castor::utils::timevalToDouble(diffTimeStuck);
        //useful if we are stuck on a particular file
        if(diffTimeStuckd>m_stuckPeriod && m_fileBeingMoved){
          m_reportPacker.reportStuckOn(m_file);
          break;
        }
      }
      
      timeval diffTimeReport = castor::utils::timevalAbsDiff(currentTime,m_previousReportTime);
      double diffTimeReportd = castor::utils::timevalToDouble(diffTimeReport);

      //heartbeat to notify activity to the mother
      if(diffTimeReportd > m_periodToReport){
        tape::threading::MutexLocker locker(&m_mutex);
        m_lc.log(LOG_DEBUG,"going to report");
        m_previousReportTime=currentTime;
        m_initialProcess.notifyHeartbeat(m_nbOfMemblocksMoved);
        m_nbOfMemblocksMoved=0;
      } 
      else{
        usleep(100000);
      }
    }
  }
  
  /*
   * 
   */
  void updateStuckTime(){
    timeval tmpTime;
    castor::utils::getTimeOfDay(&tmpTime);
    m_previousNotifiedTime=tmpTime;
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
  TaskWatchDog(double periodToReport,double stuckPeriod,
         messages::TapeserverProxy& initialProcess,
         ReportPackerInterface<placeHolder>& reportPacker,
         log::LogContext lc): 
  m_nbOfMemblocksMoved(0), m_periodToReport(periodToReport),
  m_stuckPeriod(stuckPeriod), m_initialProcess(initialProcess),
  m_reportPacker(reportPacker), m_fileBeingMoved(false), m_lc(lc) {
    m_lc.pushOrReplace(log::Param("thread","Watchdog"));
    castor::utils::getTimeOfDay(&m_previousReportTime);
    updateStuckTime();
  }
  
  /**
   * notify the wtachdog a mem block has been moved
   */
  void notify(){
    tape::threading::MutexLocker locker(&m_mutex);
    updateStuckTime();
    m_nbOfMemblocksMoved++;
  }
  
  /**
   * Start the thread
   */
  void startThread(){
    start();
  }
  
  /**
   * Ask tp stop the watchdog thread and join it
   */
  void stopAndWaitThread(){
    m_stopFlag.set();
    wait();
  }
  
  /**
   * Notify the watchdog which file we are operating
   * @param file
   */
   void notifyBeginNewJob(const FileStruct& file){
     tape::threading::MutexLocker locker(&m_mutex);
     m_file=file;
     m_fileBeingMoved=true;
   }
   
   /**
    * Notify the watchdog  we have finished operating on the current file
    */
    void fileFinished(){
      tape::threading::MutexLocker locker(&m_mutex);
      m_fileBeingMoved=false;
      m_file=FileStruct();
   }
};
 
}}}}
