/******************************************************************************
 *                      TaskWatchDog.hpp
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

template <class placeHolder> class TaskWatchDog : private castor::tape::threading::Thread{
  typedef typename ReportPackerInterface<placeHolder>::FileStruct FileStruct;
  
  castor::tape::threading::AtomicCounter<uint64_t> m_nbOfMemblocksMoved;
  timeval m_previousReportTime;
  castor::tape::threading::AtomicVariable<timeval> m_previousNotifiedTime;
  const double m_periodToReport; //in second
  const double m_stuckPeriod; //in second
  castor::tape::threading::AtomicFlag m_stopFlag;
  messages::TapeserverProxy& m_initialProcess;
  ReportPackerInterface<placeHolder>& m_reportPacker;
  FileStruct m_file;
  bool m_fileBeingMoved;
  log::LogContext m_lc;
  
  void run(){
    timeval currentTime;
    while(!m_stopFlag) {
      castor::utils::getTimeOfDay(&currentTime);
      
      timeval diffTimeStuck = castor::utils::timevalAbsDiff(currentTime,m_previousNotifiedTime);
      double diffTimeStuckd = castor::utils::timevalToDouble(diffTimeStuck);
      if(diffTimeStuckd>m_stuckPeriod && m_fileBeingMoved){
        m_reportPacker.reportStuckOn(m_file);
        break;
      }
      
      timeval diffTimeReport = castor::utils::timevalAbsDiff(currentTime,m_previousReportTime);
      double diffTimeReportd = castor::utils::timevalToDouble(diffTimeReport);
      if(diffTimeReportd > m_periodToReport){
        m_lc.log(LOG_DEBUG,"going to report");
        m_previousReportTime=currentTime;
        m_initialProcess.notifyHeartbeat(m_nbOfMemblocksMoved.getAndReset());
      } 
      else{
        usleep(100000);
      }
    }
  }
  void updateStuckTime(){
    timeval tmpTime;
    castor::utils::getTimeOfDay(&tmpTime);
    m_previousNotifiedTime=tmpTime;
  }
  public:
  TaskWatchDog(messages::TapeserverProxy& initialProcess,
         ReportPackerInterface<placeHolder>& reportPacker ,log::LogContext lc): 
  m_nbOfMemblocksMoved(0),m_periodToReport(2),m_stuckPeriod(60*10),
          m_initialProcess(initialProcess),m_reportPacker(reportPacker),
          m_fileBeingMoved(false),m_lc(lc){
    m_lc.pushOrReplace(log::Param("thread","Watchdog"));
    castor::utils::getTimeOfDay(&m_previousReportTime);
    updateStuckTime();
  }
  void notify(){
    updateStuckTime();
    m_nbOfMemblocksMoved++;
  }
  void startThread(){
    start();
  }
  void stopThread(){
    m_stopFlag.set();
    wait();
  }
   void notifyBeginNewJob(const FileStruct& file){
     m_file=file;
     m_fileBeingMoved=true;
   }
    void fileFinished(){
      m_fileBeingMoved=false;
   }
};
 
}}}}
