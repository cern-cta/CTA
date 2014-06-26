/******************************************************************************
 *                      TaskWatchDog.cpp
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

#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/messages.hpp"
#include "castor/utils/utils.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {
    
//void TaskWatchDog::run(){
//  timeval currentTime;
//  while(!m_stopFlag) {
//    castor::utils::getTimeOfDay(&currentTime);
//
//    timeval diffTimeStuck = castor::utils::timevalAbsDiff(currentTime,m_previousNotifiedTime);
//    double diffTimeStuckd = castor::utils::timevalToDouble(diffTimeStuck);
//    if(diffTimeStuckd>m_stuckPeriod){
//      
//      m_reportPacker.reportStuckOn(file);
//    }
//
//    timeval diffTimeReport = castor::utils::timevalAbsDiff(currentTime,m_previousReportTime);
//    double diffTimeReportd = castor::utils::timevalToDouble(diffTimeReport);
//    if(diffTimeReportd > m_periodToReport){
//      m_lc.log(LOG_DEBUG,"going to report");
//      m_previousReportTime=currentTime;
//      m_initialProcess.notifyHeartbeat(m_nbOfMemblocksMoved.getAndReset());
//    } 
//    else{
//      usleep(100000);
//    }
//  }
//}
//    
//TaskWatchDog::TaskWatchDog(messages::TapeserverProxy& initialProcess,
//       ReportPackerInterface<placeHolder>& reportPacker ,log::LogContext lc): 
//m_nbOfMemblocksMoved(0),m_periodToReport(2),m_stuckPeriod(60*10),
//        m_initialProcess(initialProcess),m_reportPacker(reportPacker),m_lc(lc){
//  m_lc.pushOrReplace(log::Param("thread","Watchdog"));
//  castor::utils::getTimeOfDay(&m_previousReportTime);
//}
//void TaskWatchDog::notify(){
//  timeval tmpTime;
//  castor::utils::getTimeOfDay(&tmpTime);
//  m_previousNotifiedTime=tmpTime;
//  m_nbOfMemblocksMoved++;
//}
//void TaskWatchDog::startThread(){
//  start();
//}
//void TaskWatchDog::stopThread(){
//  m_stopFlag.set();
//  wait();
//}
// void TaskWatchDog::notifyBeginNewJob(const FileStruct& file){
//   m_file=file;
// }
//  void TaskWatchDog::fileFinished(){
//   m_file=file;
// }
}}}}
