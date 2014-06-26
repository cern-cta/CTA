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
    
void TaskWatchDog::run(){
  timeval currentTime;
  while(!m_stopFlag) {
    castor::utils::getTimeOfDay(&currentTime);
    timeval diffTime = castor::utils::timevalAbsDiff(currentTime,previousTime);
    double diffTimed = castor::utils::timevalToDouble(diffTime);
    if(diffTimed > periodToReport){
      m_lc.log(LOG_DEBUG,"going to report");
      previousTime=currentTime;
      m_initialProcess.notifyHeartbeat(nbOfMemblocksMoved.getAndReset());
    }else{
      usleep(100000);
    }
  }
}
    
TaskWatchDog::TaskWatchDog(messages::TapeserverProxy& initialProcess,log::LogContext lc): 
nbOfMemblocksMoved(0),periodToReport(2),
        m_initialProcess(initialProcess),m_lc(lc){
  m_lc.pushOrReplace(log::Param("thread","Watchdog"));
  castor::utils::getTimeOfDay(&previousTime);
}
void TaskWatchDog::notify(){
  nbOfMemblocksMoved++;
}
void TaskWatchDog::startThread(){
  start();
}
void TaskWatchDog::stopThread(){
  m_stopFlag.set();
  wait();
}

}}}}
