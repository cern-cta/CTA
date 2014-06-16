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

#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"
#include "castor/tape/utils/utils.hpp"
namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {
class TaskWatchDog {
    uint64_t nbOfMemblocksMoved;
    timeval previousTime;
    static const double periodToReport = 55.; //in second
    TapeServerReporter& watcher;
  public:
    TaskWatchDog(TapeServerReporter& reportToWhom):
    nbOfMemblocksMoved(0){
      castor::utils::getTimeOfDay(&previousTime);
    }
    void notify(){
      nbOfMemblocksMoved++;
    }
    
    void run(){
      using castor::utils::timevalToDouble;
      using castor::utils::timevalAbsDiff;
      timeval currentTime;
      while(1) {
        castor::utils::getTimeOfDay(&currentTime);
        timeval diffTime = timevalAbsDiff(currentTime,previousTime);
        if(timevalToDouble(diffTime) > periodToReport){
          previousTime=currentTime;
          watcher.notifyWatchdog(nbOfMemblocksMoved);
        }
      }
    }
  };
  
}}}}