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

namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {

void TaskWatchDog::report(zmq::Socket& m_socket){
  try
  {
    messages::Header header = messages::preFillHeader();
    header.set_reqtype(messages::reqType::Heartbeat);
    header.set_bodyhashvalue("PIPO");
    header.set_bodysignature("PIPO");
    
    messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
    messages::Heartbeat body;
    body.set_bytesmoved(nbOfMemblocksMoved.getAndReset());
    messages::sendMessage(m_socket,body);
    
    m_lc.log(LOG_INFO,"Notified MF");
    zmq::Message blob;
    m_socket.recv(blob);
    log::ScopedParamContainer c(m_lc);
    c.add("size",blob.size());
    m_lc.log(LOG_INFO,"debug purpose");
  }catch(const castor::exception::Exception& e){
    log::ScopedParamContainer c(m_lc);
    c.add("ex code",e.getMessageValue());
    m_lc.log(LOG_ERR,"Error with the ZMQ socket while reporting to the MF");
  }
  catch(const std::exception& e){
    log::ScopedParamContainer c(m_lc);
    c.add("ex code",e.what());
    m_lc.log(LOG_ERR,"Exception while notifying MF");
  }catch(...){
    m_lc.log(LOG_INFO,"triple ...");
  }
}
    
void TaskWatchDog::run(){
  zmq::Socket m_socket(DataTransferSession::ctx(),ZMQ_REQ);
  castor::messages::connectToLocalhost(m_socket);
  
  using castor::utils::timevalToDouble;
  using castor::utils::timevalAbsDiff;
  timeval currentTime;
  while(!m_stopFlag) {
    castor::utils::getTimeOfDay(&currentTime);
    timeval diffTime = timevalAbsDiff(currentTime,previousTime);
    double diffTimed = timevalToDouble(diffTime);
    if( diffTimed> periodToReport){
      m_lc.log(LOG_DEBUG,"going to report");
      previousTime=currentTime;
      report(m_socket);
    }
  }
}
    
TaskWatchDog::TaskWatchDog(log::LogContext lc): 
nbOfMemblocksMoved(0),periodToReport(2),m_lc(lc){
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

DummyTaskWatchDog::DummyTaskWatchDog(log::LogContext& lc):
  TaskWatchDog(lc){
}

void DummyTaskWatchDog::run(){
  while(!m_stopFlag){
    sleep(1);
  }
}

}}}}
