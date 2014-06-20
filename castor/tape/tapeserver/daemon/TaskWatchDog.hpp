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
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "zmq/castorZmqWrapper.hpp"
#include "zmq/castorZmqUtils.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {

class TaskWatchDog : private castor::tape::threading::Thread{
    castor::tape::threading::AtomicCounter<uint64_t> nbOfMemblocksMoved;
    timeval previousTime;
    const double periodToReport; //in second
    castor::tape::threading::AtomicFlag m_stopFlag;

    log::LogContext m_lc;
    zmq::context_t m_ctx;
     
    void report(zmq::socket_t& m_socket){
      try
      {
        castor::messages::Header header;
        header.set_magic(TPMAGIC);
        header.set_protocoltype(messages::protocolType::Tape);
        header.set_protocolversion(castor::messages::protocolVersion::prototype);
        header.set_reqtype(messages::reqType::Heartbeat);
        header.set_bodyhashtype("SHA1");
        header.set_bodyhashvalue("PIPO");
        header.set_bodysignature("PIPO");
        header.set_bodysignaturetype("SHA1");

        castor::utils::sendMessage(m_socket,header,ZMQ_SNDMORE);
        
        castor::messages::Heartbeat body;
        body.set_bytesmoved(nbOfMemblocksMoved.getAndReset());
        castor::utils::sendMessage(m_socket,body);
        m_lc.log(LOG_INFO,"Notified MF");
        zmq::message_t blob;
        m_socket.recv(&blob);
      }catch(const zmq::error_t& e){
        log::ScopedParamContainer c(m_lc);
        c.add("ex code",e.what());
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
    void run(){
      GOOGLE_PROTOBUF_VERIFY_VERSION;
      zmq::socket_t m_socket(m_ctx,ZMQ_REQ);
      castor::utils::connectToLocalhost(m_socket);
      
      using castor::utils::timevalToDouble;
      using castor::utils::timevalAbsDiff;
      timeval currentTime;
      while(!m_stopFlag) {
        castor::utils::getTimeOfDay(&currentTime);
        timeval diffTime = timevalAbsDiff(currentTime,previousTime);
        double diffTimed = timevalToDouble(diffTime);
        log::ScopedParamContainer c(m_lc);
        c.add("diff.s",diffTime.tv_sec).add("diff.us",diffTime.tv_usec)
         .add("double diff",diffTimed).add("period",periodToReport);
        if( diffTimed> periodToReport){
          m_lc.log(LOG_INFO,"going to report");
          previousTime=currentTime;
          report(m_socket);
        }
      }
    }
    
  public:
    TaskWatchDog(log::LogContext lc): 
    nbOfMemblocksMoved(0),periodToReport(2),m_lc(lc),m_ctx(){
      m_lc.pushOrReplace(log::Param("thread","Watchdog"));
      castor::utils::getTimeOfDay(&previousTime);
    }
    void notify(){
      nbOfMemblocksMoved++;
    }
    void startThread(){
      start();
    }
    void stopThread(){
      m_stopFlag.set();
      wait();
    }

  };
  
}}}}