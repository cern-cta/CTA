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


#include "zmq/ZmqWrapper.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/messages/TapeserverProxy.hpp"
namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {

class TaskWatchDog : private castor::tape::threading::Thread{
protected:
    castor::tape::threading::AtomicCounter<uint64_t> nbOfMemblocksMoved;
    timeval previousTime;
    const double periodToReport; //in second
    castor::tape::threading::AtomicFlag m_stopFlag;
    messages::TapeserverProxy& m_initialProcess;
    log::LogContext m_lc;
    
    void report(zmq::Socket& m_socket);
    
    virtual void run();
    
  public:
    TaskWatchDog(messages::TapeserverProxy& initialProcess,log::LogContext lc);
    void notify();
    void startThread();
    void stopThread();

  };
 
}}}}
