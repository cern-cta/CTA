/******************************************************************************
 *                      GlobalStatusReporter.hpp
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

#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include <memory>
#include <string>
#include <stdint.h>

namespace castor {
    namespace legacymsg{
    class VmgrProxy;
    class VdqmProxy;
    class TapeserverProxy;
    class RmcProxy;
  };
  
namespace tape {
  namespace utils{
    class TpconfigLine;
  }
namespace tapeserver {
namespace daemon {
 
class GlobalStatusReporter : private castor::tape::threading::Thread {

public:
  /**
   * COnstructor
   * @param tapeserverProxy
   * @param vdqmProxy
   * @param vmgrProxy
   * @param configLineThe configuration line of the drive we are using. 
   * Used to gather several information (unitName, dgn,...)
   * @param hostname The host name of the computer
   * @param volume The volume information from the client
   * @param lc 
   */
  GlobalStatusReporter(legacymsg::TapeserverProxy& tapeserverProxy,
    legacymsg::VdqmProxy& vdqmProxy,
    const tape::utils::TpconfigLine& configLine,
    const std::string &hostname,
    const castor::tape::tapeserver::client::ClientInterface::VolumeInfo &volume,
    log::LogContext lc);
  
  /**
   * Put into the waiting list a guard value to signal the thread we want
   * to stop
   */      
  void finish();

  /**
   * Will call TapeserverProxy::gotWriteMountDetailsFromClient
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  void gotReadMountDetailsFromClient();
  
  /**
   * Will call TapeserverProxy::tapeUnmounted and VdqmProx::tapeUnmounted()
   */
  void tapeUnmounted();
//------------------------------------------------------------------------------
  /**
   * Will call TapeserverProxy::tapeMountedForRead,
   */
  void tapeMountedForRead();
  
  //start and wait for thread to finish
  void startThreads();
  void waitThreads();
private:
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(GlobalStatusReporter&)=0;
  };

  class ReportGotDetailsFromClient : public Report {
  public:
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportTapeMountedForRead : public Report {
  public:
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportTapeUnmounted : public Report {
  public:
    virtual void execute(GlobalStatusReporter&);
  };
  /**
   * Inherited from Thread, it will do the job : pop a request, execute it 
   * and delete it
   */
  virtual void run();
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  castor::tape::threading::BlockingQueue<Report*> m_fifo;
  
  /**
   A bunch of references to proxies to send messages to the 
   * outside world when we have to
   */
  legacymsg::TapeserverProxy& m_tapeserverProxy;
  legacymsg::VdqmProxy& m_vdqmProxy;
  
  /**
   * Log context, copied because it is in a separated thread
   */
  log::LogContext m_lc;
  
  const std::string m_server;
  const std::string m_unitName;
  const std::string m_dgn;
  const castor::tape::tapeserver::client::ClientInterface::VolumeInfo m_volume;
  const pid_t m_sessionPid;
};

}}}}
