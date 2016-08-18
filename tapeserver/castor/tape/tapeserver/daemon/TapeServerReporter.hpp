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

#include "castor/server/Threading.hpp"
#include "castor/server/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/log/LogContext.hpp"
#include <memory>
#include <string>
#include <stdint.h>

namespace castor {
namespace messages{
    class TapeserverProxy;
  }
namespace tape {
namespace tapeserver {
namespace daemon {
  
class TapeServerReporter : private castor::server::Thread {

public:
  /**
   * COnstructor
   * @param tapeserverProxy
   * @param driveConfig The configuration of the tape drive we are using.
   * @param hostname The host name of the computer
   * @param volume The volume information from the client
   * @param lc 
   */
  TapeServerReporter(
    cta::daemon::TapedProxy& tapeserverProxy,
    const DriveConfig &driveConfig,
    const std::string &hostname,
    const castor::tape::tapeserver::daemon::VolumeInfo &volume,
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
  
  void tapeMountedForWrite();
  
  //The following function could be split into 2 parts 
  /**
   * Notify the client we got the fseq of the first file to transfer we get in
   * exchange return the number of files on the tape according to the VMGR
   * @return the number of files on the tape according to the VMGR
   */
  uint32_t gotWriteMountDetailsFromClient();
  
  //start and wait for thread to finish
  void startThreads();
  void waitThreads();
    
private:
  bool m_threadRunnig;
  /*
  This internal mechanism could (should ?) be easily changed to a queue 
   * of {std/boost}::function coupled with bind. For instance, tapeMountedForWrite
   * should look like 
   *   m_fifo.push(bind(m_tapeserverProxy,&tapeMountedForWrite,args...))
   * and execute
   *  while(1)
   *   (m_fifo.push())();
   * But no tr1 neither boost, so, another time ...
  */
  
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(TapeServerReporter&)=0;
  };

  class ReportGotReadDetailsFromClient : public Report {
  public:
    virtual void execute(TapeServerReporter&);
  };
  class ReportTapeMountedForRead : public Report {
  public:
    virtual void execute(TapeServerReporter&);
  };
  class ReportTapeUnmounted : public Report {
  public:
    virtual void execute(TapeServerReporter&);
  };
  class ReportTapeMountedForWrite : public Report {
  public:
    virtual void execute(TapeServerReporter&);
  };
  /**
   * Inherited from Thread, it will do the job : pop a request, execute it 
   * and delete it
   */
  virtual void run();
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  castor::server::BlockingQueue<Report*> m_fifo;
  
  /**
   A bunch of references to proxies to send messages to the 
   * outside world when we have to
   */
  cta::daemon::TapedProxy& m_tapeserverProxy;
  
  /**
   * Log context, copied because it is in a separated thread
   */
  log::LogContext m_lc;

  const std::string m_server;
  const std::string m_unitName;
  const std::string m_logicalLibrary;
  const castor::tape::tapeserver::daemon::VolumeInfo m_volume;
  const pid_t m_sessionPid;

};

}}}}
