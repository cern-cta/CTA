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
namespace tapeserver {
namespace daemon {
 
class GlobalStatusReporter : private castor::tape::threading::Thread {

public:
  /**
   * Constructor
   * @param tapeserverProxy
   * @param vdqmProxy
   * @param vmgrProxy
   * @param rmcProxy
   * @param lc
   */
  GlobalStatusReporter(legacymsg::TapeserverProxy& tapeserverProxy,
          legacymsg::VdqmProxy& vdqmProxy,
          legacymsg::VmgrProxy& vmgrProxy,
          legacymsg::RmcProxy& rmcProxy,log::LogContext lc);
  
  ~GlobalStatusReporter();
  /**
   * 
   */      
  void finish();
  /**
   * Will call  VdqmProxy::assignDrive
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param mountTransactionId The mount transaction ID.
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  void reportNowOccupiedDrive(const std::string &server,const std::string &unitName, 
  const std::string &dgn, const uint32_t mountTransactionId, 
  const pid_t sessionPid);
  
  /**
   * Will call TapeserverProxy::gotWriteMountDetailsFromClient
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  void gotReadMountDetailsFromClient(const std::string &unitName,
  const std::string &vid);
  
private:
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(GlobalStatusReporter&)=0;
  };
  class ReportAssignDrive : public Report {
    const std::string server;
    const std::string unitName;
    const std::string dgn;
    const uint32_t mountTransactionId; 
    const pid_t sessionPid;
  public:
    ReportAssignDrive(const std::string &server,const std::string &unitName, 
            const std::string &dgn, const uint32_t mountTransactionId,
            const pid_t sessionPid);
    virtual ~ReportAssignDrive(){}
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportGotDetailsFromClient : public Report {
    const std::string &unitName;
    const std::string &vid;
  public:
    ReportGotDetailsFromClient(const std::string &unitName,
  const std::string &vid);
    virtual ~ReportGotDetailsFromClient(){}
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
  legacymsg::VmgrProxy& m_vmgrProxy;
  legacymsg::RmcProxy& m_rmcProxy;
  
  log::LogContext m_lc;
};

}}}}
