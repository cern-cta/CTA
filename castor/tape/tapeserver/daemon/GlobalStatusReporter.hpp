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
          log::LogContext lc);
  
  /**
   * 
   */      
  void finish();
//------------------------------------------------------------------------------
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
   * Will call VdqmProxy::releaseDrive
   */
  void releaseDrive(const std::string &server, const std::string &unitName, 
  const std::string &dgn, const bool forceUnmount, const pid_t sessionPid);
//------------------------------------------------------------------------------
  /**
   * Will call TapeserverProxy::gotWriteMountDetailsFromClient
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  void gotReadMountDetailsFromClient(const std::string &unitName,
  const std::string &vid);
  
  /**
   * Will call TapeserverProxy::tapeUnmounted and VdqmProx::tapeUnmounted()
   * 
   */
  void tapeUnmounted(const std::string &server, const std::string &unitName, 
  const std::string &dgn, const std::string &vid);
//------------------------------------------------------------------------------
    /**
   * Will call VmgrProxy::tapeMountedForRead and TapeserverProxy::tapeMountedForRead,
   * parameters TBD
   */
  void tapeMountedForRead(const std::string &server, const std::string &unitName,
  const std::string &dgn, const std::string &vid, const pid_t sessionPid);
  
  
private:
  class Report {
  protected :
    /**
     * Bunch of parameters that are globally shared amon the different reports
     */
    const std::string server;
    const std::string unitName;
    const std::string dgn;
    const uint32_t mountTransactionId; 
    const pid_t sessionPid;
  
    /**
     * Constructor for ReportAssignDrive
     */
    Report(const std::string &_server,const std::string &_unitName, 
            const std::string &_dgn, const uint32_t _mountTransactionId,
            const pid_t _sessionPid);
    
    /**
     * Constructor for ReportReleaseDrive
     */
    Report(const std::string &_server,const std::string &_unitName, 
            const std::string &_dgn,const pid_t _sessionPid);
    
    /**
     * Constructor for ReportTapeUnmounted
     */
    Report(const std::string &_server,const std::string &_unitName, 
            const std::string &_dgn);
    /**
     * Constructor for ReportGotDetailsFromClient
     */
    Report(const std::string &_unitName);
    /**
     * Temporary?? constructor for ReportTapeMountedForRead  ?
     */
    Report():server(""),unitName(""),dgn(""),mountTransactionId(0),sessionPid(0)
    {}
  public:
    virtual ~Report(){}
    virtual void execute(GlobalStatusReporter&)=0;
  };
  //vdqm proxy stuff
  class ReportAssignDrive : public Report {
  public:
    ReportAssignDrive(const std::string &server,const std::string &unitName, 
            const std::string &dgn, const uint32_t mountTransactionId,
            const pid_t sessionPid);
    virtual ~ReportAssignDrive(){}
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportReleaseDrive : public Report {
    const bool forceUnmount;
  public:
    ReportReleaseDrive(const std::string &server,const std::string &unitName, 
            const std::string &dgn, bool forceUnmount,
            const pid_t sessionPid);
    virtual ~ReportReleaseDrive(){}
    virtual void execute(GlobalStatusReporter&);
  };

  class ReportGotDetailsFromClient : public Report {
    const std::string vid;
  public:
    ReportGotDetailsFromClient(const std::string &unitName,
  const std::string &vid);
    virtual ~ReportGotDetailsFromClient(){}
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportTapeMountedForRead : public Report {
  public:
    const std::string vid;
    ReportTapeMountedForRead(const std::string &server, const std::string &unitName,
  const std::string &dgn, const std::string &vid, const pid_t sessionPid);
    virtual ~ReportTapeMountedForRead(){}
    virtual void execute(GlobalStatusReporter&);
  };
  class ReportTapeUnmounted : public Report {
     const std::string vid;
  public:
    ReportTapeUnmounted(const std::string &server,const std::string &unitName, 
            const std::string &dgn,const std::string &vid);
    virtual ~ReportTapeUnmounted(){}
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
  
  log::LogContext m_lc;
};

}}}}
