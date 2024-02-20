/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "VolumeInfo.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "daemon/TapedProxy.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"
#include <memory>
#include <stdint.h>
#include <string>

namespace castor::tape::tapeserver::daemon {

class TapeSessionReporter : private cta::threading::Thread {

public:
  /**
   * Constructor
   * @param tapeserverProxy
   * @param driveConfig The configuration of the tape drive we are using.
   * @param hostname The host name of the computer
   * @param lc 
   */
  TapeSessionReporter(cta::tape::daemon::TapedProxy& tapeserverProxy, const cta::tape::daemon::TpconfigLine& driveConfig,
    std::string_view hostname, const cta::log::LogContext& lc);

  /**
   * Put into the waiting list a guard value to signal the thread we want
   * to stop
   */
  void finish();

  /**
   * Consume the waiting list and exit
   */
  void bailout();

  /**
   * Will call TapedProxy::reportState();
   */
  void reportState(cta::tape::session::SessionState state,
                   cta::tape::session::SessionType type);

  void setVolInfo(const castor::tape::tapeserver::daemon::VolumeInfo& volumeInfo) { m_volume = volumeInfo; };

  /**
   * Start and wait for thread to finish
   */
  void startThreads();

  void waitThreads();

private:
  bool m_threadRunning;

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
    virtual ~Report() = default;

    virtual void execute(TapeSessionReporter &) = 0;
  };

  class ReportStateChange : public Report {
  public:
    ReportStateChange(cta::tape::session::SessionState state,
                      cta::tape::session::SessionType type);

    void execute(TapeSessionReporter &) override;

  private:
    cta::tape::session::SessionState m_state;
    cta::tape::session::SessionType m_type;
  };

  /**
   * Inherited from Thread, it will do the job : pop a request, execute it 
   * and delete it
   */
  void run() override;

  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  cta::threading::BlockingQueue<Report *> m_fifo;

  /**
   A bunch of references to proxies to send messages to the 
   * outside world when we have to
   */
  cta::tape::daemon::TapedProxy& m_tapeserverProxy;

  /**
   * Log context, copied because it is in a separated thread
   */
  cta::log::LogContext m_lc;

  const std::string m_server;
  const std::string m_unitName;
  const std::string m_logicalLibrary;
  castor::tape::tapeserver::daemon::VolumeInfo m_volume;
  const pid_t m_sessionPid;
};

} // namespace castor::tape::tapeserver::daemon
