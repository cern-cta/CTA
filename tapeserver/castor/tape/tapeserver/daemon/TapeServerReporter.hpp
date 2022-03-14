/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/threading/Thread.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "VolumeInfo.hpp"
#include "common/log/LogContext.hpp"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include <memory>
#include <string>
#include <stdint.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

class TapeServerReporter : private cta::threading::Thread {

public:
  /**
   * Constructor
   * @param tapeserverProxy
   * @param driveConfig The configuration of the tape drive we are using.
   * @param hostname The host name of the computer
   * @param volume The volume information from the client
   * @param lc 
   */
  TapeServerReporter(
    cta::tape::daemon::TapedProxy& tapeserverProxy,
    const cta::tape::daemon::TpconfigLine& driveConfig,
    const std::string& hostname,
    const castor::tape::tapeserver::daemon::VolumeInfo& volume,
    cta::log::LogContext lc);

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

  /**
   * Special function managing the special case of retrieves, where disk and
   * tape thread can finish in different orders (tape part)
   */
  void reportTapeUnmountedForRetrieve();

  /**
   * Special function managing the special case of retrieves, where disk and
   * tape thread can finish in different orders (disk part)
   */
  void reportDiskCompleteForRetrieve();

//------------------------------------------------------------------------------
  //start and wait for thread to finish
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

    virtual void execute(TapeServerReporter&) = 0;
  };

  class ReportStateChange : public Report {
  public:
    ReportStateChange(cta::tape::session::SessionState state,
                      cta::tape::session::SessionType type);

    void execute(TapeServerReporter&) override;

  private:
    cta::tape::session::SessionState m_state;
    cta::tape::session::SessionType m_type;
  };

  class ReportTapeUnmountedForRetrieve : public Report {
  public:
    void execute(TapeServerReporter&) override;
  };

  class ReportDiskCompleteForRetrieve : public Report {
  public:
    void execute(TapeServerReporter&) override;
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

  /**
   * Boolean allowing the management of the special case of recall where
   * end of tape and disk threads can happen in any order (tape side)
   */
  bool m_tapeUnmountedForRecall = false;

  /**
   * Boolean allowing the management of the special case of recall where
   * end of tape and disk threads can happen in any order (disk side)
   */
  bool m_diskCompleteForRecall = false;

  const std::string m_server;
  const std::string m_unitName;
  const std::string m_logicalLibrary;
  const castor::tape::tapeserver::daemon::VolumeInfo m_volume;
  const pid_t m_sessionPid;

};

}
}
}
}
