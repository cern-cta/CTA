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

#include <memory>

#include "common/threading/SocketPair.hpp"
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Session.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/SubprocessHandler.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

namespace castor {
namespace tape {
namespace System {
class realWrapper;
}
namespace tapeserver {
namespace daemon {
class CleanerSession;
class DataTransferSession;
// class TapedProxy;
}
}
}
}

namespace cta {

class IScheduler;

namespace catalogue {
class Catalogue;
}

namespace mediachanger {
class MediaChangerFacade;
}

namespace server {
class ProcessCap;
}

namespace tape {
namespace daemon {

class DriveHandlerProxy;

/**
 * Handler for tape drive session subprocesses. On process/session will handle
 * at most one tape mount. Child process will either return a clean/unclean
 */
class DriveHandler : public SubprocessHandler {
public:
  DriveHandler(const TapedConfiguration& tapedConfig, const TpconfigLine& driveConfig, ProcessManager& pm);

  ~DriveHandler() override = default;

  SubprocessHandler::ProcessingStatus getInitialStatus() override;

  SubprocessHandler::ProcessingStatus fork() override;

  void postForkCleanup() override;

  int runChild() override;

  SubprocessHandler::ProcessingStatus shutdown() override;

  void kill() override;

  SubprocessHandler::ProcessingStatus processEvent() override;

  SubprocessHandler::ProcessingStatus processSigChild() override;

  SubprocessHandler::ProcessingStatus processTimeout() override;

private:
  /** Reference to the process manager*/
  cta::tape::daemon::ProcessManager& m_processManager;
  /** The parameters */
  const TapedConfiguration& m_tapedConfig;
  /** This drive's parameters */
  const TpconfigLine& m_driveConfig;
  /** The log context */
  cta::log::LogContext* m_lc;
  
public:
  /** Possible outcomes of the previous session/child process.  */
  enum class PreviousSession {
    Initiating, ///< The process is the first to run after daemon startup. A cleanup will be run beforehand.
    Up,         ///< The previous process unmounted the tape properly and hardware is ready for another session.
    Down,       ///< The previous session tried and failed to unmount the tape, and reported such instance.
    Crashed     ///< The previous process was killed or crashed. The next session will be a cleanup.
  };

protected:
  /** Representation of the outcome of the previous session/child process. */
  PreviousSession m_previousSession = PreviousSession::Initiating;
  /** Representation of the last know state of the previous session (useful for crashes) */
  session::SessionState m_previousState = session::SessionState::StartingUp;
  /** Representation of the last know type of the previous session (useful for crashes) */
  session::SessionType m_previousType = session::SessionType::Undetermined;
  /** Previous VID, that can help the unmount process */
  std::string m_previousVid;

private:
  /** Representation of the status of the current process. */
  session::SessionState m_sessionState = session::SessionState::PendingFork;

  void setDriveDownForShutdown(const std::string& reason);

  /**
   * Utility function resetting all parameters to pre-fork state
   * @param previousSessionState outcome to be considered for the next run.
   */
  void resetToDefault(PreviousSession previousSessionState);

private:
  /** Current session's type */
  session::SessionType m_sessionType = session::SessionType::Undetermined;

  /** Current session's VID */
  std::string m_sessionVid;
  /** Current session's parameters: they are accumulated during session's lifetime
   * and logged as session ends */
  log::LogContext m_sessionEndContext;
  /** The current state we report to process manager */
  SubprocessHandler::ProcessingStatus m_processingStatus;
  /** Convenience type */
  typedef std::chrono::milliseconds Timeout;
  /** Values for the state change timeouts where applicable */
  static std::map<session::SessionState, Timeout> m_stateChangeTimeouts;
  /** Values for the heartbeat timeouts, where applicable */
  static const std::map<session::SessionState, Timeout> m_heartbeatTimeouts;
  /** Values for the data movement timeouts, where applicable */
  static const std::map<session::SessionState, Timeout> m_dataMovementTimeouts;
  /** When did we see the last state change? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastStateChangeTime = std::chrono::steady_clock::now();
  /** When did we see the last heartbeat change? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastHeartBeatTime = std::chrono::steady_clock::now();
  /** When did we see the last data movement? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastDataMovementTime = std::chrono::steady_clock::now();
  /** Type of the currently active timeout */
  std::string m_timeoutType;
  /** Session type when currently active timeout was set */
  session::SessionType m_sessionTypeWhenTimeoutDecided;
  /** State when current timeout was set */
  session::SessionState m_sessionStateWhenTimeoutDecided;

  /** Computation of the next timeout (depending on the state) */
  decltype(SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout();

  /** How much data did we see moving the session so far? (tape) */
  uint64_t m_totalTapeBytesMoved = 0;
  /** How much data did we see moving the session so far? (disk) */
  uint64_t m_totalDiskBytesMoved = 0;
  /** PID for the subprocess */
  pid_t m_pid = -1;
  /** Socket pair allowing communication with the subprocess */
  std::unique_ptr<cta::server::SocketPair> m_socketPair;

  /** Helper function accumulating logs */
  void processLogs(serializers::WatchdogMessage& message);

  /** Helper function accumulating bytes transferred */
  void processBytes(serializers::WatchdogMessage& message);

protected:
  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;

  std::unique_ptr<OStoreDBInit> m_sched_db_init;
  std::unique_ptr<SchedulerDB_t> m_sched_db;

  virtual std::shared_ptr<cta::catalogue::Catalogue> createCatalogue(const std::string& processName) const;
  virtual std::shared_ptr<cta::IScheduler> createScheduler(const std::string& processName,
    const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount);

  virtual std::shared_ptr<cta::tape::daemon::TapedProxy> createDriveHandlerProxy() const;

  virtual castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeCleanerSession(
    cta::IScheduler* scheduler) const;

  virtual castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeDataTransferSession(
    cta::IScheduler* scheduler, cta::tape::daemon::TapedProxy* driveHandlerProxy) const;
};

// TODO: remove/merge ChildProcess.

}
}
} // namespace cta::tape::daemon
