/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "SubprocessHandler.hpp"
#include "ProcessManager.hpp"
#include "TapedConfiguration.hpp"
#include "common/threading/SocketPair.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"
#include <memory>

namespace cta { namespace tape { namespace  daemon {

/**
 * Handler for tape drive session subprocesses. On process/session will handle
 * at most one tape mount. Child process will either return a clean/unclean
 */
class DriveHandler: public SubprocessHandler {
public:
  DriveHandler(const TapedConfiguration & tapedConfig, const TpconfigLine & configline, ProcessManager & pm);
  virtual ~DriveHandler();
  SubprocessHandler::ProcessingStatus getInitialStatus() override;
  SubprocessHandler::ProcessingStatus fork() override;
  void prepareForFork() override;
  int runChild() override;
  SubprocessHandler::ProcessingStatus shutdown() override;
  void kill() override;
  SubprocessHandler::ProcessingStatus processEvent() override;
  SubprocessHandler::ProcessingStatus processSigChild() override;
  SubprocessHandler::ProcessingStatus processTimeout() override;
private:
  /** Reference to the process manager*/
  cta::tape::daemon::ProcessManager & m_processManager;
  /** The parameters */
  const TapedConfiguration & m_tapedConfig;
  /** This drive's parameters */
  const TpconfigLine & m_configLine;
  /** Possible outcomes of the previous session/child process.  */
  enum class PreviousSession {
    Initiating, ///< The process is the first to run after daemon startup. A cleanup will be run beforehand.
    OK,         ///< The previous process unmounted the tape properly and hardware is ready for another session
    Crashed     ///< The previous process was killed or crashed for another reason. A cleanup will be run beforehand.
  };
  /** Representation of the outcome of the previous session/child process. */
  PreviousSession m_previousSession=PreviousSession::Initiating;
  /** Previous VID, that can help the unmount process */
  std::string m_previousVid;
  /** Representation of the status of the current process. */
  session::SessionState m_sessionState=session::SessionState::PendingFork;
  /**
   * Utility function resetting all parameters to pre-fork state
   * @param previousSessionState outcome to be considered for the next run.
   */
  void resetToDefault(PreviousSession previousSessionState);
  /** Helper function to handle Scheduling state report */
  SubprocessHandler::ProcessingStatus processStartingUp(serializers::WatchdogMessage & message);
  /** Helper function to handle Scheduling state report */
  SubprocessHandler::ProcessingStatus processScheduling(serializers::WatchdogMessage & message);
  /** Helper function to handle Checking state report */
  SubprocessHandler::ProcessingStatus processChecking(serializers::WatchdogMessage & message);
  /** Helper function to handle Mounting state report */
  SubprocessHandler::ProcessingStatus processMounting(serializers::WatchdogMessage & message);
  /** Helper function to handle Running state report */
  SubprocessHandler::ProcessingStatus processRunning(serializers::WatchdogMessage & message);
  /** Helper function to handle Unmounting state report */
  SubprocessHandler::ProcessingStatus processUnmounting(serializers::WatchdogMessage & message);
  /** Helper function to handle DraingingToDisk state report */
  SubprocessHandler::ProcessingStatus processDrainingToDisk(serializers::WatchdogMessage & message);
  /** Helper function to handle ClosingDown state report */
  SubprocessHandler::ProcessingStatus processShutingDown(serializers::WatchdogMessage & message);
  /** Current session's type */
  session::SessionType m_sessionType=session::SessionType::Undetermined;
  /** Current session's parameters: they are accumulated during session's lifetime
   * and logged as session ends */
  log::LogContext m_sessionEndContext;
  /** The current state we report to process manager */
  SubprocessHandler::ProcessingStatus m_processingStatus;
  /** Convenience type */
  typedef std::chrono::milliseconds Timeout;
  /** Values for the state change timeouts where applicable */
  static const std::map<session::SessionState, Timeout> m_stateChangeTimeouts;
  /** Values for the heartbeat timeouts, where applicable */
  static const std::map<session::SessionState, Timeout> m_heartbeatTimeouts;
  /** Values for the data movement timeouts, where applicable */
  static const std::map<session::SessionState, Timeout> m_dataMovementTimeouts;
  /** When did we see the last state change? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastStateChangeTime=decltype(m_lastStateChangeTime)::min();
  /** When did we see the last heartbeat change? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastHeartBeatTime=decltype(m_lastHeartBeatTime)::min();
  /** When did we see the last data movement? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastDataMovementTime=decltype(m_lastDataMovementTime)::min();
  /** Computation of the next timeout (depending on the state) */
  decltype (SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout();
  /** How much data did we see moving the session so far? (tape) */
  uint64_t m_totalTapeBytesMoved=0;
  /** How much data did we see moving the session so far? (disk) */
  uint64_t m_totalDiskBytesMoved=0;
  /** PID for the subprocess */
  pid_t m_pid=-1;
  /** Socket pair allowing communication with the subprocess */
  std::unique_ptr<cta::server::SocketPair> m_socketPair;
  /** Helper function accumulating logs */
  void processLogs(serializers::WatchdogMessage & message);
};

// TODO: remove/merge ChildProcess.

}}} // namespace cta::tape::daemon