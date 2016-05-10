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
    Failed,     ///< The previous process failed to unmount the tape and returned properly
    Killed,     ///< The previous process was killed by the watchdog. A cleanup will be run beforehand.
    Crashed     ///< The previous process was killed or crashed for another reason. A cleanup will be run beforehand.
  };
  /** Representation of the outcome of the previous session/child process. */
  PreviousSession m_previousSession=PreviousSession::Initiating;
public:
  /** Possible states for the current session. */
  enum class SessionState: uint32_t  {
    PendingFork, ///< The subprocess is not started yet.
    Cleaning,   ///< The subprocess is cleaning up tape left in drive.
    Scheduling, ///< The subprocess is determining what to do next.
    Mounting,   ///< The subprocess is mounting the tape.
    Running,    ///< The subprocess is running the data transfer.
    Unmounting, ///< The subprocess is unmounting the tape.
    DrainingToDisk, ///< The subprocess is flushing the memory to disk (retrieves only)
    ClosingDown, ///< The subprocess completed all tasks and will exit
    Shutdown    ///< The subprocess is finished after a shutdown was requested. 
  };
private:
  /** Session state to string */
  std::string toString(SessionState state);
  /** Representation of the status of the current process. */
  SessionState m_sessionState=SessionState::PendingFork;
  /** Helper function to handle Scheduling state report */
  SubprocessHandler::ProcessingStatus processScheduling(serializers::WatchdogMessage & message);
  /** Helper function to handle Mounting state report */
  SubprocessHandler::ProcessingStatus processMounting(serializers::WatchdogMessage & message);
  /** Helper function to handle Running state report */
  SubprocessHandler::ProcessingStatus processRunning(serializers::WatchdogMessage & message);
  /** Helper function to handle Unmounting state report */
  SubprocessHandler::ProcessingStatus processUnmounting(serializers::WatchdogMessage & message);
  /** Helper function to handle DraingingToDisk state report */
  SubprocessHandler::ProcessingStatus processDrainingToDisk(serializers::WatchdogMessage & message);
  /** Helper function to handle ClosingDown state report */
  SubprocessHandler::ProcessingStatus processClosingDown(serializers::WatchdogMessage & message);
public:
  /** Representation of the session direction */
  enum class SessionType: uint32_t {
    Undetermined, ///< The initial direction for the session is undetermined.
    Archive,      ///< Direction is disk to tape.
    Retrieve      ///< Direction is tape to disk.
  };
private:
  /** Session type to string */
  std::string toString(SessionType type);
  /** Current session's type */
  SessionType m_sessionType=SessionType::Undetermined;
  /** Current session's parameters: they are accumulated during session's lifetime
   * and logged as session ends */
  log::LogContext m_sessionEndContext;
  /** The current state we report to process manager */
  SubprocessHandler::ProcessingStatus m_processingStatus;
  /** Convenience type */
  typedef std::chrono::milliseconds Timeout;
  /** Values for the heartbeat or completion timeouts where applicable */
  static const std::map<SessionState, Timeout> m_timeouts;
  /** Special timeout for data movement timeout during running */
  static const Timeout m_dataMovementTimeout;
  /** When did we see the last block movement? */
  std::chrono::time_point<std::chrono::steady_clock> m_lastBlockMovementTime=decltype(m_lastBlockMovementTime)::min();
  /** Computation of the next timeout (depending on the state) */
  decltype (SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout();
  /** PID for the subprocess */
  pid_t m_pid=-1;
  /** Socket pair allowing communication with the subprocess */
  std::unique_ptr<cta::server::SocketPair> m_socketPair;
  /** Helper function accumulating logs */
  void processLogs(serializers::WatchdogMessage & message);
};

// TODO: remove/merge ChildProcess.

}}} // namespace cta::tape::daemon