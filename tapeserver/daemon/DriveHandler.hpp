/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "common/threading/SocketPair.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif
#include "scheduler/Scheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Session.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/SubprocessHandler.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"
#include "tapeserver/session/SessionState.hpp"
#include "tapeserver/session/SessionType.hpp"

namespace cta {

class IScheduler;

namespace catalogue {
class Catalogue;
}

namespace mediachanger {
class MediaChangerFacade;
}

namespace tape::daemon {

class DriveHandlerProxy;

/**
 * Handler for tape drive session subprocesses. On process/session will handle
 * at most one tape mount. Child process will either return a clean/unclean
 */
class DriveHandler : public SubprocessHandler {
public:
  // Possible outcomes of the previous session/child process
  enum class PreviousSession {
    Initiating, ///< The process is the first to run after daemon startup. A cleanup will be run beforehand.
    Up,         ///< The previous process unmounted the tape properly and hardware is ready for another session.
    Down,       ///< The previous session tried and failed to unmount the tape, and reported such instance.
    Crashed     ///< The previous process was killed or crashed. The next session will be a cleanup.
  };

  DriveHandler(const common::TapedConfiguration& tapedConfig, const DriveConfigEntry& driveConfig, ProcessManager& pm);
  ~DriveHandler() override = default;

  ProcessingStatus getInitialStatus() override;
  ProcessingStatus fork() override;
  void postForkCleanup() override;
  int runChild() override;
  ProcessingStatus shutdown() override;
  void kill() override;
  ProcessingStatus processEvent() override;
  ProcessingStatus processSigChild() override;
  ProcessingStatus processRefreshLoggerRequest() override;
  ProcessingStatus refreshLogger() override;
  ProcessingStatus processTimeout() override;

private:
  // Reference to the process manager
  cta::tape::daemon::ProcessManager& m_processManager;
  // The parameters
  const common::TapedConfiguration& m_tapedConfig;
  // This drive's parameters
  const DriveConfigEntry& m_driveConfig;
  // The log context
  cta::log::LogContext& m_lc;
  // Log parameter reported using the watchdog
  std::set<std::string> m_watchdogLogParams;
  // Representation of the outcome of the previous session/child process
  PreviousSession m_previousSession = PreviousSession::Initiating;
  // Representation of the last know state of the previous session (useful for crashes)
  session::SessionState m_previousState = session::SessionState::StartingUp;
  // Representation of the last know type of the previous session (useful for crashes)
  session::SessionType m_previousType = session::SessionType::Undetermined;
  // Previous VID, that can help the unmount process
  std::string m_previousVid;
  // Representation of the status of the current process
  session::SessionState m_sessionState = session::SessionState::PendingFork;
  // Current session's type
  session::SessionType m_sessionType = session::SessionType::Undetermined;
  // Current session's VID
  std::string m_sessionVid;

  // Current session's parameters: they are accumulated during session's lifetime and logged as session ends

  // The current state we report to process manager
  ProcessingStatus m_processingStatus;
  // Convenience type
  typedef std::chrono::milliseconds Timeout;
  // Values for the state change timeouts where applicable
  static std::map<session::SessionState, Timeout> m_stateChangeTimeouts;
  // Values for the heartbeat timeouts, where applicable
  static const std::map<session::SessionState, Timeout> m_heartbeatTimeouts;
  // Values for the data movement timeouts, where applicable
  static const std::map<session::SessionState, Timeout> m_dataMovementTimeouts;
  // When did we see the last state change?
  std::chrono::time_point<std::chrono::steady_clock> m_lastStateChangeTime = std::chrono::steady_clock::now();
  // When did we see the last heartbeat change?
  std::chrono::time_point<std::chrono::steady_clock> m_lastHeartBeatTime = std::chrono::steady_clock::now();
  // When did we see the last data movement?
  std::chrono::time_point<std::chrono::steady_clock> m_lastDataMovementTime = std::chrono::steady_clock::now();
  // Type of the currently active timeout
  std::string m_timeoutType;
  // Session type when currently active timeout was set
  session::SessionType m_sessionTypeWhenTimeoutDecided;
  // State when current timeout was set
  session::SessionState m_sessionStateWhenTimeoutDecided;

  // How much data did we see moving the session so far? (tape)
  uint64_t m_totalTapeBytesMoved = 0;
  // How much data did we see moving the session so far? (disk)
  uint64_t m_totalDiskBytesMoved = 0;
  // PID for the subprocess
  pid_t m_pid = -1;
  // Socket pair allowing communication with the subprocess
  std::unique_ptr<cta::server::SocketPair> m_socketPair;

  std::shared_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<SchedulerDBInit_t> m_sched_db_init;
  std::unique_ptr<SchedulerDB_t> m_sched_db;

  // Computation of the next timeout (depending on the state)
  decltype(ProcessingStatus::nextTimeout) nextTimeout();

  // Reset log parameters both on child and parent process
  void resetLogParams(cta::tape::daemon::TapedProxy* driveHandlerProxy);

  // Helper function accumulating logs
  void processLogs(serializers::WatchdogMessage& message);

  // Helper function accumulating bytes transferred
  void processBytes(serializers::WatchdogMessage& message);

  bool schedulerPing(IScheduler* scheduler, cta::tape::daemon::TapedProxy* driveHandlerProxy);
  void puttingDriveDown(IScheduler* scheduler, cta::tape::daemon::TapedProxy* driveHandlerProxy,
    std::string_view errorMsg, const cta::common::dataStructures::DriveInfo& driveInfo);
  void setDriveDownForShutdown(const std::string& reason);

  /**
   * Utility function resetting all parameters to pre-fork state
   * @param previousSessionState outcome to be considered for the next run.
   */
  void resetToDefault(PreviousSession previousSessionState);

  virtual std::shared_ptr<cta::catalogue::Catalogue> createCatalogue(const std::string& processName) const;
  virtual std::shared_ptr<cta::IScheduler> createScheduler(const std::string& processName,
    const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount);
  virtual std::shared_ptr<cta::tape::daemon::TapedProxy> createDriveHandlerProxy() const;
  virtual castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeCleanerSession(
    cta::IScheduler* scheduler) const;
  virtual castor::tape::tapeserver::daemon::Session::EndOfSessionAction executeDataTransferSession(
    cta::IScheduler* scheduler, cta::tape::daemon::TapedProxy* driveHandlerProxy) const;

protected:
  // Methods inherited by DriveHandlerMock to manipulate class internal state for unit tests
  void setPreviousSession(PreviousSession previousSessionState, session::SessionState previousState,
    session::SessionType previousType, std::string_view vid) {
    m_previousSession = previousSessionState;
    m_previousState = previousState;
    m_previousType = previousType;
    m_previousVid = vid;
  }
  void setSessionVid(std::string_view vid) { m_sessionVid = vid; }
  void setSessionState(session::SessionState state) { m_sessionState = state; }
};

} // namespace tape::daemon
} // namespace cta
