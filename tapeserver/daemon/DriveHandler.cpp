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

#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#include <set>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "rdbms/Login.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/CleanerSession.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Session.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/DriveHandlerProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"
#ifdef CTA_PGSCHED
#include "scheduler/PostgresSchedDB/PostgresSchedDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#endif

namespace cta {
namespace tape {
namespace daemon {

CTA_GENERATE_EXCEPTION_CLASS(DriveAlreadyExistException);

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DriveHandler::DriveHandler(const TapedConfiguration& tapedConfig, const TpconfigLine& configline, ProcessManager& pm) :
  SubprocessHandler(std::string("drive:") + configline.unitName), m_processManager(pm),
  m_tapedConfig(tapedConfig), m_configLine(configline),
  m_sessionEndContext(m_processManager.logContext().logger()) {
  // As the handler is started, its first duty is to create a new subprocess. This
  // will be managed by the process manager (initial request in getInitialStatus)
}

using session::SessionState;
using session::SessionType;

//------------------------------------------------------------------------------
// DriveHandler::m_stateChangeTimeouts
//------------------------------------------------------------------------------
// The following structure represents the timeouts expected for each session state transitions
// (if needed).
// The session type is not taken into account as a given state gets the same timeout regardless
// of the type session it is used in.
std::map<SessionState, DriveHandler::Timeout> DriveHandler::m_stateChangeTimeouts = {
  // Determining the drive is ready takes 1 minute, so waiting 2 should be enough.
  {SessionState::Checking,       std::chrono::duration_cast<Timeout>(std::chrono::minutes(2))},
  // Scheduling is expected to take little time, so 5 minutes is plenty. When the scheduling
  // determines there is nothing to do, it will transition to the same state (resetting the timeout).
  {SessionState::Scheduling,     std::chrono::duration_cast<Timeout>(std::chrono::minutes(5))},
  // We expect mounting (mount+load, in fact) the take no more than 10 minutes.
  {SessionState::Mounting,       std::chrono::duration_cast<Timeout>(std::chrono::minutes(10))},
  // Like mounting, unmounting is expected to take less than 10 minutes.
  {SessionState::Unmounting,     std::chrono::duration_cast<Timeout>(std::chrono::minutes(10))},
  // Draining to disk is given a grace period of 30 minutes for changing state.
  {SessionState::DrainingToDisk, std::chrono::duration_cast<Timeout>(std::chrono::minutes(30))},
  // We expect the process to exit within 5 minutes of getting in this state. This state
  // potentially covers the draining of metadata to central storage (if not faster that
  // unmounting the tape).
  // TODO: this is set temporarily to 15 minutes while the reporting is not yet parallelized.
  {SessionState::ShuttingDown,   std::chrono::duration_cast<Timeout>(std::chrono::minutes(15))}
};

//------------------------------------------------------------------------------
// DriveHandler::m_heartbeatTimeouts
//------------------------------------------------------------------------------
// The following structure represents heartbeat timeouts expected for each session state with data movement.
// TODO: decide on values.
const std::map<SessionState, DriveHandler::Timeout> DriveHandler::m_heartbeatTimeouts = {
  {SessionState::Running,        std::chrono::duration_cast<Timeout>(std::chrono::minutes(1))},
  {SessionState::DrainingToDisk, std::chrono::duration_cast<Timeout>(std::chrono::minutes(1))}
};

//------------------------------------------------------------------------------
// DriveHandler::m_dataMovementTimeouts
//------------------------------------------------------------------------------
// The following structure represents the data movement timeouts for the session states involving
// data movements and heartbeats.
// TODO: decide on values. TODO: bumped up to 15 minutes for the time being as the
// efficient retrieve requests pop is not implemented yet.
const std::map<SessionState, DriveHandler::Timeout> DriveHandler::m_dataMovementTimeouts = {
  {SessionState::Running,        std::chrono::duration_cast<Timeout>(std::chrono::minutes(15))},
  {SessionState::DrainingToDisk, std::chrono::duration_cast<Timeout>(std::chrono::minutes(15))}
};

//------------------------------------------------------------------------------
// DriveHandler::getInitialStatus
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::getInitialStatus() {
  // As we just start, we need to fork the first subprocess
  m_processingStatus.forkRequested = true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::prepareForFork
//------------------------------------------------------------------------------
void DriveHandler::postForkCleanup() {
  // We are in the child process of another handler. We can close our socket pair
  // without re-registering it from poll.
  m_socketPair.reset(nullptr);
}

//------------------------------------------------------------------------------
// DriveHandler::fork
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::fork() {
  // If anything fails while attempting to fork, we will have to declare ourselves
  // failed and ask for a shutdown by sending the TERM signal to the parent process
  // This will ensure the shutdown-kill sequence managed by the signal handler without code duplication.
  // Record we no longer ask for fork
  m_processingStatus.forkRequested = false;
  try {
    // Check we are in the right state (sanity check)
    if (m_sessionState != SessionState::PendingFork) {
      std::stringstream err;
      err << "In DriveHandler::fork(): called while not in the expected state: " << session::toString(m_sessionState)
          << " instead of " << session::toString(SessionState::PendingFork);
      throw exception::Exception(err.str());
    }
    // First prepare a socket pair for this new subprocess
    m_socketPair.reset(new cta::server::SocketPair());
    // and fork
    m_pid = ::fork();
    exception::Errnum::throwOnMinusOne(m_pid, "In DriveHandler::fork(): failed to fork()");
    m_sessionState = SessionState::StartingUp;
    m_lastStateChangeTime = std::chrono::steady_clock::now();
    if (!m_pid) {
      // We are in the child process
      SubprocessHandler::ProcessingStatus ret;
      ret.forkState = SubprocessHandler::ForkState::child;
      return ret;
    }
    else {
      // We are in the parent process
      m_processingStatus.forkState = SubprocessHandler::ForkState::parent;
      // Compute the next timeout
      m_processingStatus.nextTimeout = nextTimeout();
      // Register our socket pair side for epoll after closing child side.
      m_socketPair->close(server::SocketPair::Side::child);
      m_processManager.addFile(m_socketPair->getFdForAccess(server::SocketPair::Side::child), this);
      // We are now ready to react to timeouts and messages from the child process.
      return m_processingStatus;
    }
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("tapeDrive", m_configLine.unitName)
          .add("Error", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR, "Failed to fork drive process. Initiating shutdown with SIGTERM.");
    // Wipe all previous states as we are shutting down
    m_processingStatus = SubprocessHandler::ProcessingStatus();
    m_sessionState = SessionState::Shutdown;
    m_processingStatus.shutdownComplete = true;
    m_processingStatus.forkState = SubprocessHandler::ForkState::parent;
    // Initiate shutdown
    ::kill(::getpid(), SIGTERM);
    return m_processingStatus;
  }
}

//------------------------------------------------------------------------------
// DriveHandler::nextTimeout
//------------------------------------------------------------------------------
decltype(SubprocessHandler::ProcessingStatus::nextTimeout) DriveHandler::nextTimeout() {
  // If a timeout is defined, then we compute its expiration time. Else we just give default timeout (end of times).
  decltype(SubprocessHandler::ProcessingStatus::nextTimeout) ret = decltype(SubprocessHandler::ProcessingStatus::nextTimeout)::max();
  bool retSet = false;
  try {
    ret = m_lastStateChangeTime + m_stateChangeTimeouts.at(m_sessionState);
    retSet = true;
    m_timeoutType = "StateChange";
  } catch (...) {}
  try {
    auto newRet = m_lastHeartBeatTime + m_heartbeatTimeouts.at(m_sessionState);
    if (newRet < ret) {
      ret = newRet;
      retSet = true;
      m_timeoutType = "Heartbeat";
    }
  } catch (...) {}
  try {
    auto newRet = m_lastDataMovementTime + m_dataMovementTimeouts.at(m_sessionState);
    if (newRet < ret) {
      ret = newRet;
      retSet = true;
      m_timeoutType = "DataMovement";
    }
  } catch (...) {}
  if (retSet) {
    m_sessionStateWhenTimeoutDecided = m_sessionState;
    m_sessionTypeWhenTimeoutDecided = m_sessionType;
  }
  {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("TimeoutType", m_timeoutType)
          .add("LastStateChangeTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastStateChangeTime.time_since_epoch()).count())
          .add("LastHeartBeatTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastHeartBeatTime.time_since_epoch()).count())
          .add("LastDataMovementTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastDataMovementTime.time_since_epoch()).count())
          .add("Now", std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
          .add("Timeout", std::chrono::duration_cast<std::chrono::seconds>(ret.time_since_epoch()).count());
    m_processManager.logContext().log(log::DEBUG, "Computed new timeout");
  }
  return ret;
}

//------------------------------------------------------------------------------
// DriveHandler::kill
//------------------------------------------------------------------------------
void DriveHandler::kill() {
  // If we have a subprocess, kill it and wait for completion (if needed). We do not need to keep
  // track of the exit state as kill() means we will not be called anymore.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  if (m_pid != -1) {
    params.add("SubProcessId", m_pid);
    // The socket pair will be reopened on the next fork. Clean it up.
    if (m_socketPair) {
      m_processManager.removeFile(m_socketPair->getFdForAccess(server::SocketPair::Side::child));
      m_socketPair.reset(nullptr);
    }
    try {
      exception::Errnum::throwOnMinusOne(::kill(m_pid, SIGKILL), "Failed to kill() subprocess");
      int status;
      // wait for child process exit
      exception::Errnum::throwOnMinusOne(::waitpid(m_pid, &status, 0), "Failed to waitpid() subprocess");
      // Log the status
      params.add("WIFEXITED", WIFEXITED(status));
      if (WIFEXITED(status)) {
        params.add("WEXITSTATUS", WEXITSTATUS(status));
      }
      else {
        params.add("WIFSIGNALED", WIFSIGNALED(status));
      }
      m_processManager.logContext().log(log::INFO, "In DriveHandler::kill(): sub process completed");
      m_sessionEndContext.pushOrReplace({"Error_sessionKilled", "1"});
      m_sessionEndContext.pushOrReplace({"killSignal", WTERMSIG(status)});
      m_sessionEndContext.pushOrReplace({"status", "failure"});
      m_sessionEndContext.pushOrReplace({"tapeDrive", m_configLine.unitName});
      m_sessionEndContext.log(cta::log::INFO, "Tape session finished");
      m_sessionEndContext.clear();
      m_pid = -1;
    } catch (exception::Exception& ex) {
      params.add("Exception", ex.getMessageValue());
      m_processManager.logContext().log(log::ERR, "In DriveHandler::kill(): failed to kill existing subprocess");
    }
  }
  else {
    m_processManager.logContext().log(log::INFO, "In DriveHandler::kill(): no subprocess to kill");
  }
}

//------------------------------------------------------------------------------
// DriveHandler::processEvent
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processEvent() {
  log::ScopedParamContainer scoped(m_processManager.logContext());
  scoped.add("tapeDrive", m_configLine.unitName);
  m_lastHeartBeatTime = std::chrono::steady_clock::now();

  // Read from the socket pair
  try {
    serializers::WatchdogMessage message;
    auto datagram = m_socketPair->receive();
    if (!message.ParseFromString(datagram)) {
      // Use the tolerant parser to assess the situation.
      message.ParsePartialFromString(datagram);
      throw cta::exception::Exception(std::string("In SubprocessHandler::ProcessingStatus(): could not parse message: ") +
                                      message.InitializationErrorString());
    }
    // Logs are processed in all cases
    processLogs(message);
    // If we report bytes, process the report (this is a heartbeat)
    if (message.reportingbytes()) {
      processBytes(message);
    }
    // If we report a state change, process it (last as this can change the return value)
    if (message.reportingstate()) {
      // Log a session state change
      if (m_sessionState != static_cast<SessionState>(message.sessionstate())) {
        scoped.add("PreviousState", session::toString(m_sessionState))
              .add("PreviousType", session::toString(m_sessionType))
              .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
              .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
        m_processManager.logContext().log(log::INFO, "In DriveHandler::processEvent(): changing session state");
      }

      switch (static_cast<SessionState>(message.sessionstate())) {
        case SessionState::Checking:
          m_processingStatus = processChecking(message);
          break;
        case SessionState::Scheduling:
          m_processingStatus = processScheduling(message);
          break;
        case SessionState::Mounting:
          m_processingStatus = processMounting(message);
          break;
        case SessionState::Running:
          m_processingStatus = processRunning(message);
          break;
        case SessionState::Unmounting:
          m_processingStatus = processUnmounting(message);
          break;
        case SessionState::DrainingToDisk:
          m_processingStatus = processDrainingToDisk(message);
          break;
        case SessionState::ShuttingDown:
          m_processingStatus = processShuttingDown(message);
          break;
        case SessionState::Fatal:
          m_processingStatus = processFatal(message);
          break;
        default: {
          exception::Exception ex;
          ex.getMessage() << "In DriveHandler::processEvent(): unexpected session state:"
                          << session::toString(static_cast<SessionState>(message.sessionstate()));
          throw ex;
        }
      }

      // Update state
      m_sessionState = static_cast<SessionState>(message.sessionstate());
      m_sessionType = static_cast<SessionType>(message.sessiontype());

      m_lastStateChangeTime = std::chrono::steady_clock::now();
    }

    // Compute timeout before returning the processing status
    m_processingStatus.nextTimeout = nextTimeout();

    return m_processingStatus;
  } catch (cta::server::SocketPair::PeerDisconnected& ex) {
    // The peer disconnected: close the socket pair and remove it from the epoll list.
    if (m_socketPair) {
      m_processManager.removeFile(m_socketPair->getFdForAccess(server::SocketPair::Side::child));
      m_socketPair.reset(nullptr);
    }
    else {
      m_processManager.logContext().log(log::ERR,
                                        "In DriveHandler::processEvent(): internal error. Got a peer disconnect with no socketPair object");
    }
    // We expect to be woken up by the child's signal.
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::DEBUG,
                                      "In DriveHandler::processEvent(): Got a peer disconnect: closing socket and waiting for SIGCHILD");
    return m_processingStatus;
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR,
                                      "In DriveHandler::processEvent(): failed");
    return m_processingStatus;
  }
}

//------------------------------------------------------------------------------
// DriveHandler::resetToDefault
//------------------------------------------------------------------------------
void DriveHandler::resetToDefault(PreviousSession previousSessionState) {
  m_pid = -1;
  m_previousSession = previousSessionState;
  m_previousType = m_sessionType;
  m_previousState = m_sessionState;
  m_previousVid = m_sessionVid;
  m_sessionState = SessionState::PendingFork;
  m_sessionType = SessionType::Undetermined;
  m_lastStateChangeTime = std::chrono::steady_clock::now();
  m_lastHeartBeatTime = std::chrono::steady_clock::now();
  m_lastDataMovementTime = std::chrono::steady_clock::now();
  m_processingStatus.forkRequested = false;
  m_processingStatus.killRequested = false;
  m_processingStatus.nextTimeout = m_processingStatus.nextTimeout.max();
  m_processingStatus.shutdownComplete = false;
  m_processingStatus.shutdownRequested = false;
  m_processingStatus.sigChild = false;
}

//------------------------------------------------------------------------------
// DriveHandler::processScheduling
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processScheduling(serializers::WatchdogMessage& message) {
  // We are either going to schedule
  // Check the transition is expected. This is non-fatal as the drive session has the last word anyway.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  std::set<SessionState> expectedStates = {SessionState::StartingUp, SessionState::Scheduling};
  if (!expectedStates.count(m_sessionState) ||
      m_sessionType != SessionType::Undetermined ||
      static_cast<SessionType>(message.sessiontype()) != SessionType::Undetermined) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processScheduling(): unexpected previous state/type.");
  }

  m_sessionVid = "";
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processChecking
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processChecking(serializers::WatchdogMessage& message) {
  // We expect to come from startup/undefined and to get into checking/cleanup
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  if (m_sessionState != SessionState::StartingUp || m_sessionType != SessionType::Undetermined ||
      static_cast<SessionType>(message.sessiontype()) != SessionType::Cleanup) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processChecking(): unexpected previous state/type.");
  }

  m_sessionVid = "";
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processMounting
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processMounting(serializers::WatchdogMessage& message) {
  // The only transition expected is from scheduling. Several sessions types are possible
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  std::set<SessionType> expectedNewTypes = {SessionType::Archive, SessionType::Retrieve, SessionType::Label};
  if (m_sessionState != SessionState::Scheduling ||
      m_sessionType != SessionType::Undetermined ||
      !expectedNewTypes.count(static_cast<SessionType>(message.sessiontype()))) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processMounting(): unexpected previous state/type.");
  }

  m_sessionVid = message.vid();
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processRunning
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processRunning(serializers::WatchdogMessage& message) {
  // This status can be reported repeatedly (or we can transition from the previous one: Mounting).
  // We expect the type not to change (and to be in the right range)
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  std::set<SessionState> expectedStates = {SessionState::Mounting, SessionState::Running};
  std::set<SessionType> expectedTypes = {SessionType::Archive, SessionType::Retrieve, SessionType::Label};
  if (!expectedStates.count(m_sessionState) ||
      !expectedTypes.count(m_sessionType) ||
      (m_sessionType != static_cast<SessionType>(message.sessiontype()))) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processMounting(): unexpected previous state/type.");
  }

  // On state change reset the data movement counter
  if (m_sessionState != static_cast<SessionState>(message.sessionstate())) {
    m_lastDataMovementTime=std::chrono::steady_clock::now();
  }

  m_sessionVid = message.vid();
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processUnmounting
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processUnmounting(serializers::WatchdogMessage& message) {
  // This status can come from either running (any running compatible session type)
  // of checking in the case of the cleanup session.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  std::set<std::tuple<SessionState, SessionType>> expectedStateTypes =
    {
      std::make_tuple(SessionState::Running, SessionType::Archive),
      std::make_tuple(SessionState::Running, SessionType::Retrieve),
      std::make_tuple(SessionState::Running, SessionType::Label),
      std::make_tuple(SessionState::Checking, SessionType::Cleanup)
    };
  // (all types of sessions can unmount).
  if (!expectedStateTypes.count(std::make_tuple(m_sessionState, m_sessionType))) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processUnmounting(): unexpected previous state/type.");
  }

  m_sessionVid = message.vid();
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processDrainingToDisk
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processDrainingToDisk(serializers::WatchdogMessage& message) {
  // This status transition is expected from unmounting, and only for retrieve sessions.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  if (SessionState::Unmounting != m_sessionState ||
      SessionType::Retrieve != m_sessionType) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processDrainingToDisk(): unexpected previous state/type.");
  }

  m_sessionVid = "";
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processShuttingDown
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processShuttingDown(serializers::WatchdogMessage& message) {
  // This status transition is expected from unmounting, and only for retrieve sessions.
  // As usual, subprocess has the last word.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  std::set<SessionState> expectedStates = {SessionState::Unmounting, SessionState::DrainingToDisk};
  if (!expectedStates.count(m_sessionState)) {
    params.add("PreviousState", session::toString(m_sessionState))
          .add("PreviousType", session::toString(m_sessionType))
          .add("NewState", session::toString(static_cast<SessionState>(message.sessionstate())))
          .add("NewType", session::toString(static_cast<SessionType>(message.sessiontype())));
    m_processManager.logContext().log(log::DEBUG,
                                      "WARNING: In DriveHandler::processShuttingDown(): unexpected previous state/type.");
  }

  m_sessionVid = "";
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processFatal
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processFatal(serializers::WatchdogMessage& message) {
  // This status indicates that the session cannot be run and the server should
  // shut down (central storage unavailable).
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  m_sessionVid = "";
  m_processingStatus.shutdownRequested = true;
  m_processManager.logContext().log(log::CRIT,
                                    "In DriveHandler::processFatal(): shutting down after fatal failure.");
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processLogs
//------------------------------------------------------------------------------
void DriveHandler::processLogs(serializers::WatchdogMessage& message) {
  // Accumulate the logs added (if any)
  for (auto& log: message.addedlogparams()) {
    m_sessionEndContext.pushOrReplace({log.name(), log.value()});
  }
  for (auto& log: message.deletedlogparams()) {
    m_sessionEndContext.erase(log);
  }
}

//------------------------------------------------------------------------------
// DriveHandler::processBytes
//------------------------------------------------------------------------------
void DriveHandler::processBytes(serializers::WatchdogMessage& message) {
  // In all cases, this is a heartbeat.
  m_lastHeartBeatTime = std::chrono::steady_clock::now();

  // Record data moved totals if needed.
  if (m_totalTapeBytesMoved != message.totaltapebytesmoved() ||
      m_totalDiskBytesMoved != message.totaldiskbytesmoved()) {
    if (message.totaltapebytesmoved() < m_totalTapeBytesMoved ||
        message.totaldiskbytesmoved() < m_totalDiskBytesMoved) {
      log::ScopedParamContainer params(m_processManager.logContext());
      params.add("PreviousTapeBytesMoved", m_totalTapeBytesMoved)
            .add("PreviousDiskBytesMoved", m_totalDiskBytesMoved)
            .add("NewTapeBytesMoved", message.totaltapebytesmoved())
            .add("NewDiskBytesMoved", message.totaldiskbytesmoved());
      m_processManager.logContext().log(log::DEBUG, "WARNING: In DriveHandler::processRunning(): total bytes moved going backwards");
    }
    m_totalTapeBytesMoved = message.totaltapebytesmoved();
    m_totalDiskBytesMoved = message.totaldiskbytesmoved();
    m_lastDataMovementTime = std::chrono::steady_clock::now();
  }

  // Update next timeout if required. Next operations might not do it.
  m_processingStatus.nextTimeout = nextTimeout();
}

//------------------------------------------------------------------------------
// DriveHandler::processSigChild
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processSigChild() {
  // Check out child process's status. If the child process is still around,
  // waitpid will return 0. Non zero if process completed (and status needs to
  // be picked up) and -1 if the process is entirely gone.
  // Of course we might not have a child process to begin with.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  if (-1 == m_pid) return m_processingStatus;
  int processStatus;
  int rc = ::waitpid(m_pid, &processStatus, WNOHANG);
  // Check there was no error.
  try {
    exception::Errnum::throwOnMinusOne(rc);
  } catch (exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_processManager.logContext());
    scoped.add("pid", m_pid)
          .add("tapeDrive", m_configLine.unitName)
          .add("Message", ex.getMessageValue())
          .add("SessionState", session::toString(m_sessionState))
          .add("SessionType", toString(m_sessionType));
    m_processManager.logContext().log(log::WARNING,
                                      "In DriveHandler::processSigChild(): failed to get child process exit code. Doing nothing as we are unable to determine if it is still running or not.");
    return m_processingStatus;
  }
  if (rc) {
    // It was our process. In all cases we prepare the space for the new session
    // We did collect the exit code of our child process
    // How well did it finish? (exit() or killed?)
    // The socket pair will be reopened on the next fork. Clean it up.
    if (m_socketPair) {
      m_processManager.removeFile(m_socketPair->getFdForAccess(server::SocketPair::Side::child));
      m_socketPair.reset(nullptr);
    }
    params.add("pid", m_pid);
    if (WIFEXITED(processStatus)) {
      // Child process exited properly. The new child process will not need to start
      // a cleanup session.
      params.add("exitCode", WEXITSTATUS(processStatus));
      // If we are shutting down, we should not request a new session.
      if (m_sessionState != SessionState::Shutdown) {
        m_processManager.logContext().log(log::INFO, "Drive subprocess exited. Will spawn a new one.");
        resetToDefault(PreviousSession::Up);
        m_processingStatus.forkRequested = true;
      }
      else {
        m_processManager.logContext().log(log::INFO, "Drive subprocess exited. Will not spawn new one as we are shutting down.");
        m_processingStatus.forkRequested = false;
      }
    }
    else {
      params.add("IfSignaled", WIFSIGNALED(processStatus))
            .add("TermSignal", WTERMSIG(processStatus))
            .add("CoreDump", WCOREDUMP(processStatus));
      // Record the status of the session to decide whether we will run a cleaner on the next one.
      resetToDefault(PreviousSession::Crashed);
      // If we are shutting down, we should not request a new session.
      if (m_sessionState != SessionState::Shutdown) {
        m_processManager.logContext().log(log::INFO, "Drive subprocess crashed. Will spawn a new one.");
        m_processingStatus.forkRequested = true;
      }
      else {
        m_processManager.logContext().log(log::INFO, "Drive subprocess crashed. Will not spawn new one as we are shutting down.");
        m_processingStatus.forkRequested = false;
      }
      m_sessionEndContext.pushOrReplace({"Error_sessionKilled", "1"});
      m_sessionEndContext.pushOrReplace({"killSignal", WTERMSIG(processStatus)});
      m_sessionEndContext.pushOrReplace({"status", "failure"});
    }
    // In all cases we log the end of the session.
    m_sessionEndContext.pushOrReplace({"tapeDrive", m_configLine.unitName});
    m_sessionEndContext.moveToTheEndIfPresent("status");
    m_sessionEndContext.log(cta::log::INFO, "Tape session finished");
    m_sessionEndContext.clear();
    // And record we do not have a process anymore.
    m_pid = -1;
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::processTimeout
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::processTimeout() {
  // Process manager found that we timed out. Let's log why and kill the child process,
  // if any (there should be one).
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("tapeDrive", m_configLine.unitName);
  if (-1 == m_pid) {
    m_processManager.logContext().log(log::ERR, "In DriveHandler::processTimeout(): Received timeout without child process present.");
    m_processManager.logContext().log(log::INFO, "Re-launching child process.");
    m_processingStatus.forkRequested = true;
    m_processingStatus.nextTimeout = m_processingStatus.nextTimeout.max();
    // Record the status of the session for cleanup startup (not needed here)
    m_sessionState = SessionState::Shutdown;
    m_sessionType = SessionType::Undetermined;
    m_sessionVid = "";
    resetToDefault(PreviousSession::Crashed);
    return m_processingStatus;
  }
  auto now = std::chrono::steady_clock::now();
  params.add("SessionState", session::toString(m_sessionState))
        .add("SessionType", session::toString(m_sessionType))
        .add("TimeoutType", m_timeoutType)
        .add("SessionTypeWhenTimeoutDecided", session::toString(m_sessionTypeWhenTimeoutDecided))
        .add("SessionStateWhenTimeoutDecided", session::toString(m_sessionStateWhenTimeoutDecided))
        .add("LastDataMovementTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastDataMovementTime.time_since_epoch()).count())
        .add("LastHeartbeatTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastHeartBeatTime.time_since_epoch()).count())
        .add("LastStateChangeTime", std::chrono::duration_cast<std::chrono::seconds>(m_lastStateChangeTime.time_since_epoch()).count())
        .add("Now", std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count())
        .add("ThisTimeout", std::chrono::duration_cast<std::chrono::seconds>(m_processingStatus.nextTimeout.time_since_epoch()).count());
  // Log timeouts (if we have any)
  try {
    decltype(SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout =
      m_lastStateChangeTime + m_stateChangeTimeouts.at(m_sessionState);
    std::chrono::duration<double> timeToTimeout = nextTimeout - now;
    params.add("BeforeStateChangeTimeout_s", timeToTimeout.count());
  } catch (...) {}
  try {
    decltype(SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout =
      m_lastDataMovementTime + m_dataMovementTimeouts.at(m_sessionState);
    std::chrono::duration<double> timeToTimeout = nextTimeout - now;
    params.add("BeforeDataMovementTimeout_s", timeToTimeout.count());
  } catch (...) {}
  try {
    decltype(SubprocessHandler::ProcessingStatus::nextTimeout) nextTimeout =
      m_lastHeartBeatTime + m_heartbeatTimeouts.at(m_sessionState);
    std::chrono::duration<double> timeToTimeout = nextTimeout - now;
    params.add("BeforeHeartbeatTimeout_s", timeToTimeout.count());
  } catch (...) {}
  try {
    params.add("SubprocessId", m_pid);
    exception::Errnum::throwOnMinusOne(::kill(m_pid, SIGKILL));
    m_processManager.logContext().log(log::WARNING, "In DriveHandler::processTimeout(): Killed subprocess.");
  } catch (exception::Exception& ex) {
    params.add("Error", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR, "In DriveHandler::processTimeout(): Failed to kill subprocess.");
  }
  // We now should receive the sigchild, so we ask nothing from process manager
  m_processingStatus.nextTimeout = m_processingStatus.nextTimeout.max();
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// DriveHandler::runChild
//------------------------------------------------------------------------------
int DriveHandler::runChild() {
  // We are in the child process. It is time to open connections to the catalogue
  // and object store, and run the session.
  // If the previous session crashed, and a tape is potentially in the drive (after
  // report of starting to mount and before a report of successful unmount),
  // this session instance will be a cleaner session.
  // This condition is detected with a non empty m_previousVID and the corresponding
  // m_previousState and m_PreviousType.
  // Finally, on a crashed cleaner session, we will put the drive down.
  // A non crashed session which failed to unmount the tape will have put itself
  // down (and already failed to unmount on its own).
  // Otherwise, this session will run a regular data transfer session which will
  // schedule itself info an empty drive probe, archive, retrieve or label session.

  // Set the thread name for process ID:
  std::string threadName = "cta-tpd-";
  threadName += m_configLine.unitName;
  prctl(PR_SET_NAME, threadName.c_str());

  // Create the channel to talk back to the parent process.
  cta::tape::daemon::DriveHandlerProxy driveHandlerProxy(*m_socketPair);

  std::string hostname = cta::utils::getShortHostname();

  auto& lc = m_processManager.logContext();
  {
    log::ScopedParamContainer params(lc);
    params.add("backendPath", m_tapedConfig.backendPath.value());
    lc.log(log::DEBUG, "In DriveHandler::runChild(): will connect to object store backend.");
  }
  // Before anything, we need to check we have access to the scheduler's central storage
  std::unique_ptr<SchedulerDBInit_t> sched_db_init;
  try {
    std::string processName = "DriveProcess-";
    processName += m_configLine.unitName;
    log::ScopedParamContainer params(lc);
    params.add("processName", processName);
    lc.log(log::DEBUG, "In DriveHandler::runChild(): will create agent entry. Enabling leaving non-empty agent behind.");
    sched_db_init.reset(new SchedulerDBInit_t(processName, m_tapedConfig.backendPath.value(), m_processManager.logContext().logger(), true));
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandler::runChild(): failed to connect to objectstore or failed to instantiate agent entry. Reporting fatal error.");
    driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    sleep(1);
    return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
  }
  std::unique_ptr<SchedulerDB_t> sched_db;
  try {
    if (!m_catalogue) {
      m_catalogue = createCatalogue("DriveHandler::runChild()");
    }
    sched_db = sched_db_init->getSchedDB(*m_catalogue, lc.logger());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandler::runChild(): failed to instantiate catalogue. Reporting fatal error.");
    driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    sleep(1);
    return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
  }
  lc.log(log::DEBUG, "In DriveHandler::runChild(): will create scheduler.");
  cta::Scheduler scheduler(*m_catalogue, *sched_db, m_tapedConfig.mountCriteria.value().maxFiles,
                           m_tapedConfig.mountCriteria.value().maxBytes);
  // Before launching the transfer session, we validate that the scheduler is reachable.
  lc.log(log::DEBUG, "In DriveHandler::runChild(): will ping scheduler.");
  try {
    scheduler.ping(lc);
  } catch (const cta::catalogue::WrongSchemaVersionException& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandler::runChild(): catalogue MAJOR version mismatch. Reporting fatal error.");
    driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandler::runChild(): failed to ping central storage before session. Reporting fatal error.");
    driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
  }

  // 1) Special case first, if we crashed in a cleaner session, we put the drive down
  if (m_previousSession == PreviousSession::Crashed && m_previousType == SessionType::Cleanup) {
    log::ScopedParamContainer params(lc);
    params.add("tapeDrive", m_configLine.unitName);
    int logLevel = log::ERR;
    std::string errorMsg = "In DriveHandler::runChild(): the cleaner session crashed. Putting the drive down.";
    lc.log(log::ERR, errorMsg);
    // Get hold of the scheduler.
    try {
      cta::common::dataStructures::DriveInfo driveInfo;
      driveInfo.driveName = m_configLine.unitName;
      driveInfo.logicalLibrary = m_configLine.logicalLibrary;
      driveInfo.host = hostname;
      scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
      cta::common::dataStructures::SecurityIdentity securityIdentity;
      cta::common::dataStructures::DesiredDriveState driveState;
      driveState.up = false;
      driveState.forceDown = false;
      driveState.setReasonFromLogMsg(logLevel, errorMsg);
      scheduler.setDesiredDriveState(securityIdentity, m_configLine.unitName, driveState, lc);
      return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
    } catch (cta::exception::Exception& ex) {
      log::ScopedParamContainer param(lc);
      param.add("errorMessage", ex.getMessageValue());
      lc.log(log::CRIT, "In DriveHandler::runChild(): failed to set the drive down. Reporting fatal error.");
      driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
      sleep(1);
      return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
    }
  }

  // 2) If the previous session crashed, we might want to run a cleaner session, depending
  // on the previous state
  std::set<SessionState> statesRequiringCleaner = {SessionState::Mounting,
                                                   SessionState::Running, SessionState::Unmounting};
  if (m_previousSession == PreviousSession::Crashed && statesRequiringCleaner.count(m_previousState)) {
    if (m_previousVid.empty()) {
      int logLevel = log::ERR;
      std::string errorMsg = "In DriveHandler::runChild(): Should run cleaner but VID is missing. Putting the drive down.";
      lc.log(log::ERR, errorMsg);
      try {
        cta::common::dataStructures::DriveInfo driveInfo;
        driveInfo.driveName = m_configLine.unitName;
        driveInfo.logicalLibrary = m_configLine.logicalLibrary;
        driveInfo.host = hostname;
        scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
        cta::common::dataStructures::SecurityIdentity securityIdentity;
        cta::common::dataStructures::DesiredDriveState driveState;
        driveState.up = false;
        driveState.forceDown = false;
        driveState.setReasonFromLogMsg(logLevel, errorMsg);
        scheduler.setDesiredDriveState(securityIdentity, m_configLine.unitName, driveState, lc);
        return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
      } catch (cta::exception::Exception& ex) {
        log::ScopedParamContainer param(lc);
        param.add("errorMessage", ex.getMessageValue());
        lc.log(log::CRIT, "In DriveHandler::runChild(): failed to set the drive down. Reporting fatal error.");
        driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
        sleep(1);
        return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
      }
    }
    // Log the decision
    {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", m_previousVid)
            .add("tapeDrive", m_configLine.unitName)
            .add("PreviousState", session::toString(m_sessionState))
            .add("PreviousType", session::toString(m_sessionType));
      lc.log(log::INFO, "In DriveHandler::runChild(): starting cleaner after crash with tape potentially loaded.");
    }
    // Capabilities management.
    cta::server::ProcessCap capUtils;

    // Mounting management.
    cta::mediachanger::MediaChangerFacade mediaChangerFacade(lc.logger());

    castor::tape::System::realWrapper sWrapper;

    // TODO: the cleaner session does not yet report to the scheduler.
//    // Before launching the transfer session, we validate that the scheduler is reachable.
//    if (!scheduler.ping()) {
//      lc.log(log::CRIT, "In DriveHandler::runChild(): failed to ping central storage before cleaner. Reporting fatal error.");
//      driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
//      sleep(1);
//      return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
//    }
    try {
      scheduler.ping(lc);
    } catch (const cta::catalogue::WrongSchemaVersionException& ex) {
      log::ScopedParamContainer param(lc);
      param.add("errorMessage", ex.getMessageValue());
      lc.log(log::CRIT, "In DriveHandler::runChild() before cleanerSession: catalogue MAJOR version mismatch. Reporting fatal error.");
      driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
      return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
    } catch (cta::exception::Exception& ex) {
      log::ScopedParamContainer param(lc);
      param.add("errorMessage", ex.getMessageValue());
      lc.log(log::CRIT, "In DriveHandler::runChild() before cleanerSession: failed to ping central storage before session. Reporting fatal error.");
      driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
      return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
    }

    castor::tape::tapeserver::daemon::CleanerSession cleanerSession(
      capUtils,
      mediaChangerFacade,
      lc.logger(),
      m_configLine,
      sWrapper,
      m_previousVid,
      true,
      m_tapedConfig.tapeLoadTimeout.value(),
      "",
      *m_catalogue,
      scheduler);
    return cleanerSession.execute();
  }
  else {
    // The next session will be a normal session (no crash with a mounted tape before).
    // Capabilities management.
    cta::server::ProcessCap capUtils;

    // Mounting management.
    cta::mediachanger::MediaChangerFacade mediaChangerFacade(lc.logger());

    castor::tape::System::realWrapper sWrapper;

    castor::tape::tapeserver::daemon::DataTransferConfig dataTransferConfig;
    dataTransferConfig.bufsz = m_tapedConfig.bufferSizeBytes.value();
    dataTransferConfig.bulkRequestMigrationMaxBytes = m_tapedConfig.archiveFetchBytesFiles.value().maxBytes;
    dataTransferConfig.bulkRequestMigrationMaxFiles = m_tapedConfig.archiveFetchBytesFiles.value().maxFiles;
    dataTransferConfig.bulkRequestRecallMaxBytes = m_tapedConfig.retrieveFetchBytesFiles.value().maxBytes;
    dataTransferConfig.bulkRequestRecallMaxFiles = m_tapedConfig.retrieveFetchBytesFiles.value().maxFiles;
    dataTransferConfig.maxBytesBeforeFlush = m_tapedConfig.archiveFlushBytesFiles.value().maxBytes;
    dataTransferConfig.maxFilesBeforeFlush = m_tapedConfig.archiveFlushBytesFiles.value().maxFiles;
    dataTransferConfig.nbBufs = m_tapedConfig.bufferCount.value();
    dataTransferConfig.nbDiskThreads = m_tapedConfig.nbDiskThreads.value();
    dataTransferConfig.useLbp = true;
    dataTransferConfig.useRAO = m_tapedConfig.useRAO.value() == "yes" ? true : false;
    dataTransferConfig.raoLtoAlgorithm = m_tapedConfig.raoLtoAlgorithm.value();
    dataTransferConfig.raoLtoAlgorithmOptions = m_tapedConfig.raoLtoOptions.value();
    dataTransferConfig.externalFreeDiskSpaceScript = m_tapedConfig.externalFreeDiskSpaceScript.value();
    dataTransferConfig.tapeLoadTimeout = m_tapedConfig.tapeLoadTimeout.value();
    dataTransferConfig.xrootPrivateKey = "";
    dataTransferConfig.useEncryption = m_tapedConfig.useEncryption.value() == "yes" ? true : false;
    dataTransferConfig.externalEncryptionKeyScript = m_tapedConfig.externalEncryptionKeyScript.value();
    dataTransferConfig.wdIdleSessionTimer = m_tapedConfig.wdIdleSessionTimer.value();
    dataTransferConfig.wdGlobalLockAcqMaxSecs = m_tapedConfig.wdGlobalLockAcqMaxSecs.value();
    m_stateChangeTimeouts[session::SessionState::Checking] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdCheckMaxSecs.value()));
    m_stateChangeTimeouts[session::SessionState::Scheduling] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdScheduleMaxSecs.value()));
    m_stateChangeTimeouts[session::SessionState::Mounting] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdMountMaxSecs.value()));
    m_stateChangeTimeouts[session::SessionState::Unmounting] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdUnmountMaxSecs.value()));
    m_stateChangeTimeouts[session::SessionState::DrainingToDisk] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdDrainMaxSecs.value()));
    m_stateChangeTimeouts[session::SessionState::ShuttingDown] = std::chrono::duration_cast<Timeout>(
      std::chrono::minutes(m_tapedConfig.wdShutdownMaxSecs.value()));

    // Before launching, and if this is the first session since daemon start, we will
    // put the drive down.
    if (m_previousSession == PreviousSession::Initiating) {
      // Log that we put the drive's desired state to down and do it.
      log::ScopedParamContainer params(lc);
      params.add("tapeDrive", m_configLine.unitName);
      int logLevel = log::INFO;
      std::string msg = "Startup";
      lc.log(logLevel, msg);
      try {
        // Before setting the desired state as down, we have to make sure the drive exists in the registry.
        // this is done by reporting the drive as down first.
        cta::common::dataStructures::DriveInfo driveInfo;
        driveInfo.driveName = m_configLine.unitName;
        driveInfo.logicalLibrary = m_configLine.logicalLibrary;
        driveInfo.host = hostname;

        // Checking the drive does not already exist in the database
        if (!scheduler.checkDriveCanBeCreated(driveInfo, lc)) {
          driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
          return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
        }

        cta::common::dataStructures::DesiredDriveState  currentDesiredDriveState;
        try {
          currentDesiredDriveState = scheduler.getDesiredDriveState(m_configLine.unitName, lc);
        } catch (Scheduler::NoSuchDrive& ex) {
          lc.log(log::INFO, "In DriveHandler::runChild(): the desired drive state doesn't exist in the Catalogue DB");
        }

        cta::common::dataStructures::SecurityIdentity securityIdentity;
        cta::common::dataStructures::DesiredDriveState driveState;
        driveState.up = false;
        driveState.forceDown = false;
        scheduler.createTapeDriveStatus(driveInfo, driveState, common::dataStructures::MountType::NoMount,
                                        common::dataStructures::DriveStatus::Down, m_configLine, securityIdentity, lc);

        // Get the drive state to see if there is a reason or not, we don't want to change the reason
        // why a drive is down at the startup of the tapeserver. If it's setted up a previous Reason From Log
        // it will be change for this one.
        if (!currentDesiredDriveState.reason) {
          driveState.setReasonFromLogMsg(logLevel, msg);
        } else if (currentDesiredDriveState.reason.value().substr(0, 11) == "[cta-taped]") {
          driveState.setReasonFromLogMsg(logLevel, msg);
        } else {
          driveState.reason = currentDesiredDriveState.reason.value();
        }

        scheduler.setDesiredDriveState(securityIdentity, m_configLine.unitName, driveState, lc);
        scheduler.reportDriveConfig(m_configLine, m_tapedConfig, lc);
      } catch (cta::exception::Exception& ex) {
        params.add("Message", ex.getMessageValue())
              .add("Backtrace", ex.backtrace());
        lc.log(log::CRIT, "In DriveHandler::runChild(): failed to set drive down");
        // This is a fatal error (failure to access the scheduler). Shut daemon down.
        driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
        return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
      }
    }

    castor::tape::tapeserver::daemon::DataTransferSession dataTransferSession(
      cta::utils::getShortHostname(),
      lc.logger(),
      sWrapper,
      m_configLine,
      mediaChangerFacade,
      driveHandlerProxy,
      capUtils,
      dataTransferConfig,
      scheduler);

    auto ret = dataTransferSession.execute();
    return ret;
  }
}

//------------------------------------------------------------------------------
// DriveHandler::shutdown
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus DriveHandler::shutdown() {
  auto exitShutdown = [this]() -> SubprocessHandler::ProcessingStatus {
    m_sessionState = SessionState::Shutdown;
    m_processingStatus.nextTimeout = m_processingStatus.nextTimeout.max();
    m_processingStatus.forkRequested = false;
    m_processingStatus.killRequested = false;
    m_processingStatus.shutdownComplete = true;
    m_processingStatus.sigChild = false;
    return m_processingStatus;
  };
  // TODO: improve in the future (preempt the child process)
  auto& lc = m_processManager.logContext();
  log::ScopedParamContainer params(lc);
  params.add("tapeDrive", m_configLine.unitName);
  lc.log(log::INFO, "In DriveHandler::shutdown(): simply killing the process.");
  kill();

  // Mounting management.
  if (!m_catalogue)
    m_catalogue = createCatalogue("DriveHandler::shutdown()");

  // Create the scheduler
  std::unique_ptr<SchedulerDBInit_t> sched_db_init;

  try {
    std::string processName = "DriveHandlerShutdown-";
    processName+=m_configLine.unitName;
    log::ScopedParamContainer params(lc);
    params.add("processName", processName);
    lc.log(log::DEBUG, "In DriveHandler::shutdown(): will create agent entry. Enabling leaving non-empty agent behind.");
    sched_db_init.reset(new SchedulerDBInit_t(processName, m_tapedConfig.backendPath.value(), lc.logger(), true));
  } catch (cta::exception::Exception &ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandler::shutdown(): failed to connect to objectstore or failed to instantiate agent entry. Reporting fatal error.");
    // Putting the drive down
    try {
      setDriveDownForShutdown("Failed to connect to objectstore or failed to instantiate agent entry", &lc);
    } catch(const cta::exception::Exception &ex) {
      params.add("tapeVid", m_sessionVid)
            .add("tapeDrive", m_configLine.unitName)
            .add("message", ex.getMessageValue());
      lc.log(cta::log::ERR, "In DriveHandler::shutdown(). Failed to put the drive down.");
    }
    return exitShutdown();
  }
  std::unique_ptr<SchedulerDB_t> sched_db = sched_db_init->getSchedDB(*m_catalogue, lc.logger());
  lc.log(log::DEBUG, "In DriveHandler::shutdown(): will create scheduler.");
  auto scheduler = std::make_unique<Scheduler>(*m_catalogue, *sched_db, 0, 0);

  std::set<SessionState> statesRequiringCleaner = { SessionState::Mounting,
    SessionState::Running, SessionState::Unmounting };
  if (statesRequiringCleaner.count(m_sessionState)) {
    if (!m_sessionVid.size()) {
      lc.log(log::ERR, "In DriveHandler::shutdown(): Should run cleaner but VID is missing. Do nothing.");
    }
    else {
      log::ScopedParamContainer scoped(m_processManager.logContext());
      scoped.add("tapeVid", m_sessionVid)
            .add("tapeDrive", m_configLine.unitName)
            .add("sessionState", session::toString(m_sessionState))
            .add("sessionType", session::toString(m_sessionType));
      lc.log(log::INFO, "In DriveHandler::shutdown(): starting cleaner.");
      // Capabilities management.
      cta::server::ProcessCap capUtils;

      cta::mediachanger::MediaChangerFacade mediaChangerFacade(m_processManager.logContext().logger());
      castor::tape::System::realWrapper sWrapper;
      castor::tape::tapeserver::daemon::CleanerSession cleanerSession(
        capUtils,
        mediaChangerFacade,
        m_processManager.logContext().logger(),
        m_configLine,
        sWrapper,
        m_sessionVid,
        true,
        m_tapedConfig.tapeLoadTimeout.value(),
        "",
        *m_catalogue,
        *scheduler);
      if (cleanerSession.execute() == castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN) {
        return exitShutdown();
      }
    }
  }
  // Putting the drive down
  try {
    setDriveDownForShutdown("Shutdown", &lc);
  } catch(const cta::exception::Exception &ex) {
    params.add("tapeVid", m_sessionVid)
          .add("tapeDrive", m_configLine.unitName)
          .add("message", ex.getMessageValue());
    lc.log(cta::log::ERR, "In DriveHandler::shutdown(). Failed to put the drive down.");
  }
  return exitShutdown();
}

int DriveHandler::setDriveDownForShutdown(const std::string& reason, cta::log::LogContext* lc) {
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = m_configLine.unitName;
  driveInfo.logicalLibrary = m_configLine.logicalLibrary;
  driveInfo.host = cta::utils::getShortHostname();

  auto driveState = m_catalogue->DriveState()->getTapeDrive(driveInfo.driveName);
  if (!driveState) {
    lc->log(cta::log::WARNING, "In DriveHandler::setDriveDownForShutdown(). TapeDrive to set down doesn't exist.");
    return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
  }

  cta::common::dataStructures::DesiredDriveState desiredDriveState;
  desiredDriveState.up = false;
  desiredDriveState.forceDown = false;

  // Get the drive state to see if there is a reason or not, we don't want to change the reason
  // why a drive is down at the shutdown of the tapeserver. If it's setted up a previous Reason From Log
  // it will be change for this new one.
  if (!driveState.value().reasonUpDown) {
    desiredDriveState.setReasonFromLogMsg(cta::log::INFO, reason);
  } else if (driveState.value().reasonUpDown.value().substr(0, 11) == "[cta-taped]") {
    desiredDriveState.setReasonFromLogMsg(cta::log::INFO, reason);
  } else {
    desiredDriveState.reason = driveState.value().reasonUpDown.value();
  }

  TapeDrivesCatalogueState driveCatalogue(*m_catalogue);
  driveCatalogue.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount,
    cta::common::dataStructures::DriveStatus::Down, time(nullptr), *lc);
  driveCatalogue.setDesiredDriveState(m_configLine.unitName, desiredDriveState, *lc);

  return castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN;
}

std::unique_ptr<cta::catalogue::Catalogue> DriveHandler::createCatalogue(const std::string& methodCaller) {
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("fileCatalogConfigFile", m_tapedConfig.fileCatalogConfigFile.value());
  params.add("caller", methodCaller);
  m_processManager.logContext().log(log::DEBUG, "In DriveHandler::createCatalogue(): will get catalogue login information.");
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig.fileCatalogConfigFile.value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;
  m_processManager.logContext().log(log::DEBUG, "In DriveHandler::createCatalogue(): will connect to catalogue.");
  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_sessionEndContext.logger(),
  catalogueLogin, nbConns, nbArchiveFileListingConns);
  return catalogueFactory->create();
}

//------------------------------------------------------------------------------
// DriveHandler::~DriveHandler
//------------------------------------------------------------------------------
DriveHandler::~DriveHandler() {
  // TODO: complete
}


}
}
}  // namespace cta::tape::daemon
