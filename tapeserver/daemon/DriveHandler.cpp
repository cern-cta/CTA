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

#include "DriveHandler.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Errnum.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <set>

namespace cta { namespace tape { namespace  daemon {

DriveHandler::DriveHandler(const TapedConfiguration & tapedConfig, const TpconfigLine& configline, ProcessManager& pm):
  SubprocessHandler(std::string("drive:")+configline.unitName), m_processManager(pm), 
  m_tapedConfig(tapedConfig), m_configLine(configline),
  m_sessionEndContext(m_processManager.logContext().logger()) {
  // As the handler is started, its first duty is to create a new subprocess. This
  // will be managed by the process manager (initial request in getInitialStatus)
}
  
const std::map<DriveHandler::SessionState, DriveHandler::Timeout> DriveHandler::m_timeouts = {
  {SessionState::Cleaning, std::chrono::duration_cast<Timeout>(std::chrono::minutes(10))},
  {SessionState::Scheduling, std::chrono::duration_cast<Timeout>(std::chrono::minutes(5))},
  {SessionState::Mounting, std::chrono::duration_cast<Timeout>(std::chrono::minutes(10))},
  // The running timeout is a heartbeat timeout. It is distinct from 
  {SessionState::Running, std::chrono::duration_cast<Timeout>(std::chrono::minutes(2))},
  {SessionState::Unmounting, std::chrono::duration_cast<Timeout>(std::chrono::minutes(10))},
  {SessionState::DrainingToDisk, std::chrono::duration_cast<Timeout>(std::chrono::minutes(30))},
  {SessionState::ClosingDown, std::chrono::duration_cast<Timeout>(std::chrono::seconds(5))}
};

const DriveHandler::Timeout DriveHandler::m_dataMovementTimeout = 
  std::chrono::duration_cast<Timeout>(std::chrono::minutes(30));

SubprocessHandler::ProcessingStatus DriveHandler::getInitialStatus() {
  // As we just start, we need to fork the first subprocess
  m_processingStatus.forkRequested = true;
  return m_processingStatus;
}

void DriveHandler::prepareForFork() {
  // Nothing to do before a fork (ours or other's)
}

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
      err << "In DriveHandler::fork(): called while not in the expected state: " << toString(m_sessionState)
          << " instead of " << toString(SessionState::PendingFork);
      throw exception::Exception(err.str());
    }
    // First prepare a socket pair for this new subprocess
    m_socketPair.reset(new cta::server::SocketPair());
    // and fork
    m_pid=::fork();
    exception::Errnum::throwOnMinusOne(m_pid, "In DriveHandler::fork(): failed to fork()");
    if (!m_pid) {
      // We are in the child process
      SubprocessHandler::ProcessingStatus ret;
      ret.forkState = SubprocessHandler::ForkState::child;
      return ret;
    } else {
      // We are in the parent process
      m_processingStatus.forkState = SubprocessHandler::ForkState::parent;
      // The subprocess will start cleaning up of scheduling depending on the state of the previous session
      switch(m_previousSession) {
        // If the session is starting up of crashed/was killed before, it will first cleanup:
      case PreviousSession::Initiating:
      case PreviousSession::Killed:
      case PreviousSession::Crashed:
        m_sessionState=SessionState::Cleaning;
        // Else (OK of known failure to unmount, we wait for the scheduling (including 
        // the operator putting the drive up if previous session had failed)) without
        // calling the cleaner.
      case PreviousSession::OK:
      case PreviousSession::Failed:
        m_sessionState=SessionState::Scheduling;
      }
      m_sessionState = SessionState::Cleaning;
      // Compute the next timeout
      m_processingStatus.nextTimeout = nextTimeout();
      // Register our socketpair side for epoll after closing child side.
      m_socketPair->close(server::SocketPair::Side::child);
      m_processManager.addFile(m_socketPair->getFdForAccess(server::SocketPair::Side::child), this);
      // We are now ready to react to timeouts and messages from the child process.
      return m_processingStatus;
    }
  } catch (cta::exception::Exception & ex) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Drive", m_configLine.unitName)
          .add("Error", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR, "Failed to fork drive process. Initiating shutdown with SIGTERM.");
    // Wipe all previous states as we are shutting down
    m_processingStatus = SubprocessHandler::ProcessingStatus();
    m_sessionState=SessionState::Shutdown;
    m_processingStatus.shutdownComplete=true;
    m_processingStatus.forkState=SubprocessHandler::ForkState::parent;
    // Initiate shutdown
    ::kill(::getpid(), SIGTERM);
    return m_processingStatus;
  }
}

decltype (SubprocessHandler::ProcessingStatus::nextTimeout) DriveHandler::nextTimeout() {
  // If a timeout is defined, then we compute its expiration time. Else we just give default timeout (end of times).
  try {
    return std::chrono::steady_clock::now()+m_timeouts.at(m_sessionState);
  } catch (...) {
    return decltype(SubprocessHandler::ProcessingStatus::nextTimeout)::max();
  }
}

std::string DriveHandler::toString(SessionState state) {
  switch(state) {
  case SessionState::PendingFork:
    return "PendingFork";
  case SessionState::Cleaning:
    return "Cleaning";
  case SessionState::Scheduling:
    return "Scheduling";
  case SessionState::Mounting:
    return "Mounting";
  case SessionState::Running:
    return "Running";
  case SessionState::Unmounting:
    return "Unmounting";
  case SessionState::Shutdown:
    return "Shutdown";
  default:
    {
      std::stringstream st;
      st << "UnknownState (" << ((uint32_t) state) << ")";
      return st.str();
    }
  }
}

std::string DriveHandler::toString(SessionType type) {
  switch(type) {
  case SessionType::Archive:
    return "Archive";
  case SessionType::Retrieve:
    return "Retrieve";
  case SessionType::Undetermined:
    return "Undetermined";
  default:
    {
      std::stringstream st;
      st << "UnknownType (" << ((uint32_t) type) << ")";
      return st.str();
    }
  }
}


void DriveHandler::kill() {
  // If we have a subprocess, kill it and wait for completion (if needed). We do not need to keep
  // track of the exit state as kill() mens we will not be called anymore.
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("unitName", m_configLine.unitName);
  if (m_pid != -1) {
    ::kill(m_pid, SIGKILL);
    int status;
    // wait for child process exit
    ::waitpid(m_pid, &status, 0);
    // Log the status
    params.add("pid", m_pid)
          .add("WIFEXITED", WIFEXITED(status));
    if (WIFEXITED(status)) {
      params.add("WEXITSTATUS", WEXITSTATUS(status));
    } else {
      params.add("WIFSIGNALED", WIFSIGNALED(status));
    }
    m_processManager.logContext().log(log::INFO, "In DriveHandler::kill(): sub process completed");
  } else {
    m_processManager.logContext().log(log::INFO, "In DriveHandler::kill(): no subprocess to kill");
  }
  
}

SubprocessHandler::ProcessingStatus DriveHandler::processEvent() {
  // Read from the socket pair 
  try {
    serializers::WatchdogMessage message;
    message.ParseFromString(m_socketPair->receive());
    processLogs(message);
    switch((SessionState)message.sessionstate()) {
    case SessionState::Scheduling:
      return processScheduling(message);
    case SessionState::Mounting:
      return processMounting(message);
    case SessionState::Running:
      return processRunning(message);
    case SessionState::Unmounting:
      return processUnmounting(message);
    case SessionState::DrainingToDisk:
      return processDrainingToDisk(message);
    case SessionState::ClosingDown:
      return processClosingDown(message);
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "In DriveHandler::processEvent(): unexpected session state:" 
            << toString((SessionState)message.sessionstate());
        throw ex;
      }
    }
  } catch(cta::exception::Exception & ex) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR, 
        "In DriveHandler::processEvent(): failed");
    return m_processingStatus;
  }
}

SubprocessHandler::ProcessingStatus DriveHandler::processScheduling(serializers::WatchdogMessage& message) {
  // We are either going to schedule again either after failing to schedule (drive down or nothing to do) or
  // after successfully cleaning the drive.
  // Check the transition is expected. This is non-fatal as the drive session has the last word anyway.
  std::set<SessionState> expectedStates = { SessionState::Cleaning, SessionState::Scheduling };
  if (expectedStates.find(m_sessionState)==expectedStates.end()) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processScheduling(): unexpected previous state.");
  }
  m_sessionState=SessionState::Scheduling;
  if ((SessionType)message.sessiontype()!= SessionType::Undetermined) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Type", toString((SessionType)message.sessiontype()));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processScheduling(): unexpected session type reported.");
  }
  // We keep the type as should be here:
  m_sessionType=SessionType::Undetermined;
  // Set the timeout for this state
  m_processingStatus.nextTimeout=nextTimeout();
  return m_processingStatus;
}

SubprocessHandler::ProcessingStatus DriveHandler::processMounting(serializers::WatchdogMessage& message) {
  // The only transition expected is from scheduling.
  if (SessionState::Scheduling != m_sessionState) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processMounting(): unexpected previous state.");
  }
  // The type of mount is expected here. If we do not get it, the process is killed.
  std::set<SessionType> expectedTypes = { SessionType::Archive, SessionType::Retrieve };
  if (expectedTypes.end()==expectedTypes.find((SessionType)message.sessiontype())) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("Type", toString((SessionType)message.sessiontype()));
    m_processManager.logContext().log(log::ERR, 
        "In DriveHandler::processMounting(): unexpected session type reported. Killing the session.");
    ::kill(m_pid, SIGKILL);
    // processSigChild will do the rest for us.
    // We expect nothing anymore from the sub process...
    m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
    return m_processingStatus;
  }
  m_sessionState=SessionState::Mounting;
  m_sessionType=(SessionType)message.sessiontype();
  // update the new timeout
  m_processingStatus.nextTimeout=nextTimeout();
  return m_processingStatus;
}

SubprocessHandler::ProcessingStatus DriveHandler::processRunning(serializers::WatchdogMessage& message) {
  // This status can be reported repeatedly (or we can transition from the previous one: Mounting).
  // Log the (possible) unexpected previous state.
  std::set<SessionState> expectedStates = { SessionState::Mounting, SessionState::Running };
  if (expectedStates.end() == expectedStates.find(m_sessionState)) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processRunning(): unexpected previous state.");
  }
  // We should at least already have the direction defined, and it should not change. We will kill the
  // subprocess if those conditions are not met.
  std::set<SessionType> expectedTypes = { SessionType::Archive, SessionType::Retrieve };
  if ((expectedTypes.end()==expectedTypes.find(m_sessionType)) ||
        ((SessionType)message.sessiontype() != m_sessionType)) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousType", toString(m_sessionType));
    params.add("Type", toString((SessionType)message.sessiontype()));
    m_processManager.logContext().log(log::ERR, 
        "In DriveHandler::processRunning(): unexpected session type reported "
        "or incompatible previous type. Killing the session.");
    ::kill(m_pid, SIGKILL);
    // processSigChild will do the rest for us.
    // We expect nothing anymore from the sub process...
    m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
    return m_processingStatus;
  }
  // We are dealing with 2 competing timeouts here.
  // First is the data movement timeout. Second is the heartbeat timeout (default one).
  // If we transition from another state, we reset the block movement timer.
  // We also reset the timer if data block movement is reported
  if ((m_sessionState != SessionState::Running) || message.memblocksmoved()) {
    m_lastBlockMovementTime=std::chrono::steady_clock::now();
  }
  // Now record the 
  // We can now compute the timeout and check for potential exceeding of timeout
  auto nextHeartbeatTimeout = nextTimeout();
  auto nextBlockMoveTimeout = std::chrono::steady_clock::now() + m_dataMovementTimeout;
  m_processingStatus.nextTimeout = std::min(nextBlockMoveTimeout, nextHeartbeatTimeout);
  return m_processingStatus;
}

SubprocessHandler::ProcessingStatus DriveHandler::processUnmounting(serializers::WatchdogMessage& message) {
  // This status transition is exclusively expected from running
  if (SessionState::Running != m_sessionState) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processUnmounting(): unexpected previous state.");
  }
  // Check if the session type is consistent. It is still important as the post-unmount
  // behavior depends on it.
  if ((SessionType)message.sessiontype()!=m_sessionType) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousType", toString(m_sessionType));
    params.add("Type", toString((SessionType)message.sessiontype()));
    m_processManager.logContext().log(log::ERR, 
        "In DriveHandler::processUnmounting(): change of session type detected. "
        "Killing the session.");
    ::kill(m_pid, SIGKILL);
    // processSigChild will do the rest for us.
    // We expect nothing anymore from the sub process...
    m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
    return m_processingStatus;
  }
  m_sessionState=SessionState::Unmounting;
  m_processingStatus.nextTimeout=nextTimeout();
  return m_processingStatus;
}

SubprocessHandler::ProcessingStatus DriveHandler::processDrainingToDisk(serializers::WatchdogMessage& message) {
  // This status transition is expected from unmounting.
  if (SessionState::Unmounting != m_sessionState) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processDrainingToDisk(): unexpected previous state.");
  }
  // This state is only for retrieve sessions.
  if ((SessionType::Retrieve!=m_sessionType) 
       || (SessionType::Retrieve!=(SessionType)message.sessiontype())) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousType", toString(m_sessionType));
    params.add("Type", toString((SessionType)message.sessiontype()));
    m_processManager.logContext().log(log::ERR, 
        "In DriveHandler::processDrainingToDisk(): change of session type or "
        "incompatible session type detected. Killing the session.");
    ::kill(m_pid, SIGKILL);
    // processSigChild will do the rest for us.
    // We expect nothing anymore from the sub process...
    m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
    return m_processingStatus;
  }
  // We have a simple timeout configuration here
  m_sessionState=SessionState::DrainingToDisk;
  m_processingStatus.nextTimeout=nextTimeout();
  return m_processingStatus;
}

SubprocessHandler::ProcessingStatus DriveHandler::processClosingDown(serializers::WatchdogMessage& message) {
  // We expect to transition from Unmounting or draining to disk
  std::set<SessionState> expectedStates = { SessionState::Mounting, SessionState::Running };
  if (expectedStates.end() == expectedStates.find(m_sessionState)) {
    log::ScopedParamContainer params(m_processManager.logContext());
    params.add("PreviousState", toString(m_sessionState));
    m_processManager.logContext().log(log::WARNING, 
        "In DriveHandler::processClosingDown(): unexpected previous state.");
  }
  // We have a simple timeout configuration here
  m_sessionState=SessionState::ClosingDown;
  m_processingStatus.nextTimeout=nextTimeout();
  return m_processingStatus;
}

void DriveHandler::processLogs(serializers::WatchdogMessage& message) {
  // Accumulate the logs added (if any)
  for (auto & log: message.addedlogparams()) {
    m_sessionEndContext.pushOrReplace({log.name(), log.value()});
  }
  for (auto & log: message.deletedlogparams()) {
    m_sessionEndContext.erase(log);
  }
}

SubprocessHandler::ProcessingStatus DriveHandler::processSigChild() {
  // Check out child process's status. If the child process is still around,
  // waitpid will return 0. Non zero if process completed (and status needs to 
  // be picked up) and -1 if the process is entirely gone.
  // Of course we might not have a child process to begin with.
  if (-1 == m_pid) return m_processingStatus;
  int processStatus;
  int rc=::waitpid(m_pid, &processStatus, WNOHANG);
  if (rc) {
    try {
      exception::Errnum::throwOnMinusOne(rc);
      // We did collect the exit code of our child process
      //TODOTODO
      throw 0;
    } catch (exception::Exception ex) {
      log::ScopedParamContainer params(m_processManager.logContext());
      params.add("pid", m_pid)
            .add("Message", ex.getMessageValue())
            .add("SessionState", toString(m_sessionState))
            .add("SessionType", toString(m_sessionType));
      m_processManager.logContext().log(log::WARNING,
          "In DriveHandler::processSigChild(): failed to get child process exit code. "
          "Will respawn new subprocess.");
      m_pid=-1;
      m_sessionState=SessionState::PendingFork;
      m_sessionType=SessionType::Undetermined;
      m_previousSession=PreviousSession::Crashed;
      m_processingStatus.nextTimeout=decltype(m_processingStatus.nextTimeout)::max();
      m_processingStatus.forkRequested=true;
      return m_processingStatus;
    }
  } else {
    // It was not out sub process, we carry on.
    return m_processingStatus;
  }
}

SubprocessHandler::ProcessingStatus DriveHandler::processTimeout() {
  throw 0;
  // TODO
}

int DriveHandler::runChild() {
  ::exit(EXIT_FAILURE);
  // TODO
}

SubprocessHandler::ProcessingStatus DriveHandler::shutdown() {
  throw 0;
  // TODO
}

DriveHandler::~DriveHandler() {
  // TODO: complete
}


}}}  // namespace cta::tape::daemon