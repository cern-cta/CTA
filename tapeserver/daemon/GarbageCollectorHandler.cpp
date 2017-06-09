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

#include "GarbageCollectorHandler.hpp"
#include "common/exception/Errnum.hpp"
#include "objectstore/AgentHeartbeatThread.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/GarbageCollector.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "scheduler/Scheduler.hpp"
#include "rdbms/Login.hpp"
#include "common/make_unique.hpp"

#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>

namespace cta { namespace tape { namespace  daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
GarbageCollectorHandler::GarbageCollectorHandler(const TapedConfiguration& tapedConfig, ProcessManager& pm):
SubprocessHandler("garbageCollector"), m_processManager(pm), m_tapedConfig(tapedConfig) {
  // As the handler is started, its first duty is to create a new subprocess. This
  // will be managed by the process manager (initial request in getInitialStatus)
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::getInitialStatus
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::getInitialStatus() {
  m_processingStatus.forkRequested=true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::getInitialStatus
//------------------------------------------------------------------------------
void GarbageCollectorHandler::postForkCleanup() {
  // We are in the child process of another handler. We can close our socket pair
  // without re-registering it from poll.
  m_socketPair.reset(nullptr);
}


//------------------------------------------------------------------------------
// GarbageCollectorHandler::fork
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::fork() {
  // If anything fails while attempting to fork, we will have to declare ourselves
  // failed and ask for a shutdown by sending the TERM signal to the parent process
  // This will ensure the shutdown-kill sequence managed by the signal handler without code duplication.
  // Record we no longer ask for fork
  m_processingStatus.forkRequested = false;
  try {
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
      // Close child side of socket.
      m_socketPair->close(server::SocketPair::Side::child);
      // We are now ready to react to timeouts and messages from the child process.
      return m_processingStatus;
    }
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    m_processManager.logContext().log(log::ERR, "Failed to fork garbage collector process. Initiating shutdown with SIGTERM.");
    // Wipe all previous states as we are shutting down
    m_processingStatus = SubprocessHandler::ProcessingStatus();
    m_processingStatus.shutdownComplete=true;
    m_processingStatus.forkState=SubprocessHandler::ForkState::parent;
    // Initiate shutdown
    ::kill(::getpid(), SIGTERM);
    return m_processingStatus;
  }
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::kill
//------------------------------------------------------------------------------
void GarbageCollectorHandler::kill() {
  // If we have a subprocess, kill it and wait for completion (if needed). We do not need to keep
  // track of the exit state as kill() means we will not be called anymore.
  log::ScopedParamContainer params(m_processManager.logContext());
  if (m_pid != -1) {
    params.add("SubProcessId", m_pid);
    // The socket pair will be reopened on the next fork. Clean it up.
    m_socketPair.reset(nullptr);
    try {
      exception::Errnum::throwOnMinusOne(::kill(m_pid, SIGKILL),"Failed to kill() subprocess");
      int status;
      // wait for child process exit
      exception::Errnum::throwOnMinusOne(::waitpid(m_pid, &status, 0), "Failed to waitpid() subprocess");
      // Log the status
      params.add("WIFEXITED", WIFEXITED(status));
      if (WIFEXITED(status)) {
        params.add("WEXITSTATUS", WEXITSTATUS(status));
      } else {
        params.add("WIFSIGNALED", WIFSIGNALED(status));
      } 
      m_processManager.logContext().log(log::INFO, "In GarbageCollectorHandler::kill(): sub process completed");
    } catch (exception::Exception & ex) {
      params.add("Exception", ex.getMessageValue());
      m_processManager.logContext().log(log::ERR, "In GarbageCollectorHandler::kill(): failed to kill existing subprocess");
    }
  } else {
    m_processManager.logContext().log(log::INFO, "In GarbageCollectorHandler::kill(): no subprocess to kill");
  }
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::processEvent
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::processEvent() {
  // We do not expect any feedback for the child process...
  m_processManager.logContext().log(log::WARNING, "In GarbageCollectorHandler::processEvent(): spurious event");
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::processSigChild
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::processSigChild() {
  // Check out child process's status. If the child process is still around,
  // waitpid will return 0. Non zero if process completed (and status needs to 
  // be picked up) and -1 if the process is entirely gone.
  // Of course we might not have a child process to begin with.
  log::ScopedParamContainer params(m_processManager.logContext());
  if (-1 == m_pid) return m_processingStatus;
  int processStatus;
  int rc=::waitpid(m_pid, &processStatus, WNOHANG);
  // Check there was no error.
  try {
    exception::Errnum::throwOnMinusOne(rc);
  } catch (exception::Exception ex) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("pid", m_pid)
          .add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::WARNING,
        "In GarbageCollectorHandler::processSigChild(): failed to get child process exit code. Doing nothing as we are unable to determine if it is still running or not.");
    return m_processingStatus;
  }
  if (rc) {
    // It was our process. In all cases we prepare the space for the new session
    // We did collect the exit code of our child process
    // How well did it finish? (exit() or killed?)
    // The socket pair will be reopened on the next fork. Clean it up.
    m_socketPair.reset(nullptr);
    params.add("pid", m_pid);
    if (WIFEXITED(processStatus)) {
      // Child process exited properly. The new child process will not need to start
      // a cleanup session.
      params.add("exitCode", WEXITSTATUS(processStatus));
      // If we are shutting down, we should not request a new session.
      if (!m_shutdownInProgress) {
        m_processManager.logContext().log(log::INFO, "Garbage collector subprocess exited. Will spawn a new one.");
        m_processingStatus.forkRequested=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      } else {
        m_processManager.logContext().log(log::INFO, "Garbage collector subprocess exited. Will not spawn new one as we are shutting down.");
        m_processingStatus.forkRequested=false;
        m_processingStatus.shutdownComplete=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      }
    } else {
      params.add("IfSignaled", WIFSIGNALED(processStatus))
            .add("TermSignal", WTERMSIG(processStatus))
            .add("CoreDump", WCOREDUMP(processStatus));
      // If we are shutting down, we should not request a new session.
      if (!m_shutdownInProgress) {
        m_processManager.logContext().log(log::INFO, "Garbage collector subprocess crashed. Will spawn a new one.");
        m_processingStatus.forkRequested=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      } else {
        m_processManager.logContext().log(log::INFO, "Garbage collector subprocess crashed. Will not spawn new one as we are shutting down.");
        m_processingStatus.forkRequested=false;
        m_processingStatus.shutdownComplete=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      }
    }
    // In all cases, we do not have a PID anymore
    m_pid=-1;
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::processTimeout
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::processTimeout() {
  // The only time we expect a timeout is when shutting down
  if (!m_shutdownInProgress) {
    m_processManager.logContext().log(log::WARNING, "In GarbageCollectorHandler::processTimeout(): spurious timeout: no shutdown");
    return m_processingStatus;
  } 
  // We were shutting down and the child process did not exit in time. Killing it.
  if (-1!=m_pid) {
    m_processManager.logContext().log(log::WARNING, 
        "In GarbageCollectorHandler::processTimeout(): spurious timeout: no more process");
  } else {
    // We will help the exit of the child process by killing it.
    m_processManager.logContext().log(log::WARNING, "In GarbageCollectorHandler::processTimeout(): will kill subprocess");
    kill();
  }
  // In all cases, the shutdown is complete.
  m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
  m_processingStatus.shutdownComplete=true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::runChild
//------------------------------------------------------------------------------
int GarbageCollectorHandler::runChild() {
  // We are in the child process. It is time to open connections to the catalogue
  // and object store, and run the garbage collector.
  // We do not have to care for previous crashed sessions as we will garbage
  // collect them like any other crashed agent.
  
  // Set the thread name for process ID:
  prctl(PR_SET_NAME, "cta-taped-gc");
  
  // Before anything, we will check for access to the scheduler's central storage.
  // If we fail to access it, we cannot work. We expect the drive processes to
  // fail likewise, so we just wait for shutdown signal (no feedback to main
  // process).
  std::unique_ptr<cta::objectstore::Backend> backend(
    cta::objectstore::BackendFactory::createBackend(m_tapedConfig.objectStoreURL.value()).release());
  // If the backend is a VFS, make sure we don't delete it on exit.
  // If not, nevermind.
  try {
    dynamic_cast<cta::objectstore::BackendVFS &>(*backend).noDeleteOnExit();
  } catch (std::bad_cast &){}
  // Create the agent entry in the object store. This could fail (even before ping, so
  // handle failure like a ping failure).
  std::unique_ptr<cta::objectstore::BackendPopulator> backendPopulator;
  std::unique_ptr<cta::OStoreDBWithAgent> osdb;
  std::unique_ptr<cta::catalogue::Catalogue> catalogue;
  std::unique_ptr<cta::Scheduler> scheduler;
  try {
    backendPopulator.reset(new cta::objectstore::BackendPopulator(*backend, "garbageCollector"));
    osdb.reset(new cta::OStoreDBWithAgent(*backend, backendPopulator->getAgentReference()));
    const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig.fileCatalogConfigFile.value());
    const uint64_t nbConns = 1;
    catalogue=cta::catalogue::CatalogueFactory::create(catalogueLogin, nbConns);
    scheduler=make_unique<cta::Scheduler>(*catalogue, *osdb, 5, 2*1000*1000); //TODO: we have hardcoded the mount policy parameters here temporarily we will remove them once we know where to put them
  // Before launching the transfer session, we validate that the scheduler is reachable.
    scheduler->ping();
  } catch(cta::exception::Exception &ex) {
    {
      log::ScopedParamContainer param(m_processManager.logContext());
      param.add("errorMessage", ex.getMessageValue());
      m_processManager.logContext().log(log::CRIT, 
          "In GarbageCollectorHandler::runChild(): contact central storage. Waiting for shutdown.");
    }
    server::SocketPair::pollMap pollList;
    pollList["0"]=m_socketPair.get();
    // Wait forever (negative timeout) for something to come from parent process.
    server::SocketPair::poll(pollList, -1, server::SocketPair::Side::parent);
    m_processManager.logContext().log(log::INFO,
        "In GarbageCollectorHandler::runChild(): Received shutdown message after failure to contact storage. Exiting.");
    return EXIT_FAILURE; 
  }
  
  // The object store is accessible, let's turn the agent heartbeat on.
  objectstore::AgentHeartbeatThread agentHeartbeat(backendPopulator->getAgentReference(), *backend, m_processManager.logContext().logger());
  agentHeartbeat.startThread();
  
  // Create the garbage collector itself
  objectstore::Agent ag(backendPopulator->getAgentReference().getAgentAddress(), *backend);
  objectstore::GarbageCollector gc(*backend, ag);
  
  // Run the gc in a loop
  try {
    server::SocketPair::pollMap pollList;
    pollList["0"]=m_socketPair.get();
    bool receivedMessage=false;
    do {
      m_processManager.logContext().log(log::DEBUG, 
          "In GarbageCollectorHandler::runChild(): About to run a GC pass.");
      gc.runOnePass(m_processManager.logContext());
      try {
        server::SocketPair::poll(pollList, s_pollInterval, server::SocketPair::Side::parent);
        receivedMessage=true;
      } catch (server::SocketPair::Timeout & ex) {}
    } while (!receivedMessage);
    m_processManager.logContext().log(log::INFO,
        "In GarbageCollectorHandler::runChild(): Received shutdown message. Exiting.");
  } catch (cta::exception::Exception & ex) {
    {
      log::ScopedParamContainer params(m_processManager.logContext());
      params.add("Message", ex.getMessageValue());
      m_processManager.logContext().log(log::ERR, 
          "In GarbageCollectorHandler::runChild(): received an exception. Backtrace follows.");
    }
    m_processManager.logContext().logBacktrace(log::ERR, ex.backtrace());
  }
  agentHeartbeat.stopAndWaitThread();
  return EXIT_SUCCESS;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::shutdown
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus GarbageCollectorHandler::shutdown() {
  // We will signal the shutdown to the child process by sending a byte over the 
  // socket pair (if we have one)
  m_shutdownInProgress=true;
  if (!m_socketPair.get()) {
    m_processManager.logContext().log(log::WARNING, "In GarbageCollectorHandler::shutdown(): no socket pair");
  } else {
    m_processManager.logContext().log(log::INFO, "In GarbageCollectorHandler::shutdown(): sent shutdown message to child process");
    m_socketPair->send("\0");
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// GarbageCollectorHandler::~GarbageCollectorHandler
//------------------------------------------------------------------------------
GarbageCollectorHandler::~GarbageCollectorHandler() {
  // If we still have a child process (should not), just stop it the hard way.
  if (-1 != m_pid) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("pid", m_pid);
    ::kill(m_pid, SIGKILL);
    m_processManager.logContext().log(log::WARNING, "In GarbageCollectorHandler::~GarbageCollectorHandler(): killed leftover subprocess");
  }
}

}}} // namespace cta::tape::daemon