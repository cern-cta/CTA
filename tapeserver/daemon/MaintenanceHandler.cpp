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

#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/Logger.hpp"
#include "common/log/FileLogger.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/DiskReportRunner.hpp"
#include "scheduler/RepackRequestManager.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/daemon/MaintenanceHandler.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/PostgresSchedDB/PostgresSchedDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MaintenanceHandler::MaintenanceHandler(const TapedConfiguration& tapedConfig, ProcessManager& pm):
SubprocessHandler("maintenanceHandler"), m_processManager(pm), m_tapedConfig(tapedConfig) {
  // As the handler is started, its first duty is to create a new subprocess. This
  // will be managed by the process manager (initial request in getInitialStatus)
}

//------------------------------------------------------------------------------
// MaintenanceHandler::getInitialStatus
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::getInitialStatus() {
  m_processingStatus.forkRequested=true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// MaintenanceHandler::getInitialStatus
//------------------------------------------------------------------------------
void MaintenanceHandler::postForkCleanup() {
  // We are in the child process of another handler. We can close our socket pair
  // without re-registering it from poll.
  m_socketPair.reset(nullptr);
  // We forget about our child process which is not handled by siblings.
  m_pid = -1;
}


//------------------------------------------------------------------------------
// MaintenanceHandler::fork
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::fork() {
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
    exception::Errnum::throwOnMinusOne(m_pid, "In MaintenanceHandler::fork(): failed to fork()");
    if (!m_pid) {
      // We are in the child process

      // Start the FileLogger file descriptor update thread.
      if(m_processManager.logContext().logger().m_logType == cta::log::LoggerType::FILE){
        auto& fileLogger =
          static_cast<cta::log::FileLogger&>(m_processManager.logContext().logger());
        fileLogger.startFdThread();
      }

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
  } catch (cta::exception::Exception &) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    m_processManager.logContext().log(log::ERR, "Failed to fork maintenance process. Initiating shutdown with SIGTERM.");
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
// MaintenanceHandler::kill
//------------------------------------------------------------------------------
void MaintenanceHandler::kill() {
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
      m_processManager.logContext().log(log::INFO, "In MaintenanceHandler::kill(): sub process completed");
    } catch (exception::Exception & ex) {
      params.add("Exception", ex.getMessageValue());
      m_processManager.logContext().log(log::ERR, "In MaintenanceHandler::kill(): failed to kill existing subprocess");
    }
  } else {
    m_processManager.logContext().log(log::INFO, "In MaintenanceHandler::kill(): no subprocess to kill");
  }
}

//------------------------------------------------------------------------------
// MaintenanceHandler::processEvent
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::processEvent() {
  // We do not expect any feedback for the child process...
  m_processManager.logContext().log(log::WARNING, "In MaintenanceHandler::processEvent(): spurious event");
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// MaintenanceHandler::processSigChild
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::processSigChild() {
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
  } catch (exception::Exception &ex) {
    cta::log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("pid", m_pid)
          .add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::WARNING,
        "In MaintenanceHandler::processSigChild(): failed to get child process exit code. Doing nothing as we are unable to determine if it is still running or not.");
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
        m_processManager.logContext().log(log::INFO, "Maintenance subprocess exited. Will spawn a new one.");
        m_processingStatus.forkRequested=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      } else {
        m_processManager.logContext().log(log::INFO, "Maintenance subprocess exited. Will not spawn new one as we are shutting down.");
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
        m_processManager.logContext().log(log::INFO, "Maintenance subprocess crashed. Will spawn a new one.");
        m_processingStatus.forkRequested=true;
        m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
      } else {
        m_processManager.logContext().log(log::INFO, "Maintenance subprocess crashed. Will not spawn new one as we are shutting down.");
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
// MaintenanceHandler::processTimeout
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::processTimeout() {
  // The only time we expect a timeout is when shutting down
  if (!m_shutdownInProgress) {
    m_processManager.logContext().log(log::WARNING, "In MaintenanceHandler::processTimeout(): spurious timeout: no shutdown");
    return m_processingStatus;
  }
  // We were shutting down and the child process did not exit in time. Killing it.
  if (-1!=m_pid) {
    m_processManager.logContext().log(log::WARNING,
        "In MaintenanceHandler::processTimeout(): spurious timeout: no more process");
  } else {
    // We will help the exit of the child process by killing it.
    m_processManager.logContext().log(log::WARNING, "In MaintenanceHandler::processTimeout(): will kill subprocess");
    kill();
  }
  // In all cases, the shutdown is complete.
  m_processingStatus.nextTimeout=m_processingStatus.nextTimeout.max();
  m_processingStatus.shutdownComplete=true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// MaintenanceHandler::runChild
//------------------------------------------------------------------------------
int MaintenanceHandler::runChild() noexcept {
  try{
    exceptionThrowingRunChild();
    return EXIT_SUCCESS;
  } catch (...){
    return EXIT_FAILURE;
  }
}

void MaintenanceHandler::exceptionThrowingRunChild(){
  // We are in the child process. It is time to open connections to the catalogue
  // and object store, and run the garbage collector.
  // We do not have to care for previous crashed sessions as we will garbage
  // collect them like any other crashed agent.

  // Set the thread name for process ID:
  prctl(PR_SET_NAME, "cta-tpd-maint");

  // Before anything, we will check for access to the scheduler's central storage.
  // If we fail to access it, we cannot work. We expect the drive processes to
  // fail likewise, so we just wait for shutdown signal (no feedback to main process).
  SchedulerDBInit_t sched_db_init("Maintenance", m_tapedConfig.backendPath.value(), m_processManager.logContext().logger());

  std::unique_ptr<cta::SchedulerDB_t> sched_db;
  std::unique_ptr<cta::catalogue::Catalogue> catalogue;
  std::unique_ptr<cta::Scheduler> scheduler;
  try {
    const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig.fileCatalogConfigFile.value());
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_processManager.logContext().logger(),
      catalogueLogin, nbConns, nbArchiveFileListingConns);
    catalogue = catalogueFactory->create();
    sched_db = sched_db_init.getSchedDB(*catalogue, m_processManager.logContext().logger());
    // TODO: we have hardcoded the mount policy parameters here temporarily we will remove them once we know where to put them
    scheduler = std::make_unique<cta::Scheduler>(*catalogue, *sched_db, 5, 2*1000*1000);
    // Before launching the transfer session, we validate that the scheduler is reachable.
    scheduler->ping(m_processManager.logContext());
  } catch(cta::exception::Exception &ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("errorMessage", ex.getMessageValue());
    m_processManager.logContext().log(log::CRIT,
          "In MaintenanceHandler::exceptionThrowingRunChild(): contact central storage. Waiting for shutdown.");

    server::SocketPair::pollMap pollList;
    pollList["0"]=m_socketPair.get();
    // Wait forever (negative timeout) for something to come from parent process.
    while (true){
      try {
      server::SocketPair::poll(pollList, -1, server::SocketPair::Side::parent);
       m_processManager.logContext().log(log::INFO,
        "In MaintenanceHandler::exceptionThrowingRunChild(): Received shutdown message after failure to contact storage. Exiting.");
        throw ex;
      } catch(server::SocketPair::SignalInterrupt &){
        // Interrupted by signal. Keep waiting for something to come
        // from parent.
      }
    }
  }

  // Create the garbage collector and the disk reporter
  auto gc = sched_db_init.getGarbageCollector(*catalogue);
  auto cleanupRunner = sched_db_init.getQueueCleanupRunner(*catalogue, *sched_db);
  DiskReportRunner diskReportRunner(*scheduler);
  RepackRequestManager repackRequestManager(*scheduler);

  if(!runRepackRequestManager()){
    m_processManager.logContext().log(log::INFO,
    "In MaintenanceHandler::exceptionThrowingRunChild(): Repack management is disabled. No repack-related operations will run on this tapeserver.");
  }

  // Run the maintenance in a loop: queue cleanup, garbage collector and disk reporter
  try {
    server::SocketPair::pollMap pollList;
    pollList["0"]=m_socketPair.get();
    bool receivedMessage=false;
    do {
      utils::Timer t;
      m_processManager.logContext().log(log::DEBUG,
          "In MaintenanceHandler::exceptionThrowingRunChild(): About to do a maintenance pass.");
      cleanupRunner.runOnePass(m_processManager.logContext());
      gc.runOnePass(m_processManager.logContext());
      diskReportRunner.runOnePass(m_processManager.logContext());
      if(runRepackRequestManager()){
        repackRequestManager.runOnePass(m_processManager.logContext(), m_tapedConfig.repackMaxRequestsToExpand.value());
      }
      try {
        server::SocketPair::poll(pollList, s_pollInterval - static_cast<long>(t.secs()), server::SocketPair::Side::parent);
       receivedMessage=true;
      } catch (server::SocketPair::Timeout &) {
        // Timing out while waiting for message is not a problem for us
        // as we retry in the next loop iteration.
      } catch (server::SocketPair::SignalInterrupt &) {
        // Received a singal while waiting in poll().
      }
    } while (!receivedMessage);
    m_processManager.logContext().log(log::INFO,
        "In MaintenanceHandler::exceptionThrowingRunChild(): Received shutdown message. Exiting.");
  } catch(cta::exception::Exception & ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("Message", ex.getMessageValue());
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingRunChild(): received an exception. Backtrace follows.");

    m_processManager.logContext().logBacktrace(log::INFO, ex.backtrace());
    throw ex;
  } catch(std::exception &ex) {
    log::ScopedParamContainer exParams(m_processManager.logContext());
    exParams.add("Message", ex.what());
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingRunChild(): received a std::exception.");
    throw ex;
  } catch(...) {
    m_processManager.logContext().log(log::ERR,
        "In MaintenanceHandler::exceptionThrowingRunChild(): received an unknown exception.");
    throw;
  }
}

//------------------------------------------------------------------------------
// MaintenanceHandler::shutdown
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus MaintenanceHandler::shutdown() {
  // We will signal the shutdown to the child process by sending a byte over the
  // socket pair (if we have one)
  m_shutdownInProgress=true;
  if (!m_socketPair.get()) {
    m_processManager.logContext().log(log::WARNING, "In MaintenanceHandler::shutdown(): no socket pair");
  } else {
    m_processManager.logContext().log(log::INFO, "In MaintenanceHandler::shutdown(): sent shutdown message to child process");
    m_socketPair->send("\0");
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// MaintenanceHandler::~MaintenanceHandler
//------------------------------------------------------------------------------
MaintenanceHandler::~MaintenanceHandler() {
  // If we still have a child process (should not), just stop it the hard way.
  if (-1 != m_pid) {
    cta::log::ScopedParamContainer params(m_processManager.logContext());
    params.add("pid", m_pid);
    ::kill(m_pid, SIGKILL);
    m_processManager.logContext().log(log::WARNING, "In MaintenanceHandler::~MaintenanceHandler(): killed leftover subprocess");
  }
}

//------------------------------------------------------------------------------
// MaintenanceHandler::runRepackRequestManager
//------------------------------------------------------------------------------
bool MaintenanceHandler::runRepackRequestManager() const {
  return m_tapedConfig.useRepackManagement.value() == "yes";
}

} // namespace cta::tape::daemon
