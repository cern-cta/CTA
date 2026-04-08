/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalHandler.hpp"

#include "ProcessManager.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"

#include <list>
#include <signal.h>
#include <sys/signalfd.h>
#include <unistd.h>

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// SignalHandler::SignalHandler
//------------------------------------------------------------------------------
SignalHandler::SignalHandler(ProcessManager& pm) : SubprocessHandler("signalHandler"), m_processManager(pm) {
  // Block the signals we want to handle.
  ::sigset_t sigMask;
  ::sigemptyset(&sigMask);
  std::list<int> sigLis = {SIGHUP,
                           SIGINT,
                           SIGQUIT,
                           SIGPIPE,
                           SIGTERM,
                           SIGUSR1,
                           SIGUSR2,
                           SIGCHLD,
                           SIGTSTP,
                           SIGTTIN,
                           SIGTTOU,
                           SIGPOLL,
                           SIGURG,
                           SIGVTALRM};
  for (auto sig : sigLis) {
    ::sigaddset(&sigMask, sig);
  }
  cta::exception::Errnum::throwOnNonZero(::sigprocmask(SIG_BLOCK, &sigMask, nullptr),
                                         "In SignalHandler::SignalHandler(): sigprocmask() failed");
  // Create the signalfd. We will poll so we should never read uselessly => NONBLOCK will prevent
  // being blocked in case of issue.
  m_sigFd = ::signalfd(-1, &sigMask, SFD_NONBLOCK);
  cta::exception::Errnum::throwOnMinusOne(m_sigFd, "In SignalHandler::SignalHandler(): signalfd() failed");
  // We can already register the file descriptor
  m_processManager.addFile(m_sigFd, this);
  // Set shutdownComplete because SignalHandler is always ready to leave at all times
  m_processingStatus.shutdownComplete = true;
}

//------------------------------------------------------------------------------
// SignalHandler::~SignalHandler
//------------------------------------------------------------------------------
SignalHandler::~SignalHandler() {
  // If we still have a signal handler (this can NOT be the case in a child process),
  if (m_sigFd != -1) {
    // Deregister the file descriptor from poll
    m_processManager.removeFile(m_sigFd);
    // And close the file
    ::close(m_sigFd);
  }
}

//------------------------------------------------------------------------------
// SignalHandler::getInitialStatus
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::getInitialStatus() {
  // On initiation, we expect nothing but signals, i.e. default status
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::processEvent
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::processEvent() {
  // We have a signal
  struct ::signalfd_siginfo sigInf;
  int rc = ::read(m_sigFd, &sigInf, sizeof(sigInf));
  // We should always get something here. As we set the NONBLOCK option, lack
  // of signal here will lead to EAGAIN, which is also an error.
  cta::exception::Errnum::throwOnMinusOne(rc);
  // We should get exactly sizeof(sigInf) bytes.
  if (rc != sizeof(sigInf)) {
    std::stringstream err;
    err << "In SignalHandler::processEvent(): unexpected size for read: "
           "got="
        << rc << " expected=" << sizeof(sigInf);
    throw cta::exception::Exception(err.str());
  }
  // Prepare logging the signal
  cta::log::ScopedParamContainer params(m_processManager.logContext());
  if (char* signalName = ::strsignal(sigInf.ssi_signo)) {
    params.add("signal", signalName);
  } else {
    params.add("signal", sigInf.ssi_signo);
  }
  params.add("senderPID", sigInf.ssi_pid).add("senderUID", sigInf.ssi_uid);
  // Handle the signal
  switch (sigInf.ssi_signo) {
    case SIGHUP:
    case SIGQUIT:
    case SIGPIPE:
    case SIGUSR2:
    case SIGTSTP:
    case SIGTTIN:
    case SIGTTOU:
    case SIGPOLL:
    case SIGURG:
    case SIGVTALRM: {
      m_processManager.logContext().log(log::INFO, "In signal handler, ignoring signal");
      break;
    }
    case SIGINT:
    case SIGTERM:
      // We will now require shutdown (if not already done)
      // record the time to define timeout. After the timeout expires, we will require kill.
      if (!m_shutdownRequested) {
        m_shutdownRequested = true;
        m_shutdownStartTime = std::chrono::steady_clock::now();
        m_processManager.logContext().log(log::INFO, "In signal handler, initiating shutdown");
      } else {
        m_processManager.logContext().log(log::INFO, "In signal handler, shutdown already initiated: ignoring");
      }
      break;
    case SIGCHLD:
      // We will request the processing of sigchild until it is acknowledged (by receiving
      // it ourselves)
      m_sigChildPending = true;
      m_processManager.logContext().log(log::INFO,
                                        "In signal handler, received SIGCHLD and propagations to other handlers");
      break;
    case SIGUSR1:
      // A logger refresh (i.e. log rotation) is needed.
      // We need to broadcast this information to all other subprocesses.
      // Flag will be cleared when broadcast is read.
      m_refreshLoggerRequested = true;
      m_processManager.logContext().log(log::INFO, "In signal handler, received SIGUSR1 to request a logger refresh");
      break;
  }
  // If the shutdown was not acknowledged (by receiving it ourselves), we ask
  // for it
  m_processingStatus.shutdownRequested = m_shutdownRequested && !m_shutdownAcknowlegded;
  // Compute the timeout if shutdown was requested. Else, it is end of times.
  if (m_shutdownRequested) {
    m_processingStatus.nextTimeout = m_shutdownStartTime + m_timeoutDuration;
  } else {
    m_processingStatus.nextTimeout = decltype(m_processingStatus.nextTimeout)::max();
  }
  m_processingStatus.sigChild = m_sigChildPending;
  m_processingStatus.refreshLoggerRequested = m_refreshLoggerRequested;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::kill
//------------------------------------------------------------------------------
void SignalHandler::kill() {
  // We have nothing to kill: noop.
}

//------------------------------------------------------------------------------
// SignalHandler::postForkCleanup
//------------------------------------------------------------------------------
void SignalHandler::postForkCleanup() {
  // We should make sure the signalFD will not be altered in the child process.
  // We do not deregister it from poll
  ::close(m_sigFd);
  m_sigFd = -1;
}

//------------------------------------------------------------------------------
// SignalHandler::processTimeout
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::processTimeout() {
  // If we reach timeout, it means it's time to kill child processes
  m_processManager.logContext().log(log::INFO,
                                    "In signal handler, initiating subprocess kill after timeout on shutdown");
  m_processingStatus.killRequested = true;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::processSigChild
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::processSigChild() {
  // Our sigchild is now acknowledged
  m_sigChildPending = false;
  if (m_shutdownRequested) {
    m_processingStatus.nextTimeout = m_shutdownStartTime + m_timeoutDuration;
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::processRefreshLoggerRequest
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::processRefreshLoggerRequest() {
  auto optionalMsg = std::optional<std::string>();
  if (m_refreshLoggerRequested) {
    m_refreshLoggerRequested = false;
  }
  m_processingStatus.refreshLoggerRequested = false;
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::refreshLogger
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::refreshLogger() {
  m_processManager.logContext().logger().refresh();
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::shutdown
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::shutdown() {
  // We received (back) our own shutdown: consider it acknowledged
  m_shutdownAcknowlegded = true;
  // if we ever asked for shutdown, we have a timeout
  if (m_shutdownRequested) {
    m_processingStatus.nextTimeout = m_shutdownStartTime + m_timeoutDuration;
  }
  return m_processingStatus;
}

//------------------------------------------------------------------------------
// SignalHandler::fork
//------------------------------------------------------------------------------
SubprocessHandler::ProcessingStatus SignalHandler::fork() {
  throw cta::exception::Exception("Unexpected call to SignalHandler::fork()");
}

//------------------------------------------------------------------------------
// SignalHandler::runChild
//------------------------------------------------------------------------------
int SignalHandler::runChild() {
  throw cta::exception::Exception("Unexpected call to SignalHandler::runChild()");
}

}  // namespace cta::tape::daemon
