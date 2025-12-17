/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ChildProcess.hpp"

#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

namespace cta::threading {

void ChildProcess::start(Cleanup& cleanup) {
  m_pid = fork();
  if (!m_pid) {
    /* We are the child process. Do our stuff and exit. */
    cleanup();
    exit(run());
  } else if (-1 == m_pid) {
    /* We are in the parent process, for failed */
    throw cta::exception::Errnum("Failed to fork a child process in cta::threading::ChildProcess::ChildProcess()");
  }
  /* In parent process, child is OK. */
  m_started = true;
}

void ChildProcess::parseStatus(int status) {
  if (WIFEXITED(status)) {
    m_finished = true;
    m_exited = true;
    m_exitCode = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    m_finished = true;
    m_wasKilled = true;
  }
}

bool ChildProcess::running() {
  /* Checking for a running process before starting gets an exception */
  if (!m_started) {
    throw ProcessNeverStarted();
  }
  /* If we are not aware of process exiting, let's check and collect exit code */
  if (!m_finished) {
    /* Re-check the status now. */
    int status, ret;
    cta::exception::Errnum::throwOnMinusOne(ret = waitpid(m_pid, &status, WNOHANG),
                                            "Error from waitpid in cta::threading::ChildProcess::running()");
    if (ret == m_pid) {
      parseStatus(status);
    }
  }
  return !m_finished;
}

void ChildProcess::wait() {
  /* Checking for a running process before starting gets an exception */
  if (!m_started) {
    throw ProcessNeverStarted();
  }
  if (m_finished) {
    return;
  }
  int status, ret;
  cta::exception::Errnum::throwOnMinusOne(ret = waitpid(m_pid, &status, 0),
                                          "Error from waitpid in cta::threading::ChildProcess::wait()");
  /* Check child status*/
  if (ret == m_pid) {
    parseStatus(status);
  }
  if (!m_finished) {
    throw cta::exception::Exception("Process did not exit after waitpid().");
  }
}

int ChildProcess::exitCode() {
  if (!m_started) {
    throw ProcessNeverStarted();
  }
  if (!m_finished) {
    int status, ret;
    cta::exception::Errnum::throwOnMinusOne(ret = waitpid(m_pid, &status, WNOHANG),
                                            "Error from waitpid in cta::threading::ChildProcess::running()");
    if (ret == m_pid) {
      parseStatus(status);
    }
  }
  /* Check child status*/
  if (!m_finished) {
    throw ProcessStillRunning();
  }
  if (!m_exited) {
    throw ProcessWasKilled();
  }
  return m_exitCode;
}

void ChildProcess::kill() {
  if (!m_started) {
    throw ProcessNeverStarted();
  }
  ::kill(m_pid, SIGTERM);
}

}  // namespace cta::threading
