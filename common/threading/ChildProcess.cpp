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

#include "ChildProcess.hpp"
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>

namespace cta {
namespace threading {

void ChildProcess::start(Cleanup& cleanup) {
  m_pid = fork();
  if (!m_pid) {
    /* We are the child process. Do our stuff and exit. */
    cleanup();
    exit(run());
  }
  else if (-1 == m_pid) {
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
  }
  else if (WIFSIGNALED(status)) {
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

}  // namespace threading
}  // namespace cta