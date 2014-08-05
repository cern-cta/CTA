/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "ChildProcess.hpp"
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>

void castor::server::ChildProcess::start(Cleanup & cleanup)  {
  m_pid = fork();
  if (!m_pid) {
    /* We are the child process. Do our stuff and exit. */
    cleanup();
    exit(run());
  } else if (-1 == m_pid) {
    /* We are in the parent process, for failed */
    throw castor::exception::Errnum("Failed to fork a child process in castor::server::ChildProcess::ChildProcess()");
  }
  /* In parent process, child is OK. */
  m_started = true;
}

void castor::server::ChildProcess::parseStatus(int status) {
  if (WIFEXITED(status)) {
     m_finished = true;
     m_exited = true;
     m_exitCode = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    m_finished = true;
    m_wasKilled = true;
  }
}

bool castor::server::ChildProcess::running()  {
  /* Checking for a running process before starting gets an exception */
  if (!m_started) throw ProcessNeverStarted();
  /* If we are not aware of process exiting, let's check and collect exit code */
  if (!m_finished) {
    /* Re-check the status now. */
    int status, ret;
    castor::exception::Errnum::throwOnMinusOne(
        ret = waitpid(m_pid, &status, WNOHANG),
        "Error from waitpid in castor::server::ChildProcess::running()");
    if (ret == m_pid) parseStatus(status);
  }
  return !m_finished;
}

void castor::server::ChildProcess::wait()  {
  /* Checking for a running process before starting gets an exception */
  if (!m_started) throw ProcessNeverStarted();
  if (m_finished) return;
  int status, ret;
  castor::exception::Errnum::throwOnMinusOne(
      ret = waitpid(m_pid, &status, 0),
      "Error from waitpid in castor::server::ChildProcess::wait()");
  /* Check child status*/
  if (ret == m_pid) parseStatus(status);
  if(!m_finished)
    throw castor::tape::Exception("Process did not exit after waitpid().");
}

int castor::server::ChildProcess::exitCode()  {
  if (!m_started) throw ProcessNeverStarted();
  if (!m_finished) {
    int status, ret;
    castor::exception::Errnum::throwOnMinusOne(
        ret = waitpid(m_pid, &status, WNOHANG),
        "Error from waitpid in castor::server::ChildProcess::running()");
    if (ret == m_pid) parseStatus(status);
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

void castor::server::ChildProcess::kill()  {
  if (!m_started) throw ProcessNeverStarted();
  ::kill(m_pid, SIGTERM);
}
