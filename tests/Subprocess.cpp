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

#include "Subprocess.hpp"
#include "common/exception/Errnum.hpp"
#include <string.h>
#include <iostream>
#include <sys/signal.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <spawn.h>
#include <memory>

namespace {
class ScopedPosixSpawnFileActions{
public:
  ScopedPosixSpawnFileActions() {
    ::posix_spawn_file_actions_init(&m_action);
  }
  ~ScopedPosixSpawnFileActions() {
    ::posix_spawn_file_actions_destroy(&m_action);
  }
  operator ::posix_spawn_file_actions_t * () { return &m_action; }
private:
  ::posix_spawn_file_actions_t m_action;
};
}

namespace {
class ScopedPosixSpawnAttr{
public:
  ScopedPosixSpawnAttr() {
    ::posix_spawnattr_init(&m_attr);
  }
  ~ScopedPosixSpawnAttr() {
    ::posix_spawnattr_destroy(&m_attr);
  }
  operator ::posix_spawnattr_t * () { return &m_attr; }
private:
  ::posix_spawnattr_t m_attr;
};
}

namespace systemTests {
Subprocess::Subprocess(const std::string & executable, const std::list<std::string>& argv) {
  // Sanity checks
  if (argv.size() < 1)
    throw cta::exception::Exception(
        "In Subprocess::Subprocess: not enough elements in argv");
  // Prepare the pipes for the child's stdout and stderr (stdin will be closed)
  const size_t readSide=0;
  const size_t writeSide=1;
  int stdoutPipe[2];
  int stderrPipe[2];
  cta::exception::Errnum::throwOnNonZero(::pipe2(stdoutPipe, O_NONBLOCK), 
      "In Subprocess::Subprocess failed to create the stdout pipe");
  cta::exception::Errnum::throwOnNonZero(::pipe2(stderrPipe, O_NONBLOCK), 
      "In Subprocess::Subprocess failed to create the stderr pipe");
  // Prepare the actions to be taken on file descriptors
  ScopedPosixSpawnFileActions fileActions;
  // We will be the child process. Close the read sides of the pipes.
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_addclose(fileActions, stdoutPipe[readSide]),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (1)");
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_addclose(fileActions, stderrPipe[readSide]),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (2)");
  // Close stdin and rewire the stdout and stderr to the pipes.
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_adddup2(fileActions, stdoutPipe[writeSide], STDOUT_FILENO),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_adddup2() (1)");
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_adddup2(fileActions, stderrPipe[writeSide], STDERR_FILENO),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_adddup2() (2)");
  // Close the now duplicated pipe file descriptors
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_addclose(fileActions, stdoutPipe[writeSide]),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (3)");
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawn_file_actions_addclose(fileActions, stderrPipe[writeSide]),
      "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (4)");
  // And finally spawn the subprocess
  // Prepare the spawn attributes (we need vfork)
  ScopedPosixSpawnAttr attr;
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawnattr_setflags(attr, POSIX_SPAWN_USEVFORK),
      "In Subprocess::Subprocess(): failed to posix_spawnattr_setflags()");
  char ** cargv = new char*[argv.size()+1];
  int index = 0;
  for (auto a=argv.cbegin(); a!=argv.cend(); a++) {
    cargv[index++] = ::strdup(a->c_str());
  }
  cargv[argv.size()] = NULL;
  int spawnRc=::posix_spawnp(&m_child, executable.c_str(), fileActions, attr, cargv, ::environ);
  cta::exception::Errnum::throwOnReturnedErrno(spawnRc, "In Subprocess::Subprocess failed to posix_spawn()");
  // We are the parent process. Close the write sides of pipes.
  ::close(stdoutPipe[writeSide]);
  ::close(stderrPipe[writeSide]);
  m_stdoutFd = stdoutPipe[readSide];
  m_stderrFd = stderrPipe[readSide];
}

void Subprocess::kill(int signal) {
  ::kill(m_child, signal);
}

int Subprocess::exitValue() {
  if(!m_child)
    throw cta::exception::Exception("In Subprocess::exitValue: child process not waited for");
  return WEXITSTATUS(m_childStatus);
}

void Subprocess::wait() {
  ::waitpid(m_child, &m_childStatus, 0);
  char buff[1000];
  int rc;
  while (0<(rc=::read(m_stdoutFd, buff, sizeof(buff)))) {
    m_stdout.append(buff, rc);
  }
  ::close(m_stdoutFd);
  while (0<(rc=::read(m_stderrFd, buff, sizeof(buff)))) {
    m_stderr.append(buff, rc);
  }
  ::close(m_stderrFd);
  m_childComplete = true;
}

std::string Subprocess::stdout() {
  if(!m_child)
    throw cta::exception::Exception("In Subprocess::stdout: child process not waited for");
  return m_stdout;
}

std::string Subprocess::stderr() {
  if(!m_child)
    throw cta::exception::Exception("In Subprocess::stderr: child process not waited for");
  return m_stderr;
}

Subprocess::~Subprocess() {
  if(!m_childComplete) {
    this->kill(SIGKILL);
    this->wait();
  }
}


} // namespace systemTests