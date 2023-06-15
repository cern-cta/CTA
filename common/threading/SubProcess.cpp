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

#include "SubProcess.hpp"
#include "common/exception/Errnum.hpp"
#include <memory>
#include <string.h>
#include <iostream>
#include <sys/signal.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <spawn.h>

namespace {
class ScopedPosixSpawnFileActions {
public:
  ScopedPosixSpawnFileActions() { ::posix_spawn_file_actions_init(&m_action); }

  ~ScopedPosixSpawnFileActions() { ::posix_spawn_file_actions_destroy(&m_action); }

  operator ::posix_spawn_file_actions_t*() { return &m_action; }

private:
  ::posix_spawn_file_actions_t m_action;
};
}  // namespace

namespace {
class ScopedPosixSpawnAttr {
public:
  ScopedPosixSpawnAttr() { ::posix_spawnattr_init(&m_attr); }

  ~ScopedPosixSpawnAttr() { ::posix_spawnattr_destroy(&m_attr); }

  operator ::posix_spawnattr_t*() { return &m_attr; }

private:
  ::posix_spawnattr_t m_attr;
};
}  // namespace

namespace cta {
namespace threading {

SubProcess::SubProcess(const std::string& executable,
                       const std::list<std::string>& argv,
                       const std::string& stdinInput) :
m_childComplete(false),
m_childStatus(0) {
  // Sanity checks
  if (argv.size() < 1) {
    throw cta::exception::Exception("In Subprocess::Subprocess: not enough elements in argv");
  }
  // Prepare the pipes for the child's stdout and stderr (stdin will be closed)
  const size_t readSide = 0;
  const size_t writeSide = 1;
  int stdoutPipe[2];
  int stderrPipe[2];
  int stdinPipe[2];
  cta::exception::Errnum::throwOnNonZero(::pipe2(stdoutPipe, O_NONBLOCK),
                                         "In Subprocess::Subprocess failed to create the stdout pipe");
  cta::exception::Errnum::throwOnNonZero(::pipe2(stderrPipe, O_NONBLOCK),
                                         "In Subprocess::Subprocess failed to create the stderr pipe");
  cta::exception::Errnum::throwOnNonZero(::pipe2(stdinPipe, O_NONBLOCK),
                                         "In Subprocess::Subprocess failed to create the stdin pipe");
  // Prepare the actions to be taken on file descriptors
  ScopedPosixSpawnFileActions fileActions;
  // We will be the child process. Close the read sides of the pipes.
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stdoutPipe[readSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (1)");
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stderrPipe[readSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (2)");
  // We close the write side of the stdinPipe: the child does not write in it
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stdinPipe[writeSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (3)");
  //Rewire the stdout and stderr to the pipes.
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_adddup2(fileActions, stdoutPipe[writeSide], STDOUT_FILENO),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_adddup2() (1)");
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_adddup2(fileActions, stderrPipe[writeSide], STDERR_FILENO),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_adddup2() (2)");
  //Rewiring the read side of the stdin pipe to stdin
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_adddup2(fileActions, stdinPipe[readSide], STDIN_FILENO),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_adddup2() (3)");
  // Close the now duplicated pipe file descriptors
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stdoutPipe[writeSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (4)");
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stderrPipe[writeSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (5)");
  cta::exception::Errnum::throwOnReturnedErrno(
    posix_spawn_file_actions_addclose(fileActions, stdinPipe[readSide]),
    "In Subprocess::Subprocess(): failed to posix_spawn_file_actions_addclose() (6)");

  // And finally spawn the subprocess
  // Prepare the spawn attributes (we need vfork)
  ScopedPosixSpawnAttr attr;
  cta::exception::Errnum::throwOnReturnedErrno(posix_spawnattr_setflags(attr, POSIX_SPAWN_USEVFORK),
                                               "In Subprocess::Subprocess(): failed to posix_spawnattr_setflags()");
  {
    std::unique_ptr<char*[]> cargv(new char*[argv.size() + 1]);
    size_t index = 0;
    std::list<std::unique_ptr<char, void (*)(char*)>> cargvStrings;
    for (auto a = argv.cbegin(); a != argv.cend(); a++) {
      cargv[index] = ::strdup(a->c_str());
      std::unique_ptr<char, void (*)(char*)> upStr(cargv[index], [](char* s) { ::free(s); });
      cargvStrings.emplace_back(std::move(upStr));
      index++;
    }
    cargv[argv.size()] = nullptr;
    int spawnRc = ::posix_spawnp(&m_child, executable.c_str(), fileActions, attr, cargv.get(), ::environ);
    cta::exception::Errnum::throwOnReturnedErrno(spawnRc, "In Subprocess::Subprocess failed to posix_spawn()");
  }
  // We are the parent process.
  // close the readSide of stdin pipe as we are going to write to the subprocess stdin
  //Send input to child stdin
  //Write data to child stdin
  ::write(stdinPipe[writeSide], stdinInput.c_str(), stdinInput.size());
  //Close stdin
  ::close(stdinPipe[writeSide]);
  ::close(stdinPipe[readSide]);
  //Close the write sides of pipes.
  ::close(stdoutPipe[writeSide]);
  ::close(stderrPipe[writeSide]);
  m_stdoutFd = stdoutPipe[readSide];
  m_stderrFd = stderrPipe[readSide];
}

void SubProcess::kill(int signal) {
  ::kill(m_child, signal);
}

int SubProcess::exitValue() {
  if (!m_childComplete) {
    throw cta::exception::Exception("In Subprocess::exitValue: child process not waited for");
  }
  return WEXITSTATUS(m_childStatus);
}

bool SubProcess::wasKilled() {
  if (!m_childComplete) {
    throw cta::exception::Exception("In Subprocess::wasKilled: child process not waited for");
  }
  return WIFSIGNALED(m_childStatus);
}

int SubProcess::killSignal() {
  if (!m_childComplete) {
    throw cta::exception::Exception("In Subprocess::killSignal: child process not waited for");
  }
  return WTERMSIG(m_childStatus);
}

void SubProcess::wait() {
  ::waitpid(m_child, &m_childStatus, 0);
  char buff[1000];
  int rc;
  while (0 < (rc = ::read(m_stdoutFd, buff, sizeof(buff)))) {
    m_stdout.append(buff, rc);
  }
  ::close(m_stdoutFd);
  while (0 < (rc = ::read(m_stderrFd, buff, sizeof(buff)))) {
    m_stderr.append(buff, rc);
  }
  ::close(m_stderrFd);
  m_childComplete = true;
}

std::string SubProcess::stdout() {
  if (!m_childComplete) {
    throw cta::exception::Exception("In Subprocess::stdout: child process not waited for");
  }
  return m_stdout;
}

std::string SubProcess::stderr() {
  if (!m_childComplete) {
    throw cta::exception::Exception("In Subprocess::stderr: child process not waited for");
  }
  return m_stderr;
}

SubProcess::~SubProcess() {
  if (!m_childComplete) {
    this->kill(SIGKILL);
    this->wait();
  }
}

}  // namespace threading
}  // namespace cta
