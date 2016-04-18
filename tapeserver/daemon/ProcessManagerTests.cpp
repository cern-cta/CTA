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

#include <gtest/gtest.h>

#include "ProcessManager.hpp"
#include "SubprocessHandler.hpp"
#include "common/threading/SocketPair.hpp"
#include "common/exception/Errnum.hpp"
#include <chrono>
#include <string>

namespace unitTests {

/** A simple subprocess that sends a struct over a socketpair and waits for
 *  a reply. Once the reply has come, the child process exits and we are done */
class EchoSubprocess: public cta::tape::daemon::SubprocessHandler {
public:
  /// Constructor
  EchoSubprocess(const std::string & name, cta::tape::daemon::ProcessManager & pm): 
    cta::tape::daemon::SubprocessHandler(name), m_processManager(pm) {}
    
  /// Get status: initial status: we want to fork the subprocess
  SubprocessHandler::ProcessingStatus getInitialStatus() override {
    SubprocessHandler::ProcessingStatus ret;
    ret.forkRequested = true;
    return ret;
  }

  SubprocessHandler::ProcessingStatus fork() override {
    SubprocessHandler::ProcessingStatus ret;
    m_childProcess = ::fork();
    cta::exception::Errnum::throwOnMinusOne(m_childProcess, 
        "In EchoSubprocess::fork(): failed to fork(): ");
    if (!m_childProcess) { // We are on the child side
      // We can close the parent's socket side.
      m_socketPair.close(cta::server::SocketPair::Side::parent);
      ret.forkState = SubprocessHandler::ForkState::child;
      return ret;
    }
    // Parent side, close child socket side
    m_socketPair.close(cta::server::SocketPair::Side::child);
    // We record the subprocess start time
    m_subprocessLaunchTime = std::chrono::steady_clock::now();
    // send the echo request
    EchoRequestRepy echo;
    echo.counter = 666;
    std::string echoString;
    echoString.append((char*)&echo, sizeof(echo));
    m_socketPair.send(echoString);
    // Register the file descriptor.
    m_processManager.addFile(m_socketPair.getFdForAccess(cta::server::SocketPair::Side::child), this);
    ret.nextTimeout = m_subprocessLaunchTime + m_timeoutLength;
    ret.forkState = SubprocessHandler::ForkState::parent;
    return ret;
  }
  
  void prepareForFork() override {
    // Nothing to do before fork.
  }
  
  int runChild() override {
    // Just wait forever for an echo request
    EchoRequestRepy echo;
    memset(&echo, '\0', sizeof(echo));
    cta::server::SocketPair::pollMap pm;
    pm["0"] = &m_socketPair;
    cta::server::SocketPair::poll(pm, 1);
    if (!m_socketPair.pollFlag()) {
      throw cta::exception::Exception("In EchoProcess::runChild(): failed to receive parent's data after 1 second");
    }
    std::string echoString = m_socketPair.receive();
    echoString.copy((char*)&echo, sizeof(echo));
    if (echo.magic != 0xdeadbeef) return EXIT_FAILURE;
    echo.counter++;
    echoString.clear();
    echoString.append((char*)&echo, sizeof(echo));
    m_socketPair.send(echoString);
    return EXIT_SUCCESS;
  }

  void kill() override {
    // Send kill signal to child.
    ::kill(m_childProcess, SIGKILL);
  }

  SubprocessHandler::ProcessingStatus processEvent() override {
    // We expect to read from the child through the socket pair.
    auto echoString = m_socketPair.receive();
    EchoRequestRepy echo;
    auto size=echoString.copy((char*)&echo, sizeof(echo));
    if (size!=sizeof(echo)) {
      std::stringstream err;
      err << "In EchoSubprocess::processEvent(): unexpected size for echo: "
          "expected: " << sizeof(echo) << " received: " << size;
      throw cta::exception::Exception(err.str());
    }
    if (echo.magic != 0xdeadbeef) {
      std::stringstream err;
      err << "In EchoSubprocess::processEvent(): unexpected magic number: "
          "expected: " << std::hex << 0xdeadbeef << " received: " << echo.magic;
      throw cta::exception::Exception(err.str());
    }
    if (echo.counter != 667) {
      std::stringstream err;
      err << "In EchoSubprocess::processEvent(): unexpected counter: "
          "expected: " << 667 << " received: " << echo.counter;
      throw cta::exception::Exception(err.str());
    }
    SubprocessHandler::ProcessingStatus ret;
    ::waitpid(m_childProcess, nullptr, 0);
    m_processManager.removeFile(m_socketPair.getFdForAccess(cta::server::SocketPair::Side::child));
    ret.shutdownComplete = true;
    return ret;
  }
  
  SubprocessHandler::ProcessingStatus processTimeout() override {
    throw cta::exception::Exception("In EchoSubprocess::processTimeout(): timeout!");
  }
  
  void shutdown() override {
    // Nothing to do as the sub process will exit on its own.
  }
  
private:
  typedef cta::tape::daemon::SubprocessHandler SubprocessHandler;
  cta::server::SocketPair m_socketPair;
  cta::tape::daemon::ProcessManager & m_processManager;
  bool m_subprocesLaunched=false;
  bool m_subprocessComplete=false;
  std::chrono::time_point<std::chrono::steady_clock> m_subprocessLaunchTime;
  const std::chrono::seconds m_timeoutLength = std::chrono::seconds(2);
  ::pid_t m_childProcess;
  struct EchoRequestRepy {
    uint32_t magic = 0xdeadbeef;
    uint32_t counter = 0;
  };
};

TEST(cta_Daemon, ProcessManager) {
  cta::tape::daemon::ProcessManager pm;
  std::unique_ptr<EchoSubprocess> es(new EchoSubprocess("Echo subprocess", pm));
  // downcast pointer
  std::unique_ptr<cta::tape::daemon::SubprocessHandler> sph = std::move(es);
  pm.addHandler(std::move(sph));
  pm.run();
}

} //namespace unitTests