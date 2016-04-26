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

#include "ProcessManager.hpp"
#include "common/exception/Errnum.hpp"
#include <sys/epoll.h>
#include <unistd.h>
#include <algorithm>

namespace cta {
namespace tape {
namespace daemon {

ProcessManager::ProcessManager() {
  m_epollFd = ::epoll_create1(0);
  cta::exception::Errnum::throwOnMinusOne(m_epollFd,
      "In ProcessManager::ProcessManager(), failed to create an epoll file descriptor: ");
}

ProcessManager::~ProcessManager() {
  // First, make sure we delete all handlers. We cannot rely on the implicit 
  // destructor of m_subprocessHandlers. The handlers will unregister themselves
  // from epoll at that point.
  m_subprocessHandlers.clear();
  // We can now close the epoll fd.
  ::close(m_epollFd);
}

void ProcessManager::addHandler(std::unique_ptr<SubprocessHandler>&& handler) {
  m_subprocessHandlers.push_back(SubprocessAndStatus());
  m_subprocessHandlers.back().handler = std::move(handler);
  m_subprocessHandlers.back().status = m_subprocessHandlers.back().handler->getInitialStatus();
}

void ProcessManager::addFile(int fd, SubprocessHandler* sh) {
  struct ::epoll_event ee;
  ee.events = EPOLLIN;
  ee.data.ptr = (void *) sh;
  cta::exception::Errnum::throwOnNonZero(::epoll_ctl(m_epollFd, EPOLL_CTL_ADD, fd, &ee),
      "In ProcessManager::addFile(), failed to ::epoll_ctl(EPOLL_CTL_ADD): ");
}

void ProcessManager::removeFile(int fd) {
  cta::exception::Errnum::throwOnNonZero(::epoll_ctl(m_epollFd, EPOLL_CTL_DEL, fd, nullptr),
      "In ProcessManager::removeFile(), failed to ::epoll_ctl(EPOLL_CTL_DEL): ");
}

// TODO: add logging. add names to subprocesses
int ProcessManager::run() {
  // The first statuses were updated at subprocess registration, so we do
  // not need an initialization.
  while(true) {
    // Manage sigChild requests
    auto sigChildStatus = runSigChildManagement();
    if (sigChildStatus.doExit) return sigChildStatus.exitCode;
    // Manage shutdown requests and completions.
    auto shutdownStatus = runShutdownManagement();
    if (shutdownStatus.doExit) return shutdownStatus.exitCode;
    // Manage kill requests.
    auto killStatus = runKillManagement();
    if (killStatus.doExit) return killStatus.exitCode;
    // Manage fork requests
    auto forkStatus = runForkManagement();
    if (forkStatus.doExit) return forkStatus.exitCode;
    // All subprocesses requests have been handled. We can now switch to the 
    // event handling per se.
    runEventLoop();
  }
}

SubprocessHandler& ProcessManager::at(const std::string& name) {
  for (auto & sp: m_subprocessHandlers) {
    if (name == sp.handler.get()->index) {
      return *sp.handler.get();
    }
  }
  throw cta::exception::Exception("In ProcessManager::at(): entry not found");
}

ProcessManager::RunPartStatus ProcessManager::runShutdownManagement() {
  // Check the current statuses for shutdown requests
  // If any process requests a shutdown, we will trigger it in all.
  bool anyAskedShutdown = std::count_if(m_subprocessHandlers.cbegin(), 
      m_subprocessHandlers.cend(), 
      [](const SubprocessAndStatus &i){return i.status.shutdownRequested;});
  if (anyAskedShutdown) {
    for(auto & sp: m_subprocessHandlers) { sp.status = sp.handler->shutdown(); }
  }
  // If all processes completed their shutdown, we can exit
  bool shutdownComplete=true;
  for (auto & sp: m_subprocessHandlers) { shutdownComplete &= sp.status.shutdownComplete; }
  if (shutdownComplete) {
    RunPartStatus ret;
    ret.doExit = true;
    ret.exitCode = EXIT_SUCCESS;
    return ret;
  }
  return RunPartStatus();
}

ProcessManager::RunPartStatus ProcessManager::runKillManagement() {
  // If any process asks for a kill, we kill all sub processes and exit
  bool anyAskedKill = std::count_if(m_subprocessHandlers.cbegin(), 
      m_subprocessHandlers.cend(), 
      [](const SubprocessAndStatus &i){return i.status.killRequested;});
  if (anyAskedKill) {
    for(auto & sp: m_subprocessHandlers) { sp.handler->kill(); }
    RunPartStatus ret;
    ret.doExit = true;
    ret.exitCode = EXIT_SUCCESS;
    return ret;
  }
  return RunPartStatus();
}

ProcessManager::RunPartStatus ProcessManager::runForkManagement() {
  // For each process requesting a fork, we do it
  // prepare all processes and handlers for the fork
  for(auto & sp: m_subprocessHandlers) {
    if(sp.status.forkRequested) {
      for (auto & sp2:m_subprocessHandlers) {
        sp2.handler->prepareForFork();
      }
      auto newStatus = sp.handler->fork();
      switch (newStatus.forkState) {
      case SubprocessHandler::ForkState::child:
        // TODO: cleanup other processes (needed?)
        // We are in the child side: run the subprocess and exit.
        ::exit(sp.handler->runChild());
        break;
      case SubprocessHandler::ForkState::parent:
        // We are parent side. Record the new state for this handler
        newStatus.forkState = SubprocessHandler::ForkState::notForking;
        sp.status = newStatus;
        break;
      case SubprocessHandler::ForkState::notForking:
        throw cta::exception::Exception("In ProcessManager::runForkManagement(): "
            "unexpected for state (notForking)");
        break;
      }
    }
  }
  return RunPartStatus();
}

ProcessManager::RunPartStatus ProcessManager::runSigChildManagement() {
  // If any process received sigChild, we signal it to all processes
  bool sigChild = std::count_if(m_subprocessHandlers.cbegin(), 
      m_subprocessHandlers.cend(), 
      [](const SubprocessAndStatus &i){return i.status.sigChild;});
  if  (sigChild) {
    for(auto & sp: m_subprocessHandlers) { sp.status = sp.handler->processSigChild(); }
  }
  // If all processes completed their shutdown, we can exit
  bool shutdownComplete=true;
  for (auto & sp: m_subprocessHandlers) { shutdownComplete &= sp.status.shutdownComplete; }
  if (shutdownComplete) {
    RunPartStatus ret;
    ret.doExit = true;
    ret.exitCode = EXIT_SUCCESS;
    return ret;
  }
  return RunPartStatus();
}


void ProcessManager::runEventLoop() {
  // Compute the next timeout. Epoll expects milliseconds.
  std::chrono::time_point<std::chrono::steady_clock> nextTimeout = decltype(nextTimeout)::max();
  for (auto & sp: m_subprocessHandlers) {
    // First, if the timeout is in the past, inform the handler (who will 
    // come with a new status)
    if (sp.status.nextTimeout < std::chrono::steady_clock::now()) {
      sp.status = sp.handler->processTimeout();
      // If the handler requested kill, shutdown or fork, we can go back to handlers, 
      // which means we exit from the loop here.
      if (sp.status.forkRequested || sp.status.killRequested || sp.status.shutdownRequested || sp.status.sigChild) return;
      // If new timeout is still in the past, we overlook it
      // TODO: log
      if (sp.status.nextTimeout < std::chrono::steady_clock::now()) {
        continue;
      }
    }
    // In all other cases (timeout updated or not) find the next timeout
    nextTimeout = std::min(sp.status.nextTimeout, nextTimeout);
  }
  // We now compute the next timeout. epoll needs milliseconds
  int64_t nextTimeoutMs = 
      std::chrono::duration_cast<std::chrono::milliseconds>(
        nextTimeout - std::chrono::steady_clock::now()
      ).count();
  // Make sure the value is within a reasonable range (>=0, less that 5 minutes).
  nextTimeoutMs = std::max(0L, nextTimeoutMs);
  int64_t fiveMin = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::minutes(5)
      ).count();
  nextTimeoutMs = std::min(fiveMin, nextTimeoutMs);
  // Call epoll with an array of 5 events. As we expect 3-4 processes, this
  // should be large enough
  const int eventSlotCount = 5;
  ::epoll_event ee[eventSlotCount];
  int receivedEvents = ::epoll_wait(m_epollFd, ee, eventSlotCount, nextTimeoutMs);
  cta::exception::Errnum::throwOnMinusOne(receivedEvents, 
      "In ProcessManager::run(): failed to ::epoll_wait()");
  for (int i=0; i< receivedEvents; i++) {
    // The subprocess handers registered themselves to epoll, so we have the 
    // pointer to it.
    SubprocessHandler::ProcessingStatus status = 
        ((SubprocessHandler*)ee[i].data.ptr)->processEvent();
    // Record the status with the right handler
    for(auto & sp: m_subprocessHandlers) {
      if (ee[i].data.ptr == sp.handler.get()) {
        sp.status = status;
      }
    }
  }
  // We now updated all statuses for the next iteration of the loop.
}

}}} // namespace cta::tape::daemon