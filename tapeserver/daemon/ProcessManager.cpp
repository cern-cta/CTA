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

#include "ProcessManager.hpp"
#include "common/exception/Errnum.hpp"
#include <sys/epoll.h>
#include <unistd.h>
#include <algorithm>

namespace cta {
namespace tape {
namespace daemon {

ProcessManager::ProcessManager(log::LogContext & log): m_logContext(log) {
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
  cta::log::ScopedParamContainer params(m_logContext);
  params.add("SubprocessName", m_subprocessHandlers.back().handler->index);
  m_logContext.log(log::INFO, "Adding handler for subprocess");
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

cta::log::LogContext&  ProcessManager::logContext() {
  return m_logContext;
}


ProcessManager::RunPartStatus ProcessManager::runShutdownManagement() {
  // Check the current statuses for shutdown requests
  bool nonDriveAskedShutdown = false;
  std::list<SubprocessAndStatus*> drivesToShutdown;
  int aliveDrives = 0;

  for(const auto &i : m_subprocessHandlers) {
    if (i.handler->index.starts_with("drive:")) aliveDrives++;
    if (i.status.shutdownRequested) {
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", i.handler->index);
      m_logContext.log(log::INFO, "Subprocess requested shutdown");
      if (i.handler->index.starts_with("drive:"))
        drivesToShutdown.insert(&i);
      else {
        nonDriveAskedShutdown = true;
        break;
      }
    }
  }
  // If a non drive handler requests a shutdown or all alive drive handlers request shutdown kill everything.
  if (nonDriveAskedShutdown || (aliveDrives == static_cast<int>(drivesToShutdown.size()))) {
    for(auto & sp: m_subprocessHandlers) {
      sp.status = sp.handler->shutdown();
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", sp.handler->index)
            .add("ShutdownComplete", sp.status.shutdownComplete);
      m_logContext.log(log::INFO, "Signaled shutdown to subprocess handler");
    }
  }

  // If all processes completed their shutdown, we can exit
  bool shutdownComplete=true;
  for (auto & sp: m_subprocessHandlers) { shutdownComplete &= sp.status.shutdownComplete; }
  if (shutdownComplete) {
    m_logContext.log(log::INFO, "All subprocesses completed shutdown. Exiting.");
    RunPartStatus ret;
    ret.doExit = true;
    ret.exitCode = EXIT_SUCCESS;
    return ret;
  }

  // Only a subset of alive drives requested shutdown, proceed with those a keep going.
  if (drivesToShutdown.size()){
    cta::log::ScopedParamContainer params(m_logContext);
    m_logContext.log(log::INFO, "Shutting down " + std::to_string(drivesToShutdown.size()) + " drive handlers.");

    for(auto & dh : drivesToShutdown) {
      dh.handler->shutdown();
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", dh.handler->index)
            .add("ShutdownComplete", dh.status.shutdownComplete);
      m_logContext.log(log::INFO, "Signaled shutdown to drive handler.");
    }
  }

  return RunPartStatus();
}

ProcessManager::RunPartStatus ProcessManager::runKillManagement() {
  // If any process asks for a kill, we kill all sub processes and exit
  bool anyAskedKill = std::count_if(m_subprocessHandlers.cbegin(), 
      m_subprocessHandlers.cend(), 
      [&](const SubprocessAndStatus &i){
        if (i.status.killRequested) {
          cta::log::ScopedParamContainer params(m_logContext);
          params.add("SubprocessName", i.handler->index);
          m_logContext.log(log::INFO, "Subprocess requested kill");
        }
        return i.status.killRequested;
      });
  if (anyAskedKill) {
    for(auto & sp: m_subprocessHandlers) { 
      sp.handler->kill(); 
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", sp.handler->index);
      m_logContext.log(log::INFO, "Instructed handler to kill subprocess");
    }
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
      {
        cta::log::ScopedParamContainer params(m_logContext);
        params.add("SubprocessName", sp.handler->index);
        m_logContext.log(log::INFO, "Subprocess handler requested forking");
      }
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", sp.handler->index);
      m_logContext.log(log::INFO, "Subprocess handler will fork");
      auto newStatus = sp.handler->fork();
      switch (newStatus.forkState) {
      case SubprocessHandler::ForkState::child:
        // We are in the child side.
        // Instruct all other handlers to proceed with a post-fork cleanup.
        for (auto & sp2: m_subprocessHandlers) {
          if (&sp2 != &sp) {
            sp2.handler->postForkCleanup();
          }
        }
        // We are in the child side: run the subprocess and exit.
        m_logContext.log(log::INFO, "In child process. Running child.");
        ::exit(sp.handler->runChild());
      case SubprocessHandler::ForkState::parent:
        // We are parent side. Record the new state for this handler
        newStatus.forkState = SubprocessHandler::ForkState::notForking;
        sp.status = newStatus;
        break;
      case SubprocessHandler::ForkState::notForking:
        throw cta::exception::Exception("In ProcessManager::runForkManagement(): unexpected for state (notForking)");
      }
    }
  }
  return RunPartStatus();
}

ProcessManager::RunPartStatus ProcessManager::runSigChildManagement() {
  // If any process handler received sigChild, we signal it to all processes. Typically, this is 
  // done by the signal handler
  bool sigChild = std::count_if(m_subprocessHandlers.cbegin(), 
      m_subprocessHandlers.cend(), 
      [&](const SubprocessAndStatus &i){
        if (i.status.sigChild) {
          cta::log::ScopedParamContainer params(m_logContext);
          params.add("SubprocessName", i.handler->index);
          m_logContext.log(log::INFO, "Handler received SIGCHILD. Propagating to all handlers.");
        }
        return i.status.sigChild;
      });
  if (sigChild) {
    for(auto & sp: m_subprocessHandlers) {
      sp.status = sp.handler->processSigChild();
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("SubprocessName", sp.handler->index);
      m_logContext.log(log::INFO, "Propagated SIGCHILD.");
    }
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
      // If new timeout is still in the past, we overlook it (but log it)
      if (sp.status.nextTimeout < std::chrono::steady_clock::now()) {
        log::ScopedParamContainer params(m_logContext);
        params.add("now", std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
              .add("subprocess", sp.handler->index)
              .add("timeout", std::chrono::duration_cast<std::chrono::seconds>(sp.status.nextTimeout.time_since_epoch()).count());
        m_logContext.log(log::ERR, "In ProcessManager::runEventLoop(): got twice a timeout in the past. Skipping.");
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
  // epoll_wait can get interrupted by signal (like while debugging). This is should not be treated as an error.
  if (-1 == receivedEvents && EINTR == errno) receivedEvents = 0;
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
