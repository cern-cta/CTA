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

#include "common/log/FileLogger.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"
#include "common/exception/Errnum.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#include <future>
#include <chrono>
#include <thread>

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FileLogger::FileLogger(std::string_view hostName, std::string_view programName, std::string_view filePath, int logMask, std::optional<int> signum) :
  Logger(hostName, programName, logMask), m_waitSignal(signum.value_or(-1)), m_invalidFd(nullptr), m_filePath(filePath), m_invalidator(*this) {
  m_fd = ::open(filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP);
  exception::Errnum::throwOnMinusOne(m_fd, std::string("In FileLogger::FileLogger(): failed to open log file: ") + std::string(filePath));

  if (signum.value_or(false)){
    threading::MutexLocker lock(m_invalidatorMutex);
    invalidFdList.emplace_back(std::make_unique<std::atomic<bool>>(false));
    m_invalidFd = invalidFdList.back().get();
    m_invalidator.start();
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
FileLogger::~FileLogger() {
  if (-1 != m_fd) {
    ::close(m_fd);
  }

  // Kill invalidator thread if needed.
  if (m_invalidFd != nullptr){
    m_invalidator.kill();
  }
}

//-----------------------------------------------------------------------------
// FileLogger::writeMsgToUnderlyingLoggingSystem
//-----------------------------------------------------------------------------
void FileLogger::writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) {
  if (-1 == m_fd) {
    throw exception::Exception("In FileLogger::writeMsgToUnderlyingLoggingSystem(): file is not properly initialised");
  }

  // Prepare the string to print
  std::ostringstream logLine;
  logLine << (m_logFormat == LogFormat::JSON ? "{" : "")
          << header << body
          << (m_logFormat == LogFormat::JSON ? "}" : "")
          << std::endl;


  threading::MutexLocker fdLock(m_mutex);

  // Check flag and if we need to udpate.
  if (m_invalidFd != nullptr && m_invalidFd->exchange(false)) {
    ::close(m_fd);

    m_fd = ::open(m_filePath.data(), O_APPEND | O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP);

    exception::Errnum::throwOnMinusOne(m_fd, std::string("In FileLogger::writeMsgToUnderlyingLoggingSystem(): failed to open log file: ") + std::string(m_filePath.data()));
  }


  // Append the message to the file
  exception::Errnum::throwOnMinusOne(::write(m_fd, logLine.str().c_str(), logLine.str().size()),
    "In FileLogger::writeMsgToUnderlyingLoggingSystem(): failed to write to file");
}

//-----------------------------------------------------------------------------
// FdInvalidatorThread::FdInvalidatorThread
//-----------------------------------------------------------------------------
FileLogger::FdInvalidatorThread::FdInvalidatorThread(FileLogger& parent) : m_parent(parent) { }

//-----------------------------------------------------------------------------
// FdInvalidatorThread::run
//-----------------------------------------------------------------------------
void FileLogger::FdInvalidatorThread::run() {
  ::sigset_t sigMask;
  int recvSig;
  ::sigaddset(&sigMask, m_parent.m_waitSignal);

  auto exitFuture = m_exit.get_future();
  while(std::future_status::ready != exitFuture.wait_for(std::chrono::seconds(0))){
    ::sigwait(&sigMask, &recvSig);

    // Update registered file loggers.
    threading::MutexLocker lock(m_parent.m_invalidatorMutex);
    for(auto& fdFlag : m_parent.invalidFdList) fdFlag->exchange(true);
  }
}

} // namespace cta::log
