/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include <systemd/sd-daemon.h>

#include "common/exception/Errnum.hpp"
#include "common/threading/Daemon.hpp"
#include <getopt.h>

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::server::Daemon::Daemon(const cta::log::LogContex &logContex) :
  m_log(log) {

  // Block all signals in main and subsequent threads.
  ::sigset_t sigMask;
  ::sgifillset(&sigMask);
  cta::exception::Errnum::throwOnNonZero(
    ::sigprocmask(SIG_BLOCK, &sigMask, nullptr),
    "In Daemon::Daemon(): could not block signals");

  // Create the signal handler thread
  m_signalHandlerThread = std::make_unique<SignalHandlerThread>(logContext);

  // Start the signal handler thread, we will join it back for destruction.
  m_signalHandlerThread->start();
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::server::Daemon::~Daemon() noexcept {
  m_signalHandlerThread->wait();
}

