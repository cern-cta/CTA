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

#include "SignalHandlerThread.hpp"
#include "Daemon.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"

#include <signal.h>
#include <sys/signalfd.h>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
cta::server::SignalHandlerThread::SignalHandlerThread(const cta::log::LogContext& logContext)
    : m_sigFd(-1),
      m_logContext(logContext) {
  // Main thread already blocked all signals. So lets read them all.
  // We will ignore non handled singals in the main logic.
  ::sigset_t sigMask;
  ::sigfillset(&sigMask);
  m_sigFd = ::signalfd(-1, &sigMask, 0);
  cta::exception::Errnum::throwOnMinusOne(m_sigFd, "In SignalHandlerThread::SignalHandlerThread(): signalfd() failed");
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
cta::server::SignalHandlerThread::~SignalHandlerThread() noexcept {
  if (m_sigFd != -1) {
    ::close(m_sigFd);
  }
}

//------------------------------------------------------------------------------
// SignalHandlerThread::run
//------------------------------------------------------------------------------
void cta::server::SignalHandlerThread::run() {
  // Create file descritr to read from.
  struct ::signalfd_siginfo sigInf;

  // Loop until we receive a signal that forces us to exit the daemon
  while (g_sigTERMINT) {
    // Block until we have something to read from the file descriptor.
    int rc = ::read(m_sigFd, &sigInf, sizeof(sigInf));
    cta::exception::Errnum::throwOnMinusOne(rc);

    // We should get exactly sizeof(sigInf) bytes.
    if (rc != sizeof(sigInf)) {
      std::stringstream err;
      err << "In SignalHandlerThread::run(): unexpected size for read: "
             "got="
          << rc << " expected=" << sizeof(sigInf);
      throw cta::exception::Exception(err.str());
    }

    switch (sigInf.ssi_signo) {
      case SIGUSR1: {
        g_sigUSR1 = 1;
        m_logContext.log(log::INFO, "Received SIGUSR1.");
        break;
      }
      case SIGHUP: {
        g_sigHUP = 1;
        m_logContext.log(log::INFO, "Received SIGHUP.");
        break;
      }
      case SIGINT:
      case SIGTERM: {
        g_sigTERMINT = 1;
        m_logContext.log(log::INFO, "Received SIGTERM or SIGINT.");
        break;
      }
      default: {
        m_logContext().log(log::INFO, "In signal handler, ignoring signal number " + sigInf.ssi_signo);
        break;
      }
    }
  }
}
