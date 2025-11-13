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
#include "SignalHandler.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"

#include <unistd.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
cta::SignalReader::SignalReader() {
  // We will ignore non handled signals in the main logic.
  ::sigset_t sigMask;
  ::sigemptyset(&sigMask);
  const std::vector<int> sigList = {SIGHUP,
                                      SIGINT,
                                      SIGQUIT,
                                      SIGPIPE,
                                      SIGTERM,
                                      SIGUSR1,
                                      SIGUSR2,
                                      SIGCHLD,
                                      SIGTSTP,
                                      SIGTTIN,
                                      SIGTTOU,
                                      SIGPOLL,
                                      SIGURG,
                                      SIGVTALRM};
  for (auto sig : sigList) {
    ::sigaddset(&sigMask, sig);
  }
  exception::Errnum::throwOnNonZero(::sigprocmask(SIG_BLOCK, &sigMask, nullptr),
                                    "In SignalHandler::SignalHandler(): sigprocmask() failed");

  m_sigFd = ::signalfd(-1, &sigMask, SFD_NONBLOCK);
  exception::Errnum::throwOnMinusOne(m_sigFd, "In SignalHandler::SignalHandler(): signalfd() failed");
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
cta::SignalReader::~SignalReader() noexcept {
  if (m_sigFd != -1) {
    ::close(m_sigFd);
  }
}

//------------------------------------------------------------------------------
// SignalHandler::processSignals
//------------------------------------------------------------------------------
std::set<uint32_t> cta::SignalReader::processAndGetSignals(log::LogContext& lc) const {
  std::set<uint32_t> signalSet {};

  struct ::signalfd_siginfo sigInf;
  int rc = 0;
  while(true){
    rc = static_cast<int>(::read(m_sigFd, &sigInf, sizeof(sigInf)));

    // Exit the loop if there is no signal to be processed
    if (rc == -1 && errno == EAGAIN) break;

    // We should get exactly sizeof(sigInf) bytes.
    if (rc != sizeof(sigInf)) {
      std::stringstream err;
      err << "In SignalHandler::processSignals(): unexpected size for read: "
             "got="
          << rc << " expected=" << sizeof(sigInf);
      throw exception::Exception(err.str());
    }

    // Add signal information to the logs
    log::ScopedParamContainer params(lc);
    if (char* signalName = ::strsignal(sigInf.ssi_signo)) {
      params.add("signal", signalName);
    } else {
      params.add("signal", sigInf.ssi_signo);
    }

    switch (sigInf.ssi_signo) {
      case SIGUSR1:
        signalSet.insert(SIGUSR1);
        lc.log(log::INFO, "Received SIGUSR1.");
        break;
      case SIGUSR2:
        signalSet.insert(SIGUSR2);
        lc.log(log::INFO, "Received SIGUSR2.");
        break;
      case SIGHUP:
        signalSet.insert(SIGHUP);
        lc.log(log::INFO, "Received SIGHUP.");
        break;
      case SIGINT:
      case SIGTERM:
        signalSet.insert(SIGTERM);
        lc.log(log::INFO, "Received SIGTERM or SIGINT.");
        break;
      default:
        lc.log(log::INFO, "In signal handler, ignoring signal.");
        break;
    }
  }

  return signalSet;
}
