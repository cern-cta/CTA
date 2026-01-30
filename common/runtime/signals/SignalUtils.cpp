/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalUtils.hpp"

#include <signal.h>

namespace cta::runtime::utils {

std::string signalToString(int signal) {
  // It doesn't seem like there is a reliable way of automatically converting these values
  switch (signal) {
    case SIGHUP:
      return "SIGHUP";
    case SIGINT:
      return "SIGINT";
    case SIGQUIT:
      return "SIGQUIT";
    case SIGILL:
      return "SIGILL";
    case SIGTRAP:
      return "SIGTRAP";
    case SIGABRT:
      return "SIGABRT";
    case SIGBUS:
      return "SIGBUS";
    case SIGFPE:
      return "SIGFPE";
    case SIGKILL:
      return "SIGKILL";
    case SIGUSR1:
      return "SIGUSR1";
    case SIGSEGV:
      return "SIGSEGV";
    case SIGUSR2:
      return "SIGUSR2";
    case SIGPIPE:
      return "SIGPIPE";
    case SIGALRM:
      return "SIGALRM";
    case SIGTERM:
      return "SIGTERM";
    case SIGCHLD:
      return "SIGCHLD";
    case SIGCONT:
      return "SIGCONT";
    case SIGSTOP:
      return "SIGSTOP";
    case SIGTSTP:
      return "SIGTSTP";
    case SIGTTIN:
      return "SIGTTIN";
    case SIGTTOU:
      return "SIGTTOU";
    case SIGURG:
      return "SIGURG";
    case SIGXCPU:
      return "SIGXCPU";
    case SIGXFSZ:
      return "SIGXFSZ";
    case SIGVTALRM:
      return "SIGVTALRM";
    case SIGPROF:
      return "SIGPROF";
    case SIGWINCH:
      return "SIGWINCH";
    case SIGIO:
      return "SIGIO";
#ifdef SIGPWR
    case SIGPWR:
      return "SIGPWR";
#endif
    case SIGSYS:
      return "SIGSYS";
  }

  // Handle realtime signals
  if (signal >= SIGRTMIN && signal <= SIGRTMAX) {
    return "SIGRTMIN+" + std::to_string(signal - SIGRTMIN);
  }

  return "UNKNOWN(" + std::to_string(signal) + ")";
}

}  // namespace cta::runtime::utils
