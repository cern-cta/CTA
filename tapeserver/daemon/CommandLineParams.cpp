/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tapeserver/daemon/CommandLineParams.hpp"
#include "common/exception/Exception.hpp"
#include <getopt.h>
#include <string.h>

namespace cta::daemon {

CommandLineParams::CommandLineParams(int argc, char** argv) {
  struct ::option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    { "foreground", no_argument, nullptr, 'f' },
    { "config", required_argument, nullptr, 'c' },
    { "help", no_argument, nullptr, 'h' },
    { "stdout", no_argument, nullptr, 's' },
    { "log-to-file", required_argument, nullptr, 'l' },
    { "log-format", required_argument, nullptr, 'o' },
    { nullptr, 0, nullptr, '\0' }
  };

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fsc:l:h", longopts, nullptr)) != -1) {
    switch (c) {
    case 'f':
      foreground = true;
      break;
    case 's':
      logToStdout = true;
      break;
    case 'c':
      configFileLocation = optarg;
      break;
    case 'h':
      helpRequested = true;
      break;
    case 'l':
      logFilePath = optarg;
      logToFile = true;
      break;
    case 'o':
      logFormat = optarg;
      break;
    default:
      break;
    }
  }
  if (logToStdout && !foreground) {
    throw cta::exception::Exception("In CommandLineParams::CommandLineParams(): cannot log to stdout without running in the foreground");
  }
  if (logToFile && logToStdout) {
    throw cta::exception::Exception("In CommandLineParams::CommandLineParams(): cannot log to both stdout and file");
  }
}

std::list<cta::log::Param> CommandLineParams::toLogParams() const {
  std::list<cta::log::Param> ret;
  ret.push_back({"foreground", foreground});
  ret.push_back({"logToStdout", logToStdout});
  ret.push_back({"logToFile", logToFile});
  ret.push_back({"logFilePath", logFilePath});
  ret.push_back({"logFormat", logFormat});
  ret.push_back({"configFileLocation", configFileLocation});
  ret.push_back({"helpRequested", helpRequested});
  return ret;
}

} // namespace cta::daemon
