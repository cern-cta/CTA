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

#include "tapeserver/daemon/CommandLineParams.hpp"
#include "common/exception/Exception.hpp"
#include <getopt.h>
#include <string.h>

namespace cta::daemon {

CommandLineParams::CommandLineParams(int argc, char** argv):
  foreground(false), logToStdout(false), logToFile(false),
  helpRequested(false){
  configFileLocation = "";
  struct ::option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    { "foreground", no_argument, nullptr, 'f' },
    { "config", required_argument, nullptr, 'c' },
    { "help", no_argument, nullptr, 'h' },
    { "stdout", no_argument, nullptr, 's' },
    { "log-to-file", required_argument, nullptr, 'l' },
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
    default:
      break;
    }
  }
  if (configFileLocation.empty()) {
    throw cta::exception::Exception("In CommandLineParams::CommandLineParams(): no configFileLocation flag provided");
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
  ret.push_back({"configFileLocation", configFileLocation});
  ret.push_back({"helpRequested", helpRequested});
  return ret;
}

} // namespace cta::daemon

