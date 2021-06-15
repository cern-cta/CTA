/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "tapeserver/daemon/CommandLineParams.hpp"
#include "common/exception/Exception.hpp"
#include <getopt.h>
#include <string.h>

namespace cta { namespace daemon {

CommandLineParams::CommandLineParams(int argc, char** argv):
  foreground(false), logToStdout(false), logToFile(false),
  configFileLocation("/etc/cta/cta-taped.conf"),
  helpRequested(false){
  struct ::option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    { "foreground", no_argument, NULL, 'f' },
    { "config", required_argument, NULL, 'c' },
    { "help", no_argument, NULL, 'h' },
    { "stdout", no_argument, NULL, 's' },
    { "log-to-file", required_argument, NULL, 'l' },
    { NULL, 0, NULL, '\0' }
  };

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fsc:l:h", longopts, NULL)) != -1) {
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

}} //  namespace cta::daemon

