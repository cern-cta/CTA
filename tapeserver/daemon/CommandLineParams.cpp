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

#include "tapeserver/daemon/CommandLineParams.hpp"
#include "common/exception/Exception.hpp"
#include <getopt.h>
#include <string.h>

cta::daemon::CommandLineParams::CommandLineParams(int argc, char** argv):
  foreground(false), logToStdout(false), 
  configFileLocation("/etc/cta/cta.conf"),
  helpRequested(false){
  struct ::option longopts[5];

  longopts[0].name = "foreground";
  longopts[0].has_arg = no_argument;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';

  longopts[1].name = "config";
  longopts[1].has_arg = required_argument;
  longopts[1].flag = NULL;
  longopts[1].val = 'c';

  longopts[2].name = "help";
  longopts[2].has_arg = no_argument;
  longopts[2].flag = NULL;
  longopts[2].val = 'h';
  
  longopts[3].name = "stdout";
  longopts[3].has_arg = no_argument;
  longopts[3].flag = NULL;
  longopts[3].val = 's';
  
  memset(&longopts[4], 0, sizeof(struct ::option));

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fsc:h", longopts, NULL)) != -1) {
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
    default:
      break;
    }
  }
  if (logToStdout && !foreground) {
    throw cta::exception::Exception("In CommandLineParams::CommandLineParams(): cannot log to stdout without running in the foreground");
  }
}
