/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsdCmdLine.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/log.hpp"
#include <iostream>
#include <getopt.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsdCmdLine::AcsdCmdLine():
  foreground(false),
  help(false),
  configLocation(""){
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsdCmdLine::AcsdCmdLine(const int argc,
  char *const *const argv):
  foreground(false),    //< Prevents daemonisation
  help(false),          //< Help requested: will print out help and exit.
  configLocation(""){   //< Location of the configuration file. Defaults to /etc/cta/cta-taped.conf

  
static struct option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    { "foreground", no_argument, NULL, 'f' },
    { "config", required_argument, NULL, 'c' },
    { "help", no_argument, NULL, 'h' },
    { NULL, 0, NULL, '\0' }
  };

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fc:h", longopts, NULL)) != -1) {
     //log:write(LOG_INFO, "Usage: [options]\n");
    switch (c) {
    case 'f':
      foreground = true;
      break;
    case 'c':
      configLocation = optarg;
      break;
    case 'h':
      help = true;
   std::cout<<"Usage: "<<"cta-acsd"<<" [options]\n"
    "where options can be:\n"
    "--foreground            or -f         Remain in the Foreground\n"
    "--config <config-file>  or -c         Configuration file\n"
    "--help                  or -h         Print this help and exit\n";
      break;
    default:
      break;
    }
  }
}


