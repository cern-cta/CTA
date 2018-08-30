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

namespace cta {
namespace mediachanger {
namespace acs {
namespace daemon {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
AcsdCmdLine::AcsdCmdLine():
  foreground(false),                                     
  help(false),
  readOnly(false) {
}

//so instead 
//use the factory pattern that makes an instance of that class together with the arguments wanted passed but not a constryuctor by itself

AcsdCmdLine AcsdCmdLine::parse(const int argc, char *const *const argv) {
  AcsdCmdLine cmdline;

  struct option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    { "foreground", no_argument, NULL, 'f' },
    { "help", no_argument, NULL, 'h' },
    { "config", required_argument, NULL, 'c' },
    { "readOnly" , no_argument, NULL, 'r'},
    { NULL, 0, NULL, '\0' }
  };

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind=0;
  // Prevent getopt from printing out errors on stdout
  opterr=0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, ":fhc:r", longopts, NULL)) != -1) {
     //log:write(LOG_INFO, "Usage: [options]\n");
   switch (c) {
   case 'r':
     cmdline.readOnly = true;
     break;
   case 'f':
     cmdline.foreground = true;
     break;
   case 'c':
     cmdline.configLocation = optarg;
     break;
   case 'h':
     cmdline.help = true;
     break;
   case ':':
     throw exception::Exception(std::string("Incorrect command-line arguments: The -") + (char)optopt +
       " option is missing an argument\n\n" + getUsage());
   case '?':
     throw exception::Exception(std::string("Incorrect command-line arguments: Unknown option\n\n") + getUsage());
   default:
     throw exception::Exception(std::string("Incorrect command-line arguments\n\n") + getUsage());
   }
  }


  const int expectedNbNonOptionalArgs = 0;
  const int nbNonOptionalArgs = argc - optind;

  // Check for empty string arguments
  // These might have been passed in by systemd
  for(int i = optind; i < argc; i++) {
    if(std::string(argv[i]).empty()) {
      exception::Exception ex;
        ex.getMessage() << "Incorrect command-line arguments: Encountered an empty string argument at argv[" << i
          << "]\n\n" << getUsage();
      throw ex;
    }
  }

  if (expectedNbNonOptionalArgs != nbNonOptionalArgs) {
     exception::Exception ex;
     ex.getMessage() << "Incorrect command-line arguments: Incorrect number of non-optional arguments: expected=" <<
       expectedNbNonOptionalArgs << ",actual=" << nbNonOptionalArgs << "\n\n" << getUsage();
     throw ex;
  }

  return cmdline;
}

//------------------------------------------------------------------------------
// getUsage
//------------------------------------------------------------------------------
std::string AcsdCmdLine::getUsage() {
  std::ostringstream usage;
  usage <<
    "Usage:\n"
    "  cta-acsd [options]\n"
    "\n"
    "Where:\n"
    "\n"
    "Options:\n"
    "\n"
    "--foreground            or -f         Remain in the Foreground\n"
    "--readOnly              or -r         Request the volume is mounted for read-only access\n"
    "--config <config-file>  or -c         Configuration file\n"
    "--help                  or -h         Print this help and exit\n"
    "\n"
    "Comments to CTA team\n";
  return usage.str();
}

} // namespace daemon
} // namespace acs
} // namespace mediachanger
} // namespace cta
