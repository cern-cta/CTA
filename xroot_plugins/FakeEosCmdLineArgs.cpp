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

#include "common/exception/CommandLineNotParsed.hpp"
#include "common/utils/utils.hpp"
#include "xroot_plugins/FakeEosCmdLineArgs.hpp"

#include <getopt.h>
#include <ostream>

namespace cta {
namespace xroot_plugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FakeEosCmdLineArgs::FakeEosCmdLineArgs(const int argc, char *const *const argv):
  ctaPort(0),
  help(false) {

  static struct option longopts[] = {
    {"help", no_argument, NULL, 'h'},
    {NULL  ,           0, NULL,   0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":h", longopts, NULL)) != -1) {
    switch(opt) {
    case 'h':
      help = true;
      break;
    case ':': // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
        throw ex;
      }
    case '?': // Unknown option
      {
        exception::CommandLineNotParsed ex;
        if(0 == optopt) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
    default:
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() <<
          "getopt_long returned the following unknown value: 0x" <<
          std::hex << (int)opt;
        throw ex;
      }
    } // switch(opt)
  } // while getopt_long()

  // There is no need to continue parsing when the help option is set
  if(help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int actualNbArgs = argc - optind;

  // Check the number of arguments
  const int expectedNbArgs = 3;
  if(actualNbArgs != expectedNbArgs) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Wrong number of command-line arguments: excepted=" << expectedNbArgs << " actual=" <<
      actualNbArgs;
    throw ex;
  }

  ctaHost = argv[optind];
  try {
    ctaPort = utils::toUint16(argv[optind + 1]);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string("Invalid CTA port number: " + ex.getMessage().str()));
  }
  queryFilename = argv[optind+2];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void FakeEosCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "    cta-xroot_plugins-fakeeos [options] ctaHost ctaPort queryFilename" << std::endl <<
    "Where:" << std::endl <<
    "    ctaHost" << std::endl <<
    "        The network name of the host on which the CTA front end is running" << std::endl <<
    "    ctaPort" << std::endl <<
    "        The TCP/IP port on which the CTA front end is listening for connections" << std::endl <<
    "    queryFilename"  << std::endl <<
    "        The name of file containing the query blob to be sent to the CTA front end" << std::endl <<
    "Options:" << std::endl <<
    "    -h,--help" << std::endl <<
    "        Prints this usage message" << std::endl <<
    "Example:" << std::endl <<
    "    echo -n -e 'Hello\\n\\x00World' > query.txt" << std::endl <<
    "    cta-xrootd_plugins-fakeeos localhost 10955 query.txt" << std::endl;
}

} // namespace xroot_plugins
} // namespace cta
