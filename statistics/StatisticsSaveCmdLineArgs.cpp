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

#include "StatisticsSaveCmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <getopt.h>
#include <ostream>
#include <iostream>

namespace cta {
namespace statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsSaveCmdLineArgs::StatisticsSaveCmdLineArgs(const int argc, char *const *const argv):
  help(false) {

  static struct option longopts[] = {
    {"catalogueconf",required_argument,NULL,'c'},
    {"statisticsconf",required_argument,NULL,'s'},
    {"build",no_argument, NULL, 'b'},
    {"drop", no_argument, NULL, 'd'},
    {"json", no_argument, NULL, 'j'},
    {"help", no_argument, NULL, 'h'},
    {NULL  ,           0, NULL,   0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":hbdjc:s:", longopts, NULL)) != -1) {
    switch(opt) {
    case 'h':
      help = true;
      break;
    case 'c':
      catalogueDbConfigPath = optarg;
      break;
    case 's':
      statisticsDbConfigPath = optarg;
      break;
    case 'b':
      buildDatabase = true;
      break;
    case 'd':
      dropDatabase = true;
      break;
    case 'j':
      jsonOutput = true;
      break;
    case ':': // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char)optopt << " option requires a parameter";
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
  const int nbArgs = argc - 1;

  // Check the number of arguments
  if(nbArgs < 2 ) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "At least 2 arguments should be provided. Only " << nbArgs << " provided";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsSaveCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "    cta-statistics-save --catalogueconf catalogueDbConnectionFile [options]" << std::endl <<
    "Where:" << std::endl <<
    "    catalogueDbConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        catalogue database" << std::endl <<
    "Options:" << std::endl <<
    "    -s,--statisticsconf statisticsDbConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        statistics database" << std::endl <<
    "    -h,--help" << std::endl <<     
    "        Prints this usage message" << std::endl <<
    "    -b, --build" << std::endl <<
    "        Builds the statistics database" << std::endl <<
    "    -d, --drop" << std::endl <<
    "        Drops the statistics database" << std::endl <<
    "    -j, --json" << std::endl <<
    "        Dumps the statistics in json format on stdout" << std::endl;
}

} // namespace statistics
} // namespace cta
