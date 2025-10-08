/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <getopt.h>
#include <ostream>
#include <iostream>

#include "StatisticsSaveCmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

namespace cta::statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsSaveCmdLineArgs::StatisticsSaveCmdLineArgs(const int argc, char *const *const argv):
  help(false) {
  static struct option longopts[] = {
    {"catalogueconf",  required_argument, nullptr, 'c'},
    {"help",  no_argument, nullptr, 'h'},
    {nullptr  ,           0, nullptr,   0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while ((opt = getopt_long(argc, argv, ":hc:j", longopts, nullptr)) != -1) {
    switch (opt) {
    case 'h':
      help = true;
      break;
    case 'j':  // This should remove in the future, but we keep it for backward compatibility
      break;
    case 'c':
      catalogueDbConfigPath = optarg;
      break;
    case ':':  // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << static_cast<char>(optopt) << " option requires a parameter";
        throw ex;
      }
    case '?':  // Unknown option
      {
        exception::CommandLineNotParsed ex;
        if (0 == optopt) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << static_cast<char>(optopt);
        }
        throw ex;
      }
    default:
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() <<
          "getopt_long returned the following unknown value: 0x" <<
          std::hex << static_cast<int>(opt);
        throw ex;
      }
    }  // switch(opt)
  }  // while getopt_long()

  // There is no need to continue parsing when the help option is set
  if (help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - 1;

  // Check the number of arguments
  if (nbArgs < 2) {
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
    "    cta-statistics-save --catalogueconf catalogueDbConnectionFile" << std::endl <<
    "Where:" << std::endl <<
    "    catalogueDbConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        catalogue database" << std::endl <<
    "Options:" << std::endl <<
    "    -h,--help" << std::endl <<
    "        Prints this usage message" << std::endl;
}

} // namespace cta::statistics
