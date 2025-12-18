/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CreateSchemaCmdLineArgs.hpp"

#include "common/exception/CommandLineNotParsed.hpp"

#include <getopt.h>
#include <ostream>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateSchemaCmdLineArgs::CreateSchemaCmdLineArgs(const int argc, char* const* const argv) {
  static struct option longopts[] = {
    {"help",    no_argument,       nullptr, 'h'},
    {"version", required_argument, nullptr, 'v'},
    {nullptr,   0,                 nullptr, 0  }
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  for (int opt = 0; (opt = getopt_long(argc, argv, ":hv:", longopts, nullptr)) != -1;) {
    switch (opt) {
      case 'h':
        help = true;
        break;
      case 'v':
        catalogueVersion = optarg;
        break;
      case ':':  // Missing parameter
      {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "The -" << (char) opt << " option requires a parameter";
        throw ex;
      }
      case '?':  // Unknown option
      {
        exception::CommandLineNotParsed ex;
        if (0 == optopt) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << (char) optopt;
        }
        throw ex;
      }
      default: {
        exception::CommandLineNotParsed ex;
        ex.getMessage() << "getopt_long returned the following unknown value: 0x" << std::hex << opt;
        throw ex;
      }
    }  // switch(opt)
  }  // while getopt_long()

  // There is no need to continue parsing when the help option is set
  if (help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  // Check the number of arguments
  if (const int nbArgs = argc - optind; nbArgs != 1) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Wrong number of command-line arguments: expected=1 actual=" << nbArgs;
    throw ex;
  }

  dbConfigPath = argv[optind];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateSchemaCmdLineArgs::printUsage(std::ostream& os) {
  os << "Usage:" << std::endl
     << "    cta-catalogue-schema-create databaseConnectionFile [options]" << std::endl
     << "Where:" << std::endl
     << "    databaseConnectionFile" << std::endl
     << "        The path to the file containing the connection details of the CTA" << std::endl
     << "        catalogue database" << std::endl
     << "Options:" << std::endl
     << "    -h,--help" << std::endl
     << "        Prints this usage message" << std::endl
     << "    -v,--version" << std::endl
     << "        Version of the catalogue to be created" << std::endl;
}

}  // namespace cta::catalogue
