/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tests/ImmutableFileTestCmdLineArgs.hpp"

#include "common/exception/CommandLineNotParsed.hpp"

#include <getopt.h>
#include <ostream>

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ImmutableFileTestCmdLineArgs::ImmutableFileTestCmdLineArgs(const int argc, char* const* const argv) {
  static struct option longopts[] = {
    {"help",  no_argument, nullptr, 'h'},
    {nullptr, 0,           nullptr, 0  }
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while ((opt = getopt_long(argc, argv, ":h", longopts, nullptr)) != -1) {
    switch (opt) {
      case 'h':
        help = true;
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
        ex.getMessage() << "getopt_long returned the following unknown value: 0x" << std::hex << (int) opt;
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

  const std::string fileUrlString = argv[optind];

  fileUrl.FromString(fileUrlString);

  if (!fileUrl.IsValid()) {
    throw exception::Exception(std::string("File URL is not a valid XRootD URL: URL=") + fileUrlString);
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void ImmutableFileTestCmdLineArgs::printUsage(std::ostream& os) {
  os << "Usage:"
        "\n"
        "    cta-immutable-file-test URL"
        "\n"
        "Where:"
        "\n"
        "    URL is the XRootD URL of the destination file"
        "\n"
        "Options:"
        "\n"
        "    -h,--help"
        "\n"
        "        Prints this usage message"
        "\n"
        "WARNING:"
        "\n"
        "    This command will destroy the destination file!";
  os << std::endl;
}

}  // namespace cta
