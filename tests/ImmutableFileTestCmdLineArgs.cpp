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

#include "common/exception/CommandLineNotParsed.hpp"
#include "tests/ImmutableFileTestCmdLineArgs.hpp"

#include <getopt.h>
#include <ostream>

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ImmutableFileTestCmdLineArgs::ImmutableFileTestCmdLineArgs(const int argc, char *const *const argv):
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
  const int nbArgs = argc - optind;

  // Check the number of arguments
  if(nbArgs != 1) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Wrong number of command-line arguments: expected=1 actual=" << nbArgs;
    throw ex;
  }

  const std::string fileUrlString = argv[optind];

  fileUrl.FromString(fileUrlString);

  if(!fileUrl.IsValid()) {
    throw exception::Exception(std::string("File URL is not a valid XRootD URL: URL=") + fileUrlString);
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void ImmutableFileTestCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:"                                            "\n"
    "    cta-immutable-file-test URL"                   "\n"
    "Where:"                                            "\n"
    "    URL is the XRootD URL of the destination file" "\n"
    "Options:"                                          "\n"
    "    -h,--help"                                     "\n"
    "        Prints this usage message"                 "\n"
    "WARNING:"                                          "\n"
    "    This command will destroy the destination file!";
  os << std::endl;
}

} // namespace cta
