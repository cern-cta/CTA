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
#include "xroot_plugins/WriteNotificationMsgCmdLineArgs.hpp"

#include <getopt.h>
#include <ostream>

namespace cta {
namespace xroot_plugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
WriteNotificationMsgCmdLineArgs::WriteNotificationMsgCmdLineArgs(const int argc, char *const *const argv):
  writeJsonToStdOut(false),
  help(false) {

  static struct option longopts[] = {
    {"help", no_argument, NULL, 'h'},
    {"json", no_argument, NULL, 'j'},
    {NULL  ,           0, NULL,   0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":hj", longopts, NULL)) != -1) {
    switch(opt) {
    case 'h':
      help = true;
      break;
    case 'j':
      writeJsonToStdOut = true;
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

  if(help && writeJsonToStdOut) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Specifying both -h|--help and -j|--json is not permitted";
    throw ex;
  }

  // Calculate the number of non-option ARGV-elements
  const int actualNbArgs = argc - optind;

  if(help && actualNbArgs > 0) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "A command-line argument cannot be specified with the -h|--help option";
    throw ex;
  }

  if(writeJsonToStdOut && actualNbArgs > 0) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "A command-line argument cannot be specified with the -j|--json option";
    throw ex;
  }

  // There is no need to continue parsing if either the help or json option is
  // set
  if(help || writeJsonToStdOut) {
    return;
  }

  // Check the number of arguments
  const int expectedNbArgs = 1;
  if(actualNbArgs != expectedNbArgs) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Wrong number of command-line arguments: excepted=" << expectedNbArgs << " actual=" <<
      actualNbArgs;
    throw ex;
  }

  filename = argv[optind];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void WriteNotificationMsgCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Description:" << std::endl <<
    "    Writes a \"notification\" message to the specified file" << std::endl <<
    "Usage:" << std::endl <<
    "    cta-xroot_plugins-write-notification-msg filename" << std::endl <<
    "    cta-xroot_plugins-write-notification-msg -h|--help" << std::endl <<
    "    cta-xroot_plugins-write-notification-msg -j|--json" << std::endl <<
    "Where:" << std::endl <<
    "    filename"  << std::endl <<
    "        The name of file to which the notification message will be written."  << std::endl <<
    "        Please note that this file will be overwritten if it already exists." << std::endl <<
    "Options:" << std::endl <<
    "    -h, --help" << std::endl <<
    "        Prints this usage message" << std::endl <<
    "    -j, --json" << std::endl <<
    "        Prints the JSON representation of the notification message to standard out" << std::endl <<
    "Example:" << std::endl <<
    "    cta-xrootd_plugins-write-notification-msg notification.msg" << std::endl <<
    "    cat notification.msg | protoc --decode_raw" << std::endl;
}

} // namespace xroot_plugins
} // namespace cta
