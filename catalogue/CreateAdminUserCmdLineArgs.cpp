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

#include "catalogue/CreateAdminUserCmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <getopt.h>
#include <ostream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CreateAdminUserCmdLineArgs::CreateAdminUserCmdLineArgs(const int argc, char *const *const argv):
  help(false) {
  static struct option longopts[] = {
    {"comment"  , required_argument, NULL, 'm'},
    {"help"     ,       no_argument, NULL, 'h'},
    {"username" , required_argument, NULL, 'u'},
    {NULL       ,                 0, NULL,   0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":m:hu:", longopts, NULL)) != -1) {
    switch(opt) {
    case 'm':
      comment = optarg ? optarg : "";
      break;
    case 'h':
      help = true;
      break;
    case 'u':
      adminUsername = optarg ? optarg : "";
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

  if(adminUsername.empty()) {
    throw exception::CommandLineNotParsed("The username option must be specified with a non-empty string");
  }

  if(comment.empty()) {
    throw exception::CommandLineNotParsed("The comment option must be specified with a non-empty string");
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

    // Check the number of arguments
  if(nbArgs != 1) {
    exception::CommandLineNotParsed ex;
    ex.getMessage() << "Wrong number of command-line arguments: expected=1 actual=" << nbArgs;
    throw ex;
  }

  dbConfigPath = argv[optind];
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void CreateAdminUserCmdLineArgs::printUsage(std::ostream &os) {
  os <<
    "Usage:" << std::endl <<
    "    cta-catalogue-admin-user-create databaseConnectionFile -u <username> -m <comment> [-h]" << std::endl <<
    "Where:" << std::endl <<
    "    databaseConnectionFile" << std::endl <<
    "        The path to the file containing the connection details of the CTA" << std::endl <<
    "        catalogue database" << std::endl <<
    "Options:" << std::endl <<
    "    -u,--username <username>" << std::endl <<
    "        The name of the admin user to be created" << std::endl <<
    "    -m,--comment <comment>" << std::endl <<
    "        Comment to describe the creation of the admin user" << std::endl <<
    "    -h,--help" << std::endl <<
    "        Prints this usage message" << std::endl <<
    "" << std::endl;;
}

} // namespace catalogue
} // namespace cta
