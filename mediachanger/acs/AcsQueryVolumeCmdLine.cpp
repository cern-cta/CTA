
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

#include "Acs.hpp"
#include "AcsQueryVolumeCmdLine.hpp"
#include "Constants.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"

#include <getopt.h>
#include <stdlib.h>
#include <string.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::acs::AcsQueryVolumeCmdLine::AcsQueryVolumeCmdLine()
  throw():
  debug(FALSE),
  help(FALSE),
  queryInterval(0),
  timeout(0) {
  memset(volId.external_label, '\0', sizeof(volId.external_label));
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::acs::AcsQueryVolumeCmdLine::AcsQueryVolumeCmdLine(const int argc,
  char *const *const argv):
  debug(FALSE),
  help(FALSE),
  queryInterval(ACS_QUERY_INTERVAL),
  timeout(ACS_CMD_TIMEOUT) {
  memset(volId.external_label, '\0', sizeof(volId.external_label));

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":dhq:t:", longopts, NULL)) != -1) {
    processOption(opt);
  }

  // There is no need to continue parsing when the help option is set
  if(help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  // Check that VID has been specified
  if(nbArgs < 1) {
    cta::exception::MissingOperand ex;
    ex.getMessage() << "VID must be specified";
    throw ex;
  }

  // Parse the VID command-line argument
  volId = Acs::str2Volid(argv[optind]);
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void cta::acs::AcsQueryVolumeCmdLine::processOption(const int opt) {
  switch(opt) {
  case 'd':
    debug = true;
    break;
  case 'h':
    help = true;
    break;
  case 'q':
    queryInterval = parseQueryInterval(optarg);
    break;
  case 't':
    timeout = parseTimeout(optarg);
    break;
  case ':':
    return handleMissingParameter(optopt);
  case '?':
    return handleUnknownOption(optopt);
  default:
    {
      cta::exception::Exception ex;
      ex.getMessage() <<
        "getopt_long returned the following unknown value: 0x" <<
        std::hex << (int)opt;
      throw ex;
    }
  } // switch(opt)
}

//------------------------------------------------------------------------------
// getUsage
//------------------------------------------------------------------------------
std::string cta::acs::AcsQueryVolumeCmdLine::getUsage() throw() {
  std::ostringstream usage;
  usage <<
  "Usage:\n"
  "  cta-tape-acs-queryvolume [options] VID\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be queried.\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "  -h|--help             Print this help message and exit.\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << ACS_QUERY_INTERVAL << ".\n"
  "  -t|--timeout SECONDS  Time to wait for the query to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS in "
    << ACS_CMD_TIMEOUT << ".\n"
  "\n"
  "Comments to: CTA team\n";
  return usage.str();
}
