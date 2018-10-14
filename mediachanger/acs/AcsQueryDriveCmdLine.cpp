
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
#include "AcsQueryDriveCmdLine.hpp"
#include "Constants.hpp"
#include "common/exception/MissingOperand.hpp"
#include <getopt.h>
#include <stdlib.h>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::acs::AcsQueryDriveCmdLine::AcsQueryDriveCmdLine():
  debug(false),
  help(false),
  queryInterval(0),
  timeout(0) {
  libraryDriveSlot.panel_id.lsm_id.acs = (ACS)0;
  libraryDriveSlot.panel_id.lsm_id.lsm = (LSM)0;
  libraryDriveSlot.panel_id.panel = (PANEL)0;
  libraryDriveSlot.drive = (DRIVE)0;
  memset(volId.external_label, '\0', sizeof(volId.external_label));
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsQueryDriveCmdLine::AcsQueryDriveCmdLine(const int argc,
  char *const *const argv):
  debug(false),
  help(false),
  queryInterval(ACS_QUERY_INTERVAL),
  timeout(ACS_CMD_TIMEOUT) {
  libraryDriveSlot.panel_id.lsm_id.acs = (ACS)0;
  libraryDriveSlot.panel_id.lsm_id.lsm = (LSM)0;
  libraryDriveSlot.panel_id.panel = (PANEL)0;
  libraryDriveSlot.drive = (DRIVE)0;
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

  // Check that DRIVE_SLOT has been specified
  if(1 > nbArgs) {
    cta::exception::MissingOperand ex;
    ex.getMessage() << "DRIVE_SLOT must be specified";
    throw ex;
  }

  // Check that aren't too many non-optional arguments
  if(1 < nbArgs) {
    cta::exception::Exception ex;
    ex.getMessage() << "To many non-optional arguments: expected=1,actual=" << nbArgs;
    throw ex;
  }

  // Parse DRIVE_SLOT
  libraryDriveSlot = str2DriveId(argv[optind]);
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsQueryDriveCmdLine::processOption(const int opt) {
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
  std::string cta::mediachanger::acs::AcsQueryDriveCmdLine::getUsage() {
  std::ostringstream usage;
  usage <<
  "Usage:\n"
  "\n"
  << getProgramName() << " [options] DRIVE_SLOT\n"
  "\n"
  "Where:\n"
  "\n"
  "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
  "             The format of DRIVE_SLOT is as follows:\n"
  "\n"
  "               ACS:LSM:panel:transport\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help             Print this help message and exit.\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << ACS_QUERY_INTERVAL << ".\n"
  "  -t|--timeout SECONDS  Time to wait for the query to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS is "
    << ACS_CMD_TIMEOUT << ".\n"
  "\n";
  return usage.str();
}

//------------------------------------------------------------------------------
// getProgramName
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::AcsQueryDriveCmdLine::getProgramName() {
  return "cta-acs-querydrive";
}

