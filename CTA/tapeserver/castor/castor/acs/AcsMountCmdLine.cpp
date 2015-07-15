/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/acs/Acs.hpp"
#include "castor/acs/AcsMountCmdLine.hpp"
#include "castor/acs/Constants.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"

#include <getopt.h>
#include <stdlib.h>
#include <string.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::acs::AcsMountCmdLine::AcsMountCmdLine() throw():
  debug(false),
  help(false),
  queryInterval(0),
  readOnly(FALSE),
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
castor::acs::AcsMountCmdLine::AcsMountCmdLine(const int argc,
  char *const *const argv):
  debug(false),
  help(false),
  queryInterval(ACS_QUERY_INTERVAL),
  readOnly(FALSE),
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
    {"readonly" , 0, NULL, 'r'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":dhq:rt:", longopts, NULL)) != -1) {
    processOption(opt);
  }

  // There is no need to continue parsing when the help option is set
  if(help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  // Check that both VID and DRIVE_SLOT have been specified
  if(nbArgs < 2) {
    castor::exception::MissingOperand ex;
    ex.getMessage() << "Both VID and DRIVE_SLOT must be specified";
    throw ex;
  }

  // Parse VID
  volId = Acs::str2Volid(argv[optind]);

  // Move on to the next command-line argument
  optind++;

  // Parse DRIVE_SLOT
  libraryDriveSlot = Acs::str2DriveId(argv[optind]);
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void castor::acs::AcsMountCmdLine::processOption(const int opt) {
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
  case 'r':
    readOnly = TRUE;
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
      castor::exception::Exception ex;
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
std::string castor::acs::AcsMountCmdLine::getUsage() throw() {
  std::ostringstream usage;
  usage <<
  "Usage:\n"
  "\n"
  "  castor-tape-acs-mount [options] VID DRIVE_SLOT\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID        The VID of the volume to be mounted.\n"
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
  "\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << ACS_QUERY_INTERVAL << ".\n"
  "\n"
  "  -r|--readOnly         Request the volume is mounted for read-only access\n"
  "\n"
  "  -t|--timeout SECONDS  Time to wait for the mount to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS is "
    << ACS_CMD_TIMEOUT << ".\n"
  "\n"
  "Comments to: Castor.Support@cern.ch\n";
  return usage.str();
}
