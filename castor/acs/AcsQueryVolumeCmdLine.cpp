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
#include "castor/acs/AcsCmdLine.hpp"
#include "castor/acs/AcsQueryVolumeCmdLine.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"

#include <getopt.h>
#include <stdlib.h>
#include <string.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::acs::AcsQueryVolumeCmdLine::AcsQueryVolumeCmdLine()
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
castor::acs::AcsQueryVolumeCmdLine::AcsQueryVolumeCmdLine(const int argc,
  char *const *const argv, const int defaultQueryInterval,
  const int defaultTimeout):
  debug(FALSE),
  help(FALSE),
  queryInterval(0),
  timeout(0) {
  memset(volId.external_label, '\0', sizeof(volId.external_label));

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };

  // Set the query option to the default value
  queryInterval = defaultQueryInterval;

  // Set timeout option to the default value
  timeout = defaultTimeout;

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
    castor::exception::MissingOperand ex;
    ex.getMessage() << "VID must be specified";
    throw ex;
  }

  // Parse the VID command-line argument
  volId = Acs::str2Volid(argv[optind]);
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void castor::acs::AcsQueryVolumeCmdLine::processOption(const int opt) {
  switch(opt) {
  case 'd':
    debug = true;
    break;
  case 'h':
    help = true;
    break;
  case 'q':
    queryInterval = AcsCmdLine::parseQueryInterval(optarg);
    break;
  case 't':
    timeout = AcsCmdLine::parseTimeout(optarg);
    break;
  case ':':
    return AcsCmdLine::handleMissingParameter(optopt);
  case '?':
    return AcsCmdLine::handleUnknownOption(optopt);
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
