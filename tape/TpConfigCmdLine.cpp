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

#include "h/Ctape_api.h"
#include "h/Ctape_constants.h"
#include "tape/TpConfigCmdLine.hpp"
#include "tape/TpConfigException.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
tape::TpConfigCmdLine::TpConfigCmdLine() throw() : status(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
tape::TpConfigCmdLine::TpConfigCmdLine(const std::string &unitName,
  const int status) throw() : unitName(unitName), status(status) {
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
tape::TpConfigCmdLine tape::TpConfigCmdLine::parse(const int argc,
  const char **argv) {
  switch(argc) {
  case 2: return parseShortVersion(argc, argv);
  case 3: return parseLongVersion(argc, argv);
  default:
    {
      std::ostringstream oss;
      oss << "Wrong number of command-line arguments"
        ": expected=1 or 2 actual=" << (argc - 1);
      throw TpConfigException(oss.str(), USERR);
    }
  }
}

//------------------------------------------------------------------------------
// parseShortVersion
//------------------------------------------------------------------------------
tape::TpConfigCmdLine tape::TpConfigCmdLine::parseShortVersion(const int argc,
  const char **argv) {
  const int nbentries = CA_MAXNBDRIVES;
  char hostname[CA_MAXHOSTNAMELEN+1];
  struct drv_status drv_status[CA_MAXNBDRIVES];

  if (0 > gethostname (hostname, sizeof(hostname))) {
    throw TpConfigException("Call to gethostname() failed", SYERR);
  }

  const int nbDrives = Ctape_status (hostname, drv_status, nbentries);
  if (0 > nbDrives) {
    throw TpConfigException("Call to Ctape_status() failed", SYERR);
  }

  if(1 != nbDrives) {
    std::ostringstream oss;
    oss << "Short version of command line can only be used if there"
      " is one and only one drive connected to the tape server"
      ": nbDrives=" << nbDrives;
    throw TpConfigException(oss.str(), USERR);
  }

  return TpConfigCmdLine(drv_status[0].drive, parseStatus(argv[1]));
}

//------------------------------------------------------------------------------
// parseStatus
//------------------------------------------------------------------------------
int tape::TpConfigCmdLine::parseStatus(const std::string &str) {
  if(str == "up") {
    return CONF_UP;
  } else if (str == "down") {
    return CONF_DOWN;
  } else {
    std::ostringstream oss;
    oss << "Invalid status: expected=up or down actual=" << str;
    throw TpConfigException(oss.str(), USERR);
  }
}

//------------------------------------------------------------------------------
// parseCmdLineLongVersion
//------------------------------------------------------------------------------
tape::TpConfigCmdLine tape::TpConfigCmdLine::parseLongVersion(const int argc,
  const char **argv) {
  const std::string unitName = argv[1];
  const int status = parseStatus(argv[2]);

  return TpConfigCmdLine(unitName, status);
}
