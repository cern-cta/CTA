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

#include "castor/acs/AcsCmdLine.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"

#include <stdlib.h>

//------------------------------------------------------------------------------
// parseQueryInterval
//------------------------------------------------------------------------------
int castor::acs::AcsCmdLine::parseQueryInterval(const std::string &s) {
  const int queryInterval = atoi(s.c_str());
  if(0 >= queryInterval) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Query value must be an integer greater than 0"
      ": value=" << queryInterval;
    throw ex;
  }

  return queryInterval;
}

//------------------------------------------------------------------------------
// parseTimeout
//------------------------------------------------------------------------------
int castor::acs::AcsCmdLine::parseTimeout(const std::string &s) {
  const int timeout = atoi(s.c_str());
  if(0 >= timeout) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "Timeout value must be an integer greater than 0"
      ": value=" << timeout;
    throw ex;
  }
  return timeout;
}

//------------------------------------------------------------------------------
// handleMissingParameter
//------------------------------------------------------------------------------
void castor::acs::AcsCmdLine::handleMissingParameter(const int opt) {
  castor::exception::MissingOperand ex;
  ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
 throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void castor::acs::AcsCmdLine::handleUnknownOption(const int opt) {
  castor::exception::InvalidArgument ex;
  if(0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  } else {
    ex.getMessage() << "Unknown command-line option: -" << (char)opt;
  }
  throw ex;
}
