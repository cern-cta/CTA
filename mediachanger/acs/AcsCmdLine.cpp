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

#include "AcsCmdLine.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"

#include <stdlib.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// parseQueryInterval
//------------------------------------------------------------------------------
int cta::acs::AcsCmdLine::parseQueryInterval(const std::string &s) {
  const int queryInterval = atoi(s.c_str());
  if(0 >= queryInterval) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() << "Query value must be an integer greater than 0"
      ": value=" << queryInterval;
    throw ex;
  }

  return queryInterval;
}

//------------------------------------------------------------------------------
// parseTimeout
//------------------------------------------------------------------------------
int cta::acs::AcsCmdLine::parseTimeout(const std::string &s) {
  const int timeout = atoi(s.c_str());
  if(0 >= timeout) {
    cta::exception::InvalidArgument ex;
    ex.getMessage() << "Timeout value must be an integer greater than 0"
      ": value=" << timeout;
    throw ex;
  }
  return timeout;
}

//------------------------------------------------------------------------------
// handleMissingParameter
//------------------------------------------------------------------------------
void cta::acs::AcsCmdLine::handleMissingParameter(const int opt) {
  cta::exception::MissingOperand ex;
  ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
 throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void cta::acs::AcsCmdLine::handleUnknownOption(const int opt) {
  cta::exception::InvalidArgument ex;
  if(0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  } else {
    ex.getMessage() << "Unknown command-line option: -" << (char)opt;
  }
  throw ex;
}
