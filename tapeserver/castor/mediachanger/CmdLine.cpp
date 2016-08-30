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

#include "castor/mediachanger/CmdLine.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"
#include <getopt.h>

//------------------------------------------------------------------------------
// handleMissingParameter
//------------------------------------------------------------------------------
void castor::mediachanger::CmdLine::handleMissingParameter(const int opt) {
  castor::exception::MissingOperand ex;
  ex.getMessage() << "The -" << (char)opt << " option requires a parameter";
 throw ex;
}

//------------------------------------------------------------------------------
// handleUnknownOption
//------------------------------------------------------------------------------
void castor::mediachanger::CmdLine::handleUnknownOption(const int opt) {
  cta::exception::InvalidArgument ex;
  if(0 == optopt) {
    ex.getMessage() << "Unknown command-line option";
  } else {
    ex.getMessage() << "Unknown command-line option: -" << (char)opt;
  }
  throw ex;
}
