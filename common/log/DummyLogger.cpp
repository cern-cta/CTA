/*******************************************************************************
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
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 ******************************************************************************/

#include "common/log/DummyLogger.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::log::DummyLogger::DummyLogger(const std::string &programName):
  Logger(programName) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::log::DummyLogger::~DummyLogger() throw() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::DummyLogger::prepareForFork() {
}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params) throw() {
}
