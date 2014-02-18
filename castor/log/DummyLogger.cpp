/*******************************************************************************
 *                      castor/log/DummyLogger.cpp
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
 * @author Steven.Murray@cern.ch
 ******************************************************************************/

#include "castor/log/DummyLogger.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::DummyLogger::DummyLogger(const std::string &programName)
  throw(castor::exception::Internal, castor::exception::InvalidArgument):
  Logger(programName) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::log::DummyLogger::~DummyLogger() throw() {
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void castor::log::DummyLogger::prepareForFork()
  throw(castor::exception::Internal) {
}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void castor::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[],
  const struct timeval &timeStamp) throw() {
}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void castor::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[]) throw() {
}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void castor::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg) throw() {
}
