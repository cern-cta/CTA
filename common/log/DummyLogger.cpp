/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/log/DummyLogger.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::log::DummyLogger::DummyLogger(const std::string &programName) :
Logger(programName) { }

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::log::DummyLogger::~DummyLogger() { }

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::DummyLogger::prepareForFork() {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator()(
    const int priority,
    const std::string &msg,
    const std::vector<Param> &params,
    const struct timeval &timeStamp) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params,
  const struct timeval &timeStamp) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[],
  const struct timeval &timeStamp) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::vector<Param> &params) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const std::list<Param> &params) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg,
  const int numParams,
  const log::Param params[]) {}

//------------------------------------------------------------------------------
// operator() 
//------------------------------------------------------------------------------
void cta::log::DummyLogger::operator() (
  const int priority,
  const std::string &msg) {}
