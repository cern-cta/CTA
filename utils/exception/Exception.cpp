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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "utils/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::exception::Exception::Exception(int se, std::string context, bool embedBacktrace) : 
  m_message(context), m_backtrace(!embedBacktrace) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::exception::Exception::Exception(std::string context, bool embedBacktrace) : 
  m_message(context), m_backtrace(!embedBacktrace) {}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
cta::exception::Exception::Exception(
  const cta::exception::Exception& rhs):
  std::exception() {
  m_message << rhs.m_message.str();
  m_backtrace = rhs.m_backtrace;
}


//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
cta::exception::Exception& cta::exception::Exception::operator=(
  const cta::exception::Exception &rhs) {
  m_message << rhs.m_message.str();
  return *this;
}

//------------------------------------------------------------------------------
// what operator
//------------------------------------------------------------------------------
const char * cta::exception::Exception::what() const throw () {
  m_what = getMessageValue();
  m_what += "\n";
  m_what += (std::string) m_backtrace;
  return m_what.c_str();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::exception::Exception::~Exception() throw()  {}
//------------------------------------------------------------------------------
// setWhat
//------------------------------------------------------------------------------
void cta::exception::Exception::setWhat(const std::string& what) {
  getMessage() << what;
}

