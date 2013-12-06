/******************************************************************************
 *                      Exception.cpp
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
 * @author Sebastien Ponce
 *****************************************************************************/

// Local Files
#include "Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::exception::Exception::Exception(int se) : m_serrno(se) {}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
castor::exception::Exception::Exception(
  const castor::exception::Exception& rhs):
  std::exception() {
  m_serrno = rhs.m_serrno;
  m_message << rhs.m_message.str();
}


//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
castor::exception::Exception& castor::exception::Exception::operator=(
  const castor::exception::Exception &rhs) {
  m_serrno = rhs.m_serrno;
  m_message << rhs.m_message.str();
  return *this;
}

//------------------------------------------------------------------------------
// what operator
//------------------------------------------------------------------------------
const char * castor::exception::Exception::what() {
  m_what = getMessage().str();
  m_what += "\n";
  m_what += (std::string) m_backtrace;
  return m_what.c_str();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::exception::Exception::~Exception() throw()  {}
