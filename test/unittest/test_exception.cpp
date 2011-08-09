/******************************************************************************
 *                test/unittest/test_exception.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "test/unittest/test_exception.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
test_exception::test_exception(std::string msg): m_msg(msg) {
}

//-----------------------------------------------------------------------------
// copy constructor
//-----------------------------------------------------------------------------
test_exception::test_exception(const test_exception &tx) : m_msg(tx.m_msg) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
test_exception::~test_exception() throw() {
}

//-----------------------------------------------------------------------------
// assignment operator
//-----------------------------------------------------------------------------
test_exception &test_exception::operator=(const test_exception &tx) {
  // If this is not a self-assignment
  if(this != &tx) {
    m_msg = tx.m_msg;
  }

  return *this;
}

//-----------------------------------------------------------------------------
// what
//-----------------------------------------------------------------------------
const char* test_exception::what() const throw() {
  return m_msg.c_str();
}
