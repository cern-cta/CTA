/******************************************************************************
 *                 castor/tape/mediachanger/DebugBuf.cpp
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

#include "castor/tape/mediachanger/DebugBuf.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::DebugBuf::DebugBuf(std::ostream &os):
  m_debug(false), m_os(os), m_writePreamble(true) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::DebugBuf::~DebugBuf() {
}

//------------------------------------------------------------------------------
// setDebug
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DebugBuf::setDebug(const bool value) throw() {
  m_debug = value;
}

//------------------------------------------------------------------------------
// overflow
//------------------------------------------------------------------------------
std::streambuf::int_type castor::tape::mediachanger::DebugBuf::overflow(
  const int_type c) {
  // Only write something if debug mode is on
  if(m_debug) {
    if(m_writePreamble) {
      writePreamble();
      m_writePreamble = false;
    }
    m_os << (char)c;
  }

  // If an end of line was encountered then the next write should be preceeded
  // with a preamble
  if('\n' == (char)c) {
    m_writePreamble = true;
  }

  return c;
}

//------------------------------------------------------------------------------
// writePreamble
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DebugBuf::writePreamble() throw() {
  m_os << "DEBUG: ";
}
