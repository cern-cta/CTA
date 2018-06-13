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

#include "mediachanger/acs/AcsDebugBuf.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::DebugBuf::DebugBuf(std::ostream &os):
  m_debug(false), m_os(os), m_writePreamble(true) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::DebugBuf::~DebugBuf() {
}

//------------------------------------------------------------------------------
// setDebug
//------------------------------------------------------------------------------
void cta::mediachanger::acs::DebugBuf::setDebug(const bool value) throw() {
  m_debug = value;
}

//------------------------------------------------------------------------------
// overflow
//------------------------------------------------------------------------------
std::streambuf::int_type cta::mediachanger::acs::DebugBuf::overflow(
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
void cta::mediachanger::acs::DebugBuf::writePreamble() throw() {
  m_os << "DEBUG: ";
}
