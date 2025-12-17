/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/DebugBuf.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DebugBuf::DebugBuf(std::ostream& os) : m_os(os) {}

//------------------------------------------------------------------------------
// setDebug
//------------------------------------------------------------------------------
void cta::mediachanger::DebugBuf::setDebug(const bool value) {
  m_debug = value;
}

//------------------------------------------------------------------------------
// overflow
//------------------------------------------------------------------------------
std::streambuf::int_type cta::mediachanger::DebugBuf::overflow(const int_type c) {
  // Only write something if debug mode is on
  if (m_debug) {
    if (m_writePreamble) {
      writePreamble();
      m_writePreamble = false;
    }
    m_os << (char) c;
  }

  // If an end of line was encountered then the next write should be preceeded
  // with a preamble
  if ('\n' == (char) c) {
    m_writePreamble = true;
  }

  return c;
}

//------------------------------------------------------------------------------
// writePreamble
//------------------------------------------------------------------------------
void cta::mediachanger::DebugBuf::writePreamble() {
  m_os << "DEBUG: ";
}
