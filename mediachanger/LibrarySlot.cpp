/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlot.hpp"

#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot::LibrarySlot(const uint16_t drvOrd, bool dummy) : m_drvOrd(drvOrd), m_dummy(dummy) {
  m_str = librarySlotToString(m_drvOrd);
}

//------------------------------------------------------------------------------
// librarySlotToString
//------------------------------------------------------------------------------
std::string cta::mediachanger::LibrarySlot::librarySlotToString(const uint16_t drvOrd) const {
  std::ostringstream oss;
  if (m_dummy) {
    oss << "dummy" << drvOrd;
  } else {
    oss << "smc" << drvOrd;
  }
  return oss.str();
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const std::string& cta::mediachanger::LibrarySlot::str() const {
  return m_str;
}

//------------------------------------------------------------------------------
// getDrvOrd
//------------------------------------------------------------------------------
uint16_t cta::mediachanger::LibrarySlot::getDrvOrd() const {
  return m_drvOrd;
}

//------------------------------------------------------------------------------
// isDummy
//------------------------------------------------------------------------------
bool cta::mediachanger::LibrarySlot::isDummy() const {
  return m_dummy;
}
