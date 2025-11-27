/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/ScsiLibrarySlot.hpp"

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot() : LibrarySlot(TAPE_LIBRARY_TYPE_SCSI), m_drvOrd(0) {
  m_str = librarySlotToString(0);
}

//------------------------------------------------------------------------------
// librarySlotToString
//------------------------------------------------------------------------------
std::string cta::mediachanger::ScsiLibrarySlot::librarySlotToString(const uint16_t drvOrd) {
  std::ostringstream oss;
  oss << "smc" << drvOrd;
  return oss.str();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot(const uint16_t drvOrd)
    : LibrarySlot(TAPE_LIBRARY_TYPE_SCSI),
      m_drvOrd(drvOrd) {
  m_str = librarySlotToString(drvOrd);
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
std::unique_ptr<cta::mediachanger::LibrarySlot> cta::mediachanger::ScsiLibrarySlot::clone() {
  return std::make_unique<ScsiLibrarySlot>(*this);
}

//------------------------------------------------------------------------------
// getDrvOrd
//------------------------------------------------------------------------------
uint16_t cta::mediachanger::ScsiLibrarySlot::getDrvOrd() const {
  return m_drvOrd;
}
