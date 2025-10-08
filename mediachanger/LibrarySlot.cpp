/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlot.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot::LibrarySlot(
  const TapeLibraryType libraryType):
  m_libraryType(libraryType) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot::~LibrarySlot() = default;

//------------------------------------------------------------------------------
// getLibrarySlotType
//------------------------------------------------------------------------------
cta::mediachanger::TapeLibraryType cta::mediachanger::LibrarySlot::
  getLibraryTypeOfSlot(const std::string &slot) {
  if(0 == slot.find("dummy")) return TAPE_LIBRARY_TYPE_DUMMY;
  if(0 == slot.find("smc"))   return TAPE_LIBRARY_TYPE_SCSI;

  cta::exception::Exception ex;
  ex.getMessage() << "Cannot determine tape-library type of library slot"
    ": slot=" << slot;
  throw ex;
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const std::string &cta::mediachanger::LibrarySlot::str() const
  {
  return m_str;
}

//------------------------------------------------------------------------------
// getLibraryType
//------------------------------------------------------------------------------
cta::mediachanger::TapeLibraryType
  cta::mediachanger::LibrarySlot::getLibraryType() const {
  return m_libraryType;
}
