/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "mediachanger/LibrarySlot.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot::LibrarySlot(const TapeLibraryType libraryType) : m_libraryType(libraryType) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot::~LibrarySlot() {}

//------------------------------------------------------------------------------
// getLibrarySlotType
//------------------------------------------------------------------------------
cta::mediachanger::TapeLibraryType cta::mediachanger::LibrarySlot::getLibraryTypeOfSlot(const std::string& slot) {
  if (0 == slot.find("dummy")) {
    return TAPE_LIBRARY_TYPE_DUMMY;
  }
  if (0 == slot.find("smc")) {
    return TAPE_LIBRARY_TYPE_SCSI;
  }

  cta::exception::Exception ex;
  ex.getMessage() << "Cannot determine tape-library type of library slot"
                     ": slot="
                  << slot;
  throw ex;
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const std::string& cta::mediachanger::LibrarySlot::str() const {
  return m_str;
}

//------------------------------------------------------------------------------
// getLibraryType
//------------------------------------------------------------------------------
cta::mediachanger::TapeLibraryType cta::mediachanger::LibrarySlot::getLibraryType() const {
  return m_libraryType;
}
