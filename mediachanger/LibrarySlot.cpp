/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
cta::mediachanger::LibrarySlot::~LibrarySlot() {
}

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
