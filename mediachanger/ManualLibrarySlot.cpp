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

#include "mediachanger/ManualLibrarySlot.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ManualLibrarySlot::ManualLibrarySlot()
 :
  LibrarySlot(TAPE_LIBRARY_TYPE_MANUAL) {
  m_str = "manual";
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ManualLibrarySlot::ManualLibrarySlot(
  const std::string &str):
  LibrarySlot(TAPE_LIBRARY_TYPE_MANUAL) {
  m_str = str;

  if(str.find("manual")) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct ManualLibrarySlot"
      ": Library slot must start with manual: str=" << str;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::ManualLibrarySlot::~ManualLibrarySlot() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::ManualLibrarySlot::
  clone() {
  return new ManualLibrarySlot(*this);
}
