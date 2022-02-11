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

#include "mediachanger/DummyLibrarySlot.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot::DummyLibrarySlot()
 :
  LibrarySlot(TAPE_LIBRARY_TYPE_DUMMY) {
  m_str = "dummy";
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot::DummyLibrarySlot(
  const std::string &str):
  LibrarySlot(TAPE_LIBRARY_TYPE_DUMMY) {
  m_str = str;

  if(str.find("dummy")) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct DummyLibrarySlot"
      ": Library slot must start with dummy: str=" << str;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot::~DummyLibrarySlot() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::DummyLibrarySlot::
  clone() {
  return new DummyLibrarySlot(*this);
}
