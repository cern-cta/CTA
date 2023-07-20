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

  if(str.find("dummy") != std::string::npos) {
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
