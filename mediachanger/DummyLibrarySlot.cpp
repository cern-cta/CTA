/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/DummyLibrarySlot.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot::DummyLibrarySlot() :
  LibrarySlot(TAPE_LIBRARY_TYPE_DUMMY) {
  m_str = "dummy";
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot::DummyLibrarySlot(const std::string& str) :
  LibrarySlot(TAPE_LIBRARY_TYPE_DUMMY) {
  m_str = str;

  if(str.find("dummy") == std::string::npos) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct DummyLibrarySlot: Library slot must start with dummy: str=" << str;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::DummyLibrarySlot::clone() {
  return new DummyLibrarySlot(*this);
}
