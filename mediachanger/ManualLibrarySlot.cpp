/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "mediachanger/ManualLibrarySlot.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ManualLibrarySlot::ManualLibrarySlot()
  throw():
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
cta::mediachanger::ManualLibrarySlot::~ManualLibrarySlot() throw() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::ManualLibrarySlot::
  clone() {
  return new ManualLibrarySlot(*this);
}
