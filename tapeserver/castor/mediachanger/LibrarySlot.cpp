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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/LibrarySlot.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::LibrarySlot::LibrarySlot(
  const TapeLibraryType libraryType) throw():
  m_libraryType(libraryType) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::mediachanger::LibrarySlot::~LibrarySlot() throw() {
}

//------------------------------------------------------------------------------
// getLibrarySlotType
//------------------------------------------------------------------------------
castor::mediachanger::TapeLibraryType castor::mediachanger::LibrarySlot::
  getLibraryTypeOfSlot(const std::string &slot) {
  if(0 == slot.find("acs"))    return TAPE_LIBRARY_TYPE_ACS;
  if(0 == slot.find("manual")) return TAPE_LIBRARY_TYPE_MANUAL;
  if(0 == slot.find("smc"))    return TAPE_LIBRARY_TYPE_SCSI;

  castor::exception::Exception ex;
  ex.getMessage() << "Cannot determine tape-library type of library slot"
    ": slot=" << slot;
  throw ex;
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const std::string &castor::mediachanger::LibrarySlot::str() const
  throw() {
  return m_str;
}

//------------------------------------------------------------------------------
// getLibraryType
//------------------------------------------------------------------------------
castor::mediachanger::TapeLibraryType
  castor::mediachanger::LibrarySlot::getLibraryType() const throw() {
  return m_libraryType;
}
