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

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/ScsiLibrarySlot.hpp"
#include "castor/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot()
  throw():
  LibrarySlot(TAPE_LIBRARY_TYPE_SCSI),
  m_drvOrd(0) {
  m_str = librarySlotToString(0);
}

//------------------------------------------------------------------------------
// librarySlotToString
//------------------------------------------------------------------------------
std::string castor::mediachanger::ScsiLibrarySlot::librarySlotToString(
  const uint16_t drvOrd) {
  std::ostringstream oss;
  oss << "smc" << drvOrd;
  return oss.str();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot(const uint16_t drvOrd):
  LibrarySlot(TAPE_LIBRARY_TYPE_SCSI),
  m_drvOrd(drvOrd) {
  m_str = librarySlotToString(drvOrd);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::mediachanger::ScsiLibrarySlot::~ScsiLibrarySlot() throw() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::mediachanger::LibrarySlot *castor::mediachanger::ScsiLibrarySlot::
  clone() {
  return new ScsiLibrarySlot(*this);
}

//------------------------------------------------------------------------------
// getDrvOrd
//------------------------------------------------------------------------------
uint16_t castor::mediachanger::ScsiLibrarySlot::getDrvOrd() const throw() {
  return m_drvOrd;
}
