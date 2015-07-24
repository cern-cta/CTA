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

#include "castor/mediachanger/AcsLibrarySlot.hpp"
#include "castor/utils/utils.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot() throw():
  LibrarySlot(TAPE_LIBRARY_TYPE_ACS),
  m_acs(0),
  m_lsm(0),
  m_panel(0),
  m_drive(0) {
  m_str = librarySlotToString(0, 0, 0, 0);
}

//------------------------------------------------------------------------------
// librarySlotToString
//------------------------------------------------------------------------------
std::string castor::mediachanger::AcsLibrarySlot::librarySlotToString(
  const uint32_t acs, const uint32_t lsm, const uint32_t panel,
  const uint32_t drive) const {
  std::ostringstream oss;
  oss << "acs" << acs << "," << lsm << "," << panel << "," << drive;
  return oss.str();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot(const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive) throw():
  LibrarySlot(TAPE_LIBRARY_TYPE_ACS),
  m_acs(acs),
  m_lsm(lsm),
  m_panel(panel),
  m_drive(drive) {
  m_str = librarySlotToString(acs, lsm, panel, drive);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::~AcsLibrarySlot() throw() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::mediachanger::LibrarySlot *castor::mediachanger::AcsLibrarySlot::
  clone() {
  return new AcsLibrarySlot(*this);
}

//------------------------------------------------------------------------------
// getAcs
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getAcs() const throw () {
  return m_acs;
}

//------------------------------------------------------------------------------
// getLsm
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getLsm() const throw () {
  return m_lsm;
}

//------------------------------------------------------------------------------
// getPanel
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getPanel() const throw () {
  return m_panel;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
uint32_t castor::mediachanger::AcsLibrarySlot::getDrive() const throw () {
  return m_drive;
}
