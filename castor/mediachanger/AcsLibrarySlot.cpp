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

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot() throw():
  m_acs(0),
  m_lsm(0),
  m_panel(0),
  m_drive(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::AcsLibrarySlot::AcsLibrarySlot(const std::string &str) {
  // TO BE DONE
  m_acs = 1;
  m_lsm = 2;
  m_panel = 3;
  m_drive = 4;
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

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
std::string castor::mediachanger::AcsLibrarySlot::str() const {
  std::ostringstream oss;

  oss << "acs= " << m_acs << " lsm=" << m_lsm << " panel=" << m_panel <<
    " drive=" << m_drive;
  return oss.str();
}
