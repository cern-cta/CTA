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

#include "mediachanger/AcsLibrarySlot.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::AcsLibrarySlot::AcsLibrarySlot():
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
std::string cta::mediachanger::AcsLibrarySlot::librarySlotToString(
  const uint32_t acs, const uint32_t lsm, const uint32_t panel,
  const uint32_t drive) const {
  std::ostringstream oss;
  oss << "acs" << acs << "," << lsm << "," << panel << "," << drive;
  return oss.str();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::AcsLibrarySlot::AcsLibrarySlot(const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive):
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
cta::mediachanger::AcsLibrarySlot::~AcsLibrarySlot() {
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::AcsLibrarySlot::
  clone() {
  return new AcsLibrarySlot(*this);
}

//------------------------------------------------------------------------------
// getAcs
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::AcsLibrarySlot::getAcs() const {
  return m_acs;
}

//------------------------------------------------------------------------------
// getLsm
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::AcsLibrarySlot::getLsm() const {
  return m_lsm;
}

//------------------------------------------------------------------------------
// getPanel
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::AcsLibrarySlot::getPanel() const {
  return m_panel;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::AcsLibrarySlot::getDrive() const {
  return m_drive;
}
