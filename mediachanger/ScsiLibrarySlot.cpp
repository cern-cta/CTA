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

#include "mediachanger/ScsiLibrarySlot.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot() : LibrarySlot(TAPE_LIBRARY_TYPE_SCSI), m_drvOrd(0) {
  m_str = librarySlotToString(0);
}

//------------------------------------------------------------------------------
// librarySlotToString
//------------------------------------------------------------------------------
std::string cta::mediachanger::ScsiLibrarySlot::librarySlotToString(const uint16_t drvOrd) {
  std::ostringstream oss;
  oss << "smc" << drvOrd;
  return oss.str();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot::ScsiLibrarySlot(const uint16_t drvOrd) :
LibrarySlot(TAPE_LIBRARY_TYPE_SCSI),
m_drvOrd(drvOrd) {
  m_str = librarySlotToString(drvOrd);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot::~ScsiLibrarySlot() {}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot* cta::mediachanger::ScsiLibrarySlot::clone() {
  return new ScsiLibrarySlot(*this);
}

//------------------------------------------------------------------------------
// getDrvOrd
//------------------------------------------------------------------------------
uint16_t cta::mediachanger::ScsiLibrarySlot::getDrvOrd() const {
  return m_drvOrd;
}
