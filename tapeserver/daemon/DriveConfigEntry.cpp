/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2024 CERN
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

#include "DriveConfigEntry.hpp"
#include "common/exception/Exception.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
cta::tape::daemon::DriveConfigEntry::DriveConfigEntry(
  const std::string &unitName,
  const std::string &logicalLibrary,
  const std::string &devFilename,
  const std::string &librarySlot):
  unitName(unitName),
  logicalLibrary(logicalLibrary),
  devFilename(devFilename),
  rawLibrarySlot(librarySlot),
  m_librarySlot(mediachanger::LibrarySlotParser::parse(rawLibrarySlot)){
  if (unitName.size() > maxNameLen)
    throw cta::exception::Exception("In DriveConfigEntry::DriveConfigEntry: unitName too long");
  if (logicalLibrary.size() > maxNameLen)
    throw cta::exception::Exception("In DriveConfigEntry::DriveConfigEntry: logicalLibrary too long");
  if (devFilename.size() > maxNameLen)
    throw cta::exception::Exception("In DriveConfigEntry::DriveConfigEntry: devFilename too long");
  if (librarySlot.size() > maxNameLen)
    throw cta::exception::Exception("In DriveConfigEntry::DriveConfigEntry: librarySlot too long");
}

//------------------------------------------------------------------------------
// Copy constructor.
//------------------------------------------------------------------------------
DriveConfigEntry::DriveConfigEntry(const DriveConfigEntry& o): DriveConfigEntry(o.unitName, o.logicalLibrary,
    o.devFilename, o.rawLibrarySlot) {}

//------------------------------------------------------------------------------
// DriveConfigEntry::librarySlot
//------------------------------------------------------------------------------
const cta::mediachanger::LibrarySlot& DriveConfigEntry::librarySlot() const {
  return *m_librarySlot;
}

//------------------------------------------------------------------------------
// DriveConfigEntry::operator=
//------------------------------------------------------------------------------
DriveConfigEntry& DriveConfigEntry::operator=(const DriveConfigEntry& o) {
  unitName = o.unitName;
  logicalLibrary = o.logicalLibrary;
  devFilename = o.devFilename;
  rawLibrarySlot = o.rawLibrarySlot;
  m_librarySlot.reset(mediachanger::LibrarySlotParser::parse(rawLibrarySlot));
  return *this;
}

} // namespace cta::tape::daemon
