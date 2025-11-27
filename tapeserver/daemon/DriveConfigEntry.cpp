/*
 * SPDX-FileCopyrightText: 2024 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveConfigEntry.hpp"

#include "common/exception/Exception.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
cta::tape::daemon::DriveConfigEntry::DriveConfigEntry(const std::string& unitName,
                                                      const std::string& logicalLibrary,
                                                      const std::string& devFilename,
                                                      const std::string& librarySlot)
    : unitName(unitName),
      logicalLibrary(logicalLibrary),
      devFilename(devFilename),
      rawLibrarySlot(librarySlot),
      m_librarySlot(mediachanger::LibrarySlotParser::parse(rawLibrarySlot)) {
  if (unitName.size() > maxNameLen) {
    throw cta::exception::Exception("DriveConfigEntry::DriveConfigEntry - unitName '" + unitName
                                    + "' exceeds max length of " + std::to_string(maxNameLen) + " (got "
                                    + std::to_string(unitName.size()) + ")");
  }

  if (logicalLibrary.size() > maxNameLen) {
    throw cta::exception::Exception("DriveConfigEntry::DriveConfigEntry - logicalLibrary '" + logicalLibrary
                                    + "' exceeds max length of " + std::to_string(maxNameLen) + " (got "
                                    + std::to_string(logicalLibrary.size()) + ")");
  }

  if (devFilename.size() > maxNameLen) {
    throw cta::exception::Exception("DriveConfigEntry::DriveConfigEntry - devFilename '" + devFilename
                                    + "' exceeds max length of " + std::to_string(maxNameLen) + " (got "
                                    + std::to_string(devFilename.size()) + ")");
  }

  if (librarySlot.size() > maxNameLen) {
    throw cta::exception::Exception("DriveConfigEntry::DriveConfigEntry - librarySlot '" + librarySlot
                                    + "' exceeds max length of " + std::to_string(maxNameLen) + " (got "
                                    + std::to_string(librarySlot.size()) + ")");
  }
}

//------------------------------------------------------------------------------
// Copy constructor.
//------------------------------------------------------------------------------
DriveConfigEntry::DriveConfigEntry(const DriveConfigEntry& o)
    : DriveConfigEntry(o.unitName, o.logicalLibrary, o.devFilename, o.rawLibrarySlot) {}

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
  m_librarySlot = mediachanger::LibrarySlotParser::parse(rawLibrarySlot);
  return *this;
}

}  // namespace cta::tape::daemon
