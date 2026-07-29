/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveInfo.hpp"

#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
cta::common::dataStructures::DriveInfo::DriveInfo(const std::string& driveName,
                                                  const std::string& hostname,
                                                  const std::string& logicalLibrary,
                                                  const std::string& devFilename,
                                                  const std::string& librarySlot)
    : driveName(driveName),
      host(hostname),
      logicalLibrary(logicalLibrary),
      devFilename(devFilename),
      rawLibrarySlot(librarySlot) {
  if (driveName.size() > maxNameLen) {
    throw cta::exception::Exception("Drive name '" + driveName + "' exceeds max length of " + std::to_string(maxNameLen)
                                    + " (got " + std::to_string(driveName.size()) + ")");
  }

  if (logicalLibrary.size() > maxNameLen) {
    throw cta::exception::Exception("Logical library '" + logicalLibrary + "' exceeds max length of "
                                    + std::to_string(maxNameLen) + " (got " + std::to_string(logicalLibrary.size())
                                    + ")");
  }

  if (devFilename.size() > maxNameLen) {
    throw cta::exception::Exception("Device filename '" + devFilename + "' exceeds max length of "
                                    + std::to_string(maxNameLen) + " (got " + std::to_string(devFilename.size()) + ")");
  }

  if (librarySlot.size() > maxNameLen) {
    throw cta::exception::Exception("Library slot '" + librarySlot + "' exceeds max length of "
                                    + std::to_string(maxNameLen) + " (got " + std::to_string(librarySlot.size()) + ")");
  }
}

}  // namespace cta::common::dataStructures
