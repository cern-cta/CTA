/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/archiveNS/ArchiveFileStatus.hpp"
#include "nameserver/NameServerTapeFile.hpp"

#include <string>
#include <list>

namespace cta {

/**
 * A directory entry within the archive namespace.
 */
struct ArchiveDirEntry {

  /**
   * Enumeration of the different types of directory entry.
   */
  enum EntryType {
    ENTRYTYPE_NONE,
    ENTRYTYPE_FILE,
    ENTRYTYPE_DIRECTORY};

  /**
   * Thread safe method that returns the string representation of the specified
   * enumeration value.
   *
   * @param enumValue The integer value of the type.
   * @return The string representation.
   */
  static const char *entryTypeToStr(const EntryType enumValue) throw();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  ArchiveDirEntry();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   *
   * @param type The type of the entry.
   * @param name The name of the directory entry.
   * @param status The status of the entry.
   */
  ArchiveDirEntry(const EntryType type, const std::string &name,
    const ArchiveFileStatus &status, const std::list<NameServerTapeFile> &tapeCopies);

  /**
   * The type of the directory entry.
   */
  EntryType type;

  /**
   * The name of the directory entry.
   */
  std::string name;

  /**
   * The status of the entry.
   */
  ArchiveFileStatus status;
  
  /**
   * The tape copies associated to this file.
   */
  std::list<NameServerTapeFile> tapeCopies;

}; // class ArchiveDirEntry

} // namespace cta
