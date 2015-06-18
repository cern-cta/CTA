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

#include "common/FileStatus.hpp"

#include <string>

namespace cta {

/**
 * A directory entry.
 */
class DirEntry {
public:

  /**
   * Enumeration of the different possible type so directory entry.
   */
  enum EntryType {
    ENTRYTYPE_NONE,
    ENTRYTYPE_FILE,
    ENTRYTYPE_DIRECTORY};

  /**
   * Thread safe method that returns the string representation of the specified
   * enumeration value.
   */
  static const char *entryTypeToStr(const EntryType enumValue) throw();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  DirEntry();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   *
   * @param entryType The type of the entry.
   * @param name The name of the directory entry.
   * @param status The status of the entry.
   */
  DirEntry(const EntryType entryType, const std::string &name,
    const FileStatus &status);

  /**
   * Returns the type of the directory entry.
   *
   * @return The type of the directory entry.
   */
  EntryType getType() const throw();

  /**
   * Returns the name of the directory entry.
   *
   * @return The name of the directory entry.
   */
  const std::string &getName() const throw();

  /**
   * Returns the status of the entry.
   *
   * @return The status of the entry.
   */
  FileStatus &getStatus() throw();

  /**
   * Returns the status of the entry.
   *
   * @return The status of the entry.
   */ 
  const FileStatus &getStatus() const throw();

private:

  /**
   * The type of the directory entry.
   */
  EntryType m_entryType;

  /**
   * The name of the directory entry.
   */
  std::string m_name;

  /**
   * The status of the entry.
   */
  FileStatus m_status;

}; // class DirEntry

} // namespace cta
