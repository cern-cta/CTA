#pragma once

#include "FileAttribute.hpp"

#include <list>
#include <string>

namespace cta {

struct DirectoryEntry {
  /**
   * Enumeration of the different types of directory entry.
   */
  enum EntryType {
    NONE,
    FILE_ENTRY,
    DIRECTORY_ENTRY
  };

  /**
   * Thread safe method that returns the string representation of the
   * specified enumeration (integer) value.
   *
   * @param enumValue The enumeration (integer) value.
   * @return The string value.
   */
  static const char *typeToString(const EntryType enumValue) throw();

  /**
   * The type of the directory entry.
   */
  EntryType entryType;

  /**
   * The name of the directory.
   */
  std::string name;

  /**
   * The attributes of the directory.
   */
  std::list<FileAttribute> attributes;

  /**
   * Constructor.
   *
   * Initialises the entry type of the DirectoryEntry to NONE.
   */
  DirectoryEntry();

}; // DirectoryEntry

} // namespace cta
