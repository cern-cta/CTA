#pragma once

#include "FileAttribute.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * The structure of a directory entry.
 */
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
   * The user ID of the file's owner.
   */
  uint32_t ownerId;

  /**
   * The group ID of the file.
   */
  uint32_t groupId;

  /**
   * Access permissions for the file's owner "a la" posix. For example a value
   * of 7 means read, write and execute permissions.
   */
  uint8_t ownerPerms;

  /**
   * Access permissions for the file's group  "a la" posix. For example a
   * value of 7 means read, write and execute permissions.
   */
  uint8_t groupPerms;

  /**
   * Access permissions for users other than the owner or members of the file's
   * group.
   */
  uint8_t otherPerms;

  /**
   * The attributes of the directory.
   */
  std::list<FileAttribute> attributes;

  /**
   * Constructor.
   *
   * Initialises the entry type of the DirectoryEntry to NONE and all integer
   * member-variables to 0.
   */
  DirectoryEntry();

}; // DirectoryEntry

} // namespace cta
