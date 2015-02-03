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
   * Enumeration of the different possible type so directory entry.
   */
  enum EntryType {
    ENTRYTYPE_FILE,
    ENTRYTYPE_DIRECTORY};

  /**
   * Thread safe method that returns the string reprsentation of the specified
   * enumeration value.
   */
  static const char *entryTypeToStr(const EntryType enumValue) throw();

  /**
   * The type of the entry.
   */
  EntryType entryType;

  /**
   * The name of the directory entry.
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
   * The mode bits of the file.
   */
  uint16_t mode;

  /**
   * The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  std::string storageClassName;

  /**
   * The attributes of the directory.
   */
  std::list<FileAttribute> attributes;

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  DirectoryEntry();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   *
   * @param entryType The type of the entry.
   * @param name The name of the directory entry.
   */
  DirectoryEntry(const EntryType entryType, const std::string &name);

}; // DirectoryEntry

} // namespace cta
