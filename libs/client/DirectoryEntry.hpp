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
   * The mode bits of the file. These bits will have the same values as the
   * st_mode field of the "struct stat" as described in the unix manual page
   * "man 2 stat".
   */
  uint16_t mode;

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
