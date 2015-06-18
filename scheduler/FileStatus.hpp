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

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * The status of a file or a directory.
 */
class FileStatus {
public:

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  FileStatus();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   *
   * @param storageClassName The name of the directory's storage class or an
   * empty string if the directory does not have a storage class.
   */
  FileStatus(const std::string &storageClassName);

  /**
   * Returns the user ID of the owner.
   *
   * @return The user ID of the owner.
   */
  uint32_t getUid() const throw();

  /**
   * Returns the group ID of the owner.
   *
   * @return The group ID of the owner.
   */
  uint32_t getGid() const throw();

  /**
   * Returns the mode bits of the directory entry.
   *
   * @return The mode bits of the directory entry.
   */
  mode_t getMode() const throw();

  /**
   * Sets the name of the storage class.
   *
   * @param storageClassName The name of the storage class.
   */
  void setStorageClassName(const std::string &storageClassName);

  /**
   * Returns the name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   *
   * @return The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  const std::string &getStorageClassName() const throw();

private:

  /**
   * The user ID of the directory entry's owner.
   */
  uint32_t m_ownerId;

  /**
   * The group ID of the directory entry.
   */
  uint32_t m_groupId;

  /**
   * The mode bits of the directory entry.
   */
  mode_t m_mode;

  /**
   * The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  std::string m_storageClassName;

}; // class FileStatus

} // namespace cta
