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

#include "common/checksum/Checksum.hpp"
#include "common/UserIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * The status of a file or a directory entry in the archive namespace.
 */
struct ArchiveFileStatus {

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  ArchiveFileStatus();

  /**
   * Constructor.
   *
   * @param owner The identity of the owner.
   * @param mode The mode bits of the file or directory.
   * @param size The size of the file in bytes.
   * @param checksum The checksum of the file.
   * @param storageClassName The name of the file or directory's storage class.
   * An empty string indicates no storage class.
   */
  ArchiveFileStatus(
    const UserIdentity &owner,
    const mode_t mode,
    const uint64_t size,
    const Checksum &checksum,
    const std::string &storageClassName);

  /**
   * The identity of the owner.
   */
  UserIdentity owner;

  /**
   * The mode bits of the directory entry.
   */
  mode_t mode;

  /**
   * Returns the size of the file in bytes.
   *
   * @return The size of the file in bytes.
   */
  uint64_t size;

  /**
   * The checksum of the file.
   */
  Checksum checksum;

  /**
   * The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  std::string storageClassName;

}; // class ArchiveFileStatus

} // namespace cta
