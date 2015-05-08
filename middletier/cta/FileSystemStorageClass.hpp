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

#include "cta/Exception.hpp"
#include "cta/StorageClass.hpp"
#include "cta/UserIdentity.hpp"

#include <sstream>
#include <string>
#include <time.h>

namespace cta {

// Forward declaration of FileSystemStorageClasses
class FileSystemStorageClasses;

/**
 * Class used to store a storage class and to provide other information and/or
 * features required by the file system.
 */
class FileSystemStorageClass {
public:

  /**
   * Constructor.
   *
   * Initialisez the usage count to 0.
   */
  FileSystemStorageClass();

  /**
   * Constructor.
   *
   * Initialisez the usage count to 0.
   */
  FileSystemStorageClass(const StorageClass &storageClass);

  /**
   * Returns the storage class.
   */
  const StorageClass &getStorageClass() const throw();

  /**
   * Returns the usage count.
   */
  uint64_t getUsageCount() const throw();

  /**
   * Increments the usage count by 1.
   */
  void incUsageCount();

  /**
   * Decrements the usage count by 1.
   */
  void decUsageCount();

private:

  /**
   * The storage classes used in the file system.
   */
  FileSystemStorageClasses *m_storageClasses;

  /**
   * The storage class.
   */
  StorageClass m_storageClass;

  /**
   * The usage count.
   */
  uint64_t m_usageCount;

}; // class FileSystemStorageClass

} // namespace cta
