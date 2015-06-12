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

#include "scheduler/FileSystemStorageClass.hpp"
#include "scheduler/UserIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

class CreationLog;
  
/**
 * The storage classes used in the file system.
 */
class FileSystemStorageClasses {
public:

  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creator The identity of the user that created the storage class.
   * @param comment The comment describing the storage class.
   */
  void createStorageClass(
    const std::string &name,
    const uint16_t nbCopies,
    const CreationLog &creationLog);

  /**
   * Deletes the specified storage class.
   *
   * @param name The name of the storage class.
   */
  void deleteStorageClass(const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses() const;

  /**
   * Returns the specified storage class.
   *
   * @param name The name of the storage class.
   * @return The specified storage class.
   */
  const StorageClass &getStorageClass(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class does not exist.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassExists(const std::string &name) const;

  /**
   * Increments the usage count of the specified storage class.
   *
   * @param name The name of the storage class.  If this parameter is set to the
   * empty string then this method does nothing.
   */
  void incStorageClassUsageCount(const std::string &name);

  /**
   * Decrements the usage count of the specified storage class.
   *
   * @param name The name of the storage class.  If this parameter is set to the
   * empty string then this method does nothing.
   */
  void decStorageClassUsageCount(const std::string &name);

  /**
   * Throws an exception if the specified storage class is use.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassIsNotInUse(const std::string &name) const;

protected:

  /**
   * The current mapping from storage class names to storage classes and their
   * usage counts.
   */
  std::map<std::string, FileSystemStorageClass> m_storageClasses;

  /**
   * Throws an exception if the specified storage class already exists.
   *
   * @param name The name of teh storage class.
   */
  void checkStorageClassDoesNotAlreadyExist(const std::string &name) const;

}; // class FileSystemStorageClasses

} // namespace cta
