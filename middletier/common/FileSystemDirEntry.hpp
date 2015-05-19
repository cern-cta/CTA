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

#include "middletier/common/DirEntry.hpp"
#include "middletier/common/FileSystemStorageClasses.hpp"

namespace cta {

/**
 * Class used to store a directory entry and to provide other information and/or
 * features required by the file system.
 */
class FileSystemDirEntry {
public:

  /**
   * Constructor.
   */
  FileSystemDirEntry();

  /**
   * Constructor.
   *
   * @param storageClasses The storage classes used by the file system.
   * @param entry The directory entry.
   */
  FileSystemDirEntry(FileSystemStorageClasses &storageClasses,
    const DirEntry &entry);

  /**
   * Returns the directory entry.
   *
   * @return The directory entry.
   */
  const DirEntry &getEntry() const throw();

  /**
   * Sets the name of the storage class.
   *
   * This method updates the usage-counts of the file-system storage-classes.
   */
  void setStorageClassName(const std::string &storageClassName);

private:

  /**
   * The storage classes used by the file system.
   */
  FileSystemStorageClasses *m_storageClasses;

  /**
   * The directory entry.
   */
  DirEntry m_entry;

}; // FileSystemDirEntry

} // namespace cta
