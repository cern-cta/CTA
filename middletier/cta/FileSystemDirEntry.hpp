#pragma once

#include "cta/DirEntry.hpp"
#include "cta/FileSystemStorageClasses.hpp"

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
