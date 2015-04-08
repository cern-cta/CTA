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
