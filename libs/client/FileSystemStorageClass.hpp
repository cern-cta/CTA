#pragma once

#include "Exception.hpp"
#include "StorageClass.hpp"
#include "UserIdentity.hpp"

#include <sstream>
#include <string>
#include <time.h>

namespace cta {

/**
 * Class used to store a storage class together with the other information
 * required by the file system.
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
   * The storage class.
   */
  StorageClass m_storageClass;

  /**
   * The usage count.
   */
  uint64_t m_usageCount;

}; // class FileSystemStorageClass

} // namespace cta
