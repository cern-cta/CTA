#pragma once

#include "Exception.hpp"
#include "StorageClass.hpp"

#include <sstream>

namespace cta {

/**
 * Class used to store a storage class and it usage count.
 */
class StorageClassAndUsageCount {
public:

  /**
   * Constructor.
   *
   * Initialisez the usage count to 0.
   */
  StorageClassAndUsageCount();

  /**
   * Constructor.
   *
   * Initialisez the usage count to 0.
   */
  StorageClassAndUsageCount(const StorageClass &storageClass);

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

}; // class StorageClassAndUsageCount

} // namespace cta
