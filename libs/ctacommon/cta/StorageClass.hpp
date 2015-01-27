#pragma once

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing an archive storage-class.
 */
struct StorageClass {

  /**
   * The name of the storage class.
   */
  std::string name;

  /**
   * The number of copies a file associated with this storage
   * class should have on tape.
   */
  uint8_t nbCopies;

  /**
   * Constructor.
   *
   * Initialises nbCopies to 0.
   */
  StorageClass();

  /**
   * Constructor.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  StorageClass(const std::string &name, const uint8_t nbCopies);

}; // struct StorageClass

} // namespace cta
