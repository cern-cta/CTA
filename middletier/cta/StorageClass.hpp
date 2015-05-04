#pragma once

#include "cta/ConfigurationItem.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing an archive storage-class.
 */
class StorageClass: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  StorageClass();

  /**
   * Destructor.
   */
  ~StorageClass() throw();

  /**
   * Constructor.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creator The identity of the user that created the storage class.
   * @param comment The comment describing the storage class.
   * @param creationTime Optionally the absolute time at which the
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  StorageClass(
    const std::string &name,
    const uint16_t nbCopies,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the storage class.
   *
   * @return The name of the storage class.
   */
  const std::string &getName() const throw();

  /**
   * Returns the number of copies a file associated with this storage
   * class should have on tape.
   *
   * @return The number of copies a file associated with this storage
   * class should have on tape.
   */
  uint16_t getNbCopies() const throw();

private:

  /**
   * The name of the storage class.
   */
  std::string m_name;

  /**
   * The number of copies a file associated with this storage
   * class should have on tape.
   */
  uint16_t m_nbCopies;

}; // class StorageClass

} // namespace cta
