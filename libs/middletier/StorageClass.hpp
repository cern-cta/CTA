#pragma once

#include "UserIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing an archive storage-class.
 */
class StorageClass {
public:

  /**
   * Constructor.
   */
  StorageClass();

  /**
   * Constructor.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creator The identity of the user that created the storage class.
   * @param comment The comment describing the storage class.
   */
  StorageClass(
    const std::string &name,
    const uint8_t nbCopies,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

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
  uint8_t getNbCopies() const throw();

  /**
   * Returns the time when the storage class was created.
   *
   * @return The time when the storage class was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the storage class.
   *
   * @return The identity of the user that created the storage class.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the storage class.
   *
   * @return The comment describing the storage class.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The name of the storage class.
   */
  std::string m_name;

  /**
   * The number of copies a file associated with this storage
   * class should have on tape.
   */
  uint8_t m_nbCopies;

  /**
   * The time when the storage class was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the storage class.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the storage class.
   */
  std::string m_comment;

}; // class StorageClass

} // namespace cta
