#pragma once

#include "UserIdentity.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

/**
 * An archive route.
 */
class ArchiveRoute {
public:

  /**
   * Constructor.
   */
  ArchiveRoute();

  /**
   * Constructor.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.  Copy numbers start from 1.
   * @param tapePoolName The name of the destination tape pool.
   * @param creator The identity of the user that created the storage class.
   * @param comment Comment describing the storage class.
   */
  ArchiveRoute(
    const std::string &storageClassName,
    const uint8_t copyNb,
    const std::string &tapePoolName,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

  /**
   * Returns the name of the storage class that identifies the source disk
   * files.
   *
   * @return The name of the storage class that identifies the source disk
   * files.
   */
  const std::string &getStorageClassName() const throw();

  /**
   * Returns the tape copy number.
   *
   * @return The tape copy number.
   */
  uint8_t getCopyNb() const throw();

  /**
   * Returns the name of the destination tape pool.
   *
   * @return The name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

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
   * The name of the storage class that identifies the source disk files.
   */
  std::string m_storageClassName;

  /**
   * The tape copy number.
   */
  uint32_t m_copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

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

}; // class ArchiveRoute

} // namespace cta
