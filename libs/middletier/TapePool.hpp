#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a tape pool.
 */
class TapePool {
public:

  /**
   * Constructor.
   */
  TapePool();

  /**
   * Constructor.
   *
   * @param name The name of the tape pool.
   * @param nbDrives The maximum number of drives that can be concurrently
   * assigned to this pool independent of whether they are archiving or
   * retrieving files.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param creator The identity of the user that created the tape pool.
   * @param comment The comment describing the tape pool.
   */
  TapePool(
    const std::string &name,
    const uint16_t nbDrives,
    const uint32_t nbPartialTapes,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

  /**
   * Returns the name of the tape pool.
   *
   * @return The name of the tape pool.
   */
  const std::string &getName() const throw();

  /**
   * Returns the maximum number of drives that can be concurrently
   * assigned to this pool independent of whether they are archiving or
   * retrieving files.
   *
   * @return The maximum number of drives that can be concurrently
   * assigned to this pool independent of whether they are archiving or
   * retrieving files.
   */
  uint16_t getNbDrives() const throw();

  /**
   * Returns the maximum number of tapes that can be partially full at any
   * moment in time.
   *
   * @return The maximum number of tapes that can be partially full at any
   * moment in time.
   */
  uint32_t getNbPartialTapes() const throw();

  /**
   * Returns the time when the tape pool was created.
   *
   * @return The time when the tape pool was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the tape pool.
   *
   * @return The identity of the user that created the tape pool.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the tape pool.
   *
   * @return The comment describing the tape pool.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The name of the tape pool.
   */
  std::string m_name;

  /**
   * The maximum number of drives that can be concurrently assigned to this
   * pool independent of whether they are archiving or retrieving files.
   */
  uint16_t m_nbDrives;

  /**
   * The maximum number of tapes that can be partially full at any moment in
   * time.
   */
  uint32_t m_nbPartialTapes;

  /**
   * The time when the tape pool was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the tape pool.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the tape pool.
   */
  std::string m_comment;

}; // class TapePool

} // namespace cta
