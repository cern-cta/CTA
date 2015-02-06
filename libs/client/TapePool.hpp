#pragma once

#include "UserIdentity.hpp"

#include <stdint.h>
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
   * @param creator The identity of the user that created the tape pool.
   * @param comment The comment describing the tape pool.
   */
  TapePool(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the name of the tape pool.
   *
   * @return The name of the tape pool.
   */
  const std::string &getName() const throw();

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
