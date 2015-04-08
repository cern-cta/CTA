#pragma once

#include "cta/UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a logical library.
 */
class LogicalLibrary {
public:

  /**
   * Constructor.
   */
  LogicalLibrary();

  /**
   * Constructor.
   *
   * @param name The name of the logical library.
   * @param creator The identity of the user that created the logical library.
   * @param comment The comment describing the logical library.
   */
  LogicalLibrary(
    const std::string &name,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

  /**
   * Returns the name of the logical library.
   *
   * @return The name of the logical library.
   */
  const std::string &getName() const throw();

  /**
   * Returns the time when the logical library was created.
   *
   * @return The time when the logical library was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the logical library.
   *
   * @return The identity of the user that created the logical library.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the logical library.
   *
   * @return The comment describing the logical library.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The name of the logical library.
   */
  std::string m_name;

  /**
   * The time when the logical library was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the logical library.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the logical library.
   */
  std::string m_comment;

}; // class LogicalLibrary

} // namespace cta
