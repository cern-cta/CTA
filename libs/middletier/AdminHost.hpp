#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a administration host.
 */
class AdminHost {
public:

  /**
   * Constructor.
   */
  AdminHost();

  /**
   * Constructor.
   *
   * @param name The network name of the administration host.
   * @param creator The identity of the user that created the administration host.
   * @param comment The comment describing the administration host.
   */
  AdminHost(
    const std::string &name,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

  /**
   * Returns the network name of the administration host.
   *
   * @return The network name of the administration host.
   */
  const std::string &getName() const throw();

  /**
   * Returns the time when the administration host was created.
   *
   * @return The time when the administration host was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the administration host.
   *
   * @return The identity of the user that created the administration host.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the administration host.
   *
   * @return The comment describing the administration host.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The network name of the administration host.
   */
  std::string m_name;

  /**
   * The time when the administration host was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the administration host.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the administration host.
   */
  std::string m_comment;

}; // class AdminHost

} // namespace cta
