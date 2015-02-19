#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing an administrator.
 */
class AdminUser {
public:

  /**
   * Constructor.
   */
  AdminUser();

  /**
   * Constructor.
   *
   * @param user The identity of the administrator.
   * @param creator The identity of the user that created the administrator.
   * @param comment The comment describing the administrator.
   */
  AdminUser(
    const UserIdentity &user,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the identity of the administrator.
   *
   * @return The identity of the administrator.
   */
  const UserIdentity &getUser() const throw();

  /**
   * Returns the time when the administrator was created.
   *
   * @return The time when the administrator was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the administrator.
   *
   * @return The identity of the user that created the administrator.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the administrator.
   *
   * @return The comment describing the administrator.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The identity of the administrator.
   */
  UserIdentity m_user;

  /**
   * The time when the administrator was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the administrator.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the administrator.
   */
  std::string m_comment;

}; // class AdminUser

} // namespace cta
