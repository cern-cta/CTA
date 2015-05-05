#pragma once

#include "cta/ConfigurationItem.hpp"
#include "cta/UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing an administrator.
 */
class AdminUser: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  AdminUser();

  /**
   * Destructor.
   */
  ~AdminUser() throw();

  /**
   * Constructor.
   *
   * @param user The identity of the administrator.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment made by the creator of this configuration
   * item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  AdminUser(
    const UserIdentity &user,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the identity of the administrator.
   *
   * @return The identity of the administrator.
   */
  const UserIdentity &getUser() const throw();

private:

  /**
   * The identity of the administrator.
   */
  UserIdentity m_user;

}; // class AdminUser

} // namespace cta
