#pragma once

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class reprsenting the identity of a user.
 */
class UserIdentity {
public:

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  UserIdentity() throw();

  /**
   * Constructor.
   *
   * @param uid The user ID of the user.
   * @param gid The group ID of the user.
   */
  UserIdentity(const uint32_t uid, const uint32_t gid) throw();

  /**
   * Sets the user ID of the user.
   *
   * @param uid The user ID of the user.
   */
  void setUid(const uint32_t uid) throw();

  /**
   * Returns the user ID of the user.
   */
  uint32_t getUid() const throw();

  /**
   * Sets the group ID of the user.
   *
   * @param gid The group ID of the user.
   */
  void setGid(const uint32_t gid) throw();

  /**
   * Returns the group ID of the user.
   */
  uint32_t getGid() const throw();

private:

  /**
   * The user ID of the user.
   */
  uint32_t m_uid;

  /**
   * The group ID of the user.
   */
  uint32_t m_gid;


}; // class UserIdentity

} // namespace cta
