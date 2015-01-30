#pragma once

#include <stdint.h>

namespace cta {

/**
 * Class reprsenting the identity of a user.
 */
struct UserIdentity {

  /**
   * The user ID of the user.
   */
  uint32_t uid;

  /**
   * The group ID of the user.
   */
  uint32_t gid;

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

}; // struct UserIdentity

} // namespace cta
