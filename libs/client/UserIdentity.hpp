#pragma once

#include <stdint.h>
#include <string>

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
   * The host from which the user is communicating.
   */
  std::string host;

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
   * @param host The host from which the user is communicating.
   */
  UserIdentity(const uint32_t uid, const uint32_t gid, const std::string &host)
    throw();

}; // struct UserIdentity

} // namespace cta
