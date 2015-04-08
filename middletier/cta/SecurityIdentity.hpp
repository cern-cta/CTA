#pragma once

#include "cta/UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class containing the security information necessary to authorise a user
 * submitting a requests from a specific host.
 */
struct SecurityIdentity {

  /**
   * The identity of the user.
   */
  UserIdentity user;

  /**
   * The network name of the host from which they are submitting a request.
   */
  std::string host;

  /**
   * Constructor.
   */
  SecurityIdentity();

  /**
   * Constructor.
   */
  SecurityIdentity(const UserIdentity &user, const std::string &host);

}; // struct SecurityIdentity

} // namespace cta
