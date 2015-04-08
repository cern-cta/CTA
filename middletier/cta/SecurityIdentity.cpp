#include "cta/SecurityIdentity.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SecurityIdentity::SecurityIdentity() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SecurityIdentity::SecurityIdentity(const UserIdentity &user,
  const std::string &host): user(user), host(host) {
}
