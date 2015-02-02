#include "UserIdentity.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity() throw():
  uid(0),
  gid(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity(
  const uint32_t uid,
  const uint32_t gid,
  const std::string &host) throw():
  uid(uid),
  gid(gid),
  host(host) {
}
