#include "cta/AdminUser.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminUser::AdminUser() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::AdminUser::~AdminUser() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminUser::AdminUser(
  const UserIdentity &user,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_user(user) {
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::AdminUser::getUser() const throw() {
  return m_user;
}
