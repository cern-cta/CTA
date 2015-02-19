#include "AdminUser.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminUser::AdminUser():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminUser::AdminUser(
  const UserIdentity &user,
  const UserIdentity &creator,
  const std::string &comment):
  m_user(user),
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::AdminUser::getUser() const throw() {
  return m_user;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::AdminUser::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::AdminUser::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::AdminUser::getComment() const throw() {
  return m_comment;
}
