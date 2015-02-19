#include "Exception.hpp"
#include "MockAdminUserTable.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::MockAdminUserTable::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  checkAdminUserDoesNotAlreadyExist(user);
  AdminUser adminUser(user, requester.user, comment);
  m_adminUsers[user.getUid()] = adminUser;
}

//------------------------------------------------------------------------------
// checkAdminUserDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockAdminUserTable::checkAdminUserDoesNotAlreadyExist(
  const UserIdentity &user) const {
  std::map<uint32_t, AdminUser>::const_iterator itor =
    m_adminUsers.find(user.getUid());
  if(itor != m_adminUsers.end()) {
    std::ostringstream message;
    message << "Administrator with uid " << user.getUid() <<
      " already exists";
    throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockAdminUserTable::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  for(std::map<uint32_t, AdminUser>::iterator itor = m_adminUsers.begin();
    itor != m_adminUsers.end(); itor++) {
    if(user.getUid() == itor->first) {
      m_adminUsers.erase(itor);
      return;
    }
  }

  // Reaching this point means the administrator to be deleted does not exist
  std::ostringstream message;
  message << "Administration with uid " << user.getUid() << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::MockAdminUserTable::getAdminUsers(
  const SecurityIdentity &requester) const {
  std::list<cta::AdminUser> users;
  for(std::map<uint32_t, AdminUser>::const_iterator itor =
    m_adminUsers.begin(); itor != m_adminUsers.end(); itor++) {
    users.push_back(itor->second);
  }
  return users;
}
