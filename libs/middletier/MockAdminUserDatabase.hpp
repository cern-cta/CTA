#pragma once 

#include "AdminUser.hpp"
#include "SecurityIdentity.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <map>

namespace cta {

/**
 * Mock database of admin users.
 */
class MockAdminUserDatabase {
public:

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment);

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user);

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester) const;

  /**
   * Throws an exception if the specified administrator already exists.
   *
   * @param user The identity of the administrator.
   */
  void checkAdminUserDoesNotAlreadyExist(const UserIdentity &user) const;

private:

  /**
   * Mapping from administrator uid to administrator.
   */
  std::map<uint32_t, AdminUser> m_adminUsers;

}; // MockAdminUserDatabase

} // namespace cta
