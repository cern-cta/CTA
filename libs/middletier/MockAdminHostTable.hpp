#pragma once

#include "AdminHost.hpp"
#include "SecurityIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * Mock database-table of administration hosts.
 */
class MockAdminHostTable {
public:

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment);

  /**
   * Throws an exception if the specified administration host already exists.
   *
   * @param hostName The network name of the administration host.
   */
  void checkAdminHostDoesNotAlreadyExist(const std::string &hostName) const;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName);

  /**
   * Returns the current list of administration hosts.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const;

private:

  /**
   * Mapping from administration host-name to administration host.
   */
  std::map<std::string, AdminHost> m_adminHosts;

}; // class MockAdminHostTable

} // namespace cta
