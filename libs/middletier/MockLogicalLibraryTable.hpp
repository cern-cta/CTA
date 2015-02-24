#pragma once

#include "LogicalLibrary.hpp"
#include "SecurityIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * Mock database-table logical libraries.
 */
class MockLogicalLibraryTable {
public:

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment);

  /**
   * Throws an exception if the specified logical library already exists.
   *
   * @param name The name of the logical library.
   */
  void checkLogicalLibraryDoesNotAlreadyExist(const std::string &name) const;

  /**
   * Throws an exception if the specified logical library does not exists.
   *
   * @param name The name of the logical library.
   */
  void checkLogicalLibraryExists(const std::string &name) const;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const;

private:

  /**
   * Mapping from logical-library name to logical library.
   */
  std::map<std::string, LogicalLibrary> m_libraries;

}; // class MockLogicalLibraryTable

} // namespace cta
