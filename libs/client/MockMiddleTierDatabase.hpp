#pragma once

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MiddleTierUser.hpp"
#include "MigrationRoutes.hpp"
#include "StorageClass.hpp"

#include <vector>

namespace cta {

/**
 * Mock middle-tier.
 */
class MockMiddleTierDatabase {
public:

  /**
   * Constructor.
   *
   * Creates the root directory "/" owned by user root and with no file
   * attributes or permissions.
   */
  MockMiddleTierDatabase();

  /**
   * Destructor.
   */
  ~MockMiddleTierDatabase() throw();

  /**
   * The current list of administrators.
   */
  std::list<UserIdentity> adminUsers;

  /**
   * The current list of administration hosts.
   */
  std::list<std::string> adminHosts;

  /**
   * Container of the storage classes used by the file system.
   */
  FileSystemStorageClasses storageClasses;

  /**
   * Mapping from tape pool name to tape pool.
   */
  std::map<std::string, TapePool> tapePools;

  /**
   * Container of migration routes.
   */
  MigrationRoutes migrationRoutes;

  /**
   * The root node of the file-system.
   */
  FileSystemNode fileSystemRoot;

}; // class MockMiddleTierDatabase

} // namespace cta
