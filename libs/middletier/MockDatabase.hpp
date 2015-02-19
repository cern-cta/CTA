#pragma once

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MockAdminHostDatabase.hpp"
#include "MockAdminUserDatabase.hpp"
#include "MockLogicalLibraryDatabase.hpp"
#include "MockMigrationRouteDatabase.hpp"
#include "MockTapeDatabase.hpp"
#include "MockTapePoolDatabase.hpp"

namespace cta {

/**
 * Mock database.
 */
struct MockDatabase {
  /**
   * Constructor.
   *
   * Creates the root directory "/" owned by user root and with no file
   * attributes or permissions.
   */
  MockDatabase();

  /**
   * Destructor.
   */
  ~MockDatabase() throw();

  /**
   * Database of administrators.
   */
  MockAdminUserDatabase adminUsers;

  /**
   * Database of administration hosts.
   */
  MockAdminHostDatabase adminHosts;

  /**
   * Container of the storage classes used by the file system.
   */
  FileSystemStorageClasses storageClasses;

  /**
   * Database of tape pools.
   */
  MockTapePoolDatabase tapePools;

  /**
   * Container of migration routes.
   */
  MockMigrationRouteDatabase migrationRoutes;

  /**
   * The root node of the file-system.
   */
  FileSystemNode fileSystemRoot;

  /**
   * Database of logical libraries.
   */
  MockLogicalLibraryDatabase libraries;

  /**
   * Database of tapes.
   */
  MockTapeDatabase tapes;

}; // struct MockDatabase

} // namespace cta
