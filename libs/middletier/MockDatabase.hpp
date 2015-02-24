#pragma once

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MockAdminHostTable.hpp"
#include "MockAdminUserTable.hpp"
#include "MockArchivalJobTable.hpp"
#include "MockLogicalLibraryTable.hpp"
#include "MockMigrationRouteTable.hpp"
#include "MockRetrievalJobTable.hpp"
#include "MockTapeTable.hpp"
#include "MockTapePoolTable.hpp"

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
   * Table of administrators.
   */
  MockAdminUserTable adminUsers;

  /**
   * Table of administration hosts.
   */
  MockAdminHostTable adminHosts;

  /**
   * Container of the storage classes used by the file system.
   */
  FileSystemStorageClasses storageClasses;

  /**
   * Table of tape pools.
   */
  MockTapePoolTable tapePools;

  /**
   * Container of migration routes.
   */
  MockMigrationRouteTable migrationRoutes;

  /**
   * The root node of the file-system.
   */
  FileSystemNode fileSystemRoot;

  /**
   * Table of logical libraries.
   */
  MockLogicalLibraryTable libraries;

  /**
   * Table of tapes.
   */
  MockTapeTable tapes;

  /**
   * Table of archival jobs.
   */
  MockArchivalJobTable archivalJobs;

  /**
   * Table of retrieval jobs.
   */
  MockRetrievalJobTable retrievalJobs;

}; // struct MockDatabase

} // namespace cta
