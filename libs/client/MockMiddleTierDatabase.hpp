#pragma once

#include "AdminHost.hpp"
#include "AdminUser.hpp"
#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "LogicalLibrary.hpp"
#include "MiddleTierUser.hpp"
#include "MigrationRoutes.hpp"
#include "StorageClass.hpp"

#include <stdint.h>
#include <string>
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
   * Mapping from administrator uid to administrator.
   */
  std::map<uint32_t, AdminUser> adminUsers;

  /**
   * Mapping from administration host-name to administration host.
   */
  std::map<std::string, AdminHost> adminHosts;

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

  /**
   * Mapping from logical-library name to logical library.
   */
  std::map<std::string, LogicalLibrary> libraries;

}; // class MockMiddleTierDatabase

} // namespace cta
